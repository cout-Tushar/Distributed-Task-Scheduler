import "dotenv/config";
import pg, { type PoolClient } from "pg";
import { randomUUID } from "node:crypto";
import {
  limits,
  type Submission,
  type Run,
  type State,
  type JobType,
} from "./jobs.js";

export const db = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  user: process.env.PGUSER || "scheduler",
  database: process.env.PGDATABASE || "scheduler",
  password: process.env.PGPASSWORD || "scheduler",
  host: process.env.PGHOST || "127.0.0.1",
  connectionTimeoutMillis: 5000,
});
db.on("error", (err) => console.error("PostgreSQL connection", err.message));

export async function transaction<T>(fn: (tx: PoolClient) => Promise<T>) {
  const tx = await db.connect();
  try {
    await tx.query("BEGIN");
    const value = await fn(tx);
    await tx.query("COMMIT");
    return value;
  } catch (err) {
    await tx.query("ROLLBACK");
    throw err;
  } finally {
    tx.release();
  }
}

await transaction(async (tx) => {
  // Serialize schema initialization when API and worker replicas start together.
  await tx.query("SELECT pg_advisory_xact_lock(164381902)");
  await tx.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY, type TEXT NOT NULL, payload JSONB NOT NULL, state TEXT NOT NULL,
      attempt INTEGER NOT NULL, "maxAttempts" INTEGER NOT NULL, generation INTEGER NOT NULL,
      "createdAt" DOUBLE PRECISION NOT NULL, "updatedAt" DOUBLE PRECISION NOT NULL, error TEXT, result TEXT
    );
    CREATE TABLE IF NOT EXISTS submission_keys (
      key TEXT PRIMARY KEY,
      job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
    );
    CREATE TABLE IF NOT EXISTS events (
      seq BIGSERIAL PRIMARY KEY, id TEXT NOT NULL REFERENCES jobs(id), generation INTEGER NOT NULL,
      state TEXT NOT NULL, attempt INTEGER NOT NULL, at DOUBLE PRECISION NOT NULL, detail TEXT
    );
    CREATE INDEX IF NOT EXISTS events_by_job ON events(id,seq);
    CREATE TABLE IF NOT EXISTS dispatch (
      seq BIGSERIAL PRIMARY KEY, id TEXT NOT NULL REFERENCES jobs(id), generation INTEGER NOT NULL,
      type TEXT NOT NULL, action TEXT NOT NULL, UNIQUE(id,generation,action)
    );
  `);
});

export type RecordRow = Run &
  Submission & {
    state: State;
    attempt: number;
    maxAttempts: number;
    error: string | null;
    result: string | null;
  };

export async function get(id: string, tx?: PoolClient) {
  const result = await (tx ?? db).query<RecordRow>(
    "SELECT * FROM jobs WHERE id=$1" + (tx ? " FOR UPDATE" : ""),
    [id],
  );
  return result.rows[0];
}

async function event(
  tx: PoolClient,
  id: string,
  state: State,
  detail: string | null = null,
) {
  const result = await tx.query<{ seq: string }>(
    `
    INSERT INTO events(id,generation,state,attempt,at,detail)
    SELECT id,generation,$2,attempt,"updatedAt",$3 FROM jobs WHERE id=$1 RETURNING seq`,
    [id, state, detail],
  );
  return result.rows[0].seq;
}

export function schedule(
  tx: PoolClient,
  run: Run,
  type: JobType,
  action: "enqueue" | "dead" | "remove",
) {
  return tx.query(
    "INSERT INTO dispatch(id,generation,type,action) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
    [run.id, run.generation, type, action],
  );
}

export function create(input: Submission, key?: string) {
  return transaction(async (tx) => {
    const id = randomUUID();
    if (key !== undefined) {
      // Reserve the key before enqueueing. The deferred FK is checked at commit.
      const reservation = await tx.query(
        "INSERT INTO submission_keys(key,job_id) VALUES($1,$2) ON CONFLICT (key) DO NOTHING",
        [key, id],
      );
      if (!reservation.rowCount) {
        const existing = await tx.query<{ id: string }>(
          `SELECT j.id FROM submission_keys k JOIN jobs j ON j.id=k.job_id
           WHERE k.key=$1 AND j.type=$2 AND j.payload=$3::jsonb`,
          [key, input.type, input.payload],
        );
        return existing.rows[0]?.id ?? null;
      }
    }
    await tx.query(
      `INSERT INTO jobs(id,type,payload,state,attempt,"maxAttempts",generation,"createdAt","updatedAt")
      VALUES($1,$2,$3,'QUEUED',0,$4,1,$5,$5)`,
      [id, input.type, input.payload, limits[input.type], Date.now()],
    );
    await event(tx, id, "QUEUED");
    await schedule(tx, { id, generation: 1 }, input.type, "enqueue");
    return id;
  });
}

export function retry(id: string) {
  return transaction(async (tx) => {
    const row = await get(id, tx);
    if (!row) return 404;
    if (row.state !== "DLQ") return 409;
    await schedule(tx, row, row.type, "remove");
    await tx.query(
      `UPDATE jobs SET generation=generation+1,state='QUEUED',attempt=0,error=NULL,"updatedAt"=$2 WHERE id=$1`,
      [id, Date.now()],
    );
    await event(tx, id, "QUEUED", "operator retry");
    await schedule(
      tx,
      { id, generation: row.generation + 1 },
      row.type,
      "enqueue",
    );
    return 202;
  });
}

export async function transition(
  tx: PoolClient,
  id: string,
  state: State,
  attempt: number,
  error: string | null = null,
  result: string | null = null,
) {
  await tx.query(
    `UPDATE jobs SET state=$2,attempt=$3,error=$4,result=$5,"updatedAt"=$6 WHERE id=$1`,
    [id, state, attempt, error, result, Date.now()],
  );
  return event(tx, id, state, error);
}
