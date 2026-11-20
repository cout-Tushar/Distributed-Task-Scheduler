import { DatabaseSync } from "node:sqlite";
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import {
  limits,
  type Submission,
  type Run,
  type State,
  type JobType,
} from "./jobs.js";

const filename = process.env.JOB_DB || "data/jobs.sqlite";
mkdirSync(dirname(filename), { recursive: true });
export const db = new DatabaseSync(filename);
db.exec(`
  PRAGMA busy_timeout=10000;
  PRAGMA journal_mode=WAL;
  CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY, type TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
    attempt INTEGER NOT NULL, maxAttempts INTEGER NOT NULL, generation INTEGER NOT NULL,
    createdAt INTEGER NOT NULL, updatedAt INTEGER NOT NULL, error TEXT, result TEXT
  );
  CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY, id TEXT, generation INTEGER, state TEXT, attempt INTEGER, at INTEGER, detail TEXT
  );
  CREATE INDEX IF NOT EXISTS events_by_job ON events(id,seq);
  CREATE TABLE IF NOT EXISTS dispatch (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, generation INTEGER, type TEXT, action TEXT,
    UNIQUE(id,generation,action)
  );
`);

export interface RecordRow extends Run {
  type: JobType;
  payload: string;
  state: State;
  attempt: number;
  maxAttempts: number;
  result: string | null;
  error: string | null;
}
export function get(id: string) {
  return db.prepare("SELECT * FROM jobs WHERE id=?").get(id) as unknown as
    RecordRow | undefined;
}
export function transaction<T>(fn: () => T) {
  db.exec("BEGIN IMMEDIATE");
  try {
    const value = fn();
    db.exec("COMMIT");
    return value;
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}
function event(id: string, state: State, detail: string | null = null) {
  db.prepare(
    "INSERT INTO events(id,generation,state,attempt,at,detail) SELECT id,generation,?,attempt,updatedAt,? FROM jobs WHERE id=?",
  ).run(state, detail, id);
}
export function schedule(
  run: Run,
  type: JobType,
  action: "enqueue" | "dead" | "remove",
) {
  db.prepare(
    "INSERT OR IGNORE INTO dispatch(id,generation,type,action) VALUES(?,?,?,?)",
  ).run(run.id, run.generation, type, action);
}
export function create(input: Submission) {
  return transaction(() => {
    const id = randomUUID();
    const now = Date.now();
    db.prepare(
      "INSERT INTO jobs VALUES(?,?,?,'QUEUED',0,?,1,?,?,NULL,NULL)",
    ).run(
      id,
      input.type,
      JSON.stringify(input.payload),
      limits[input.type],
      now,
      now,
    );
    event(id, "QUEUED");
    schedule({ id, generation: 1 }, input.type, "enqueue");
    return id;
  });
}
export function retry(id: string) {
  return transaction(() => {
    const row = get(id);
    if (row?.state !== "DLQ") return false;
    schedule(row, row.type, "remove");
    db.prepare(
      "UPDATE jobs SET generation=generation+1,state='QUEUED',attempt=0,error=NULL,updatedAt=? WHERE id=?",
    ).run(Date.now(), id);
    event(id, "QUEUED", "operator retry");
    schedule({ id, generation: row.generation + 1 }, row.type, "enqueue");
    return true;
  });
}
// Caller holds the transaction and checks the current generation.
export function transition(
  id: string,
  state: State,
  attempt: number,
  error: string | null = null,
  result: string | null = null,
) {
  db.prepare(
    "UPDATE jobs SET state=?,attempt=?,error=?,result=?,updatedAt=? WHERE id=?",
  ).run(state, attempt, error, result, Date.now(), id);
  event(id, state, error);
}
