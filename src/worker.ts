import type { PoolClient } from "pg";
import { DelayedError, Worker, type Job } from "bullmq";
import { connection, prefix } from "./config/redis.js";
import { circuitOpenUntil, execute } from "./handlers.js";
import { types, runId, type Run, type JobType } from "./jobs.js";
import {
  db,
  get,
  transaction,
  transition,
  schedule,
  type RecordRow,
} from "./store.js";
import { queues, closeQueues } from "./queues.js";

const selected = process.env.WORKER_TYPE
  ? [process.env.WORKER_TYPE as JobType]
  : types;
if (selected.some((t) => !types.includes(t)))
  throw new Error("Unknown WORKER_TYPE");
async function markDead(tx: PoolClient, row: RecordRow, reason: string) {
  await transition(tx, row.id, "FAILED", row.attempt, reason);
  await schedule(tx, row, row.type, "dead");
}
async function processJob(job: Job<Run>, token?: string) {
  const resumeAt = circuitOpenUntil(job.queueName as JobType);
  if (resumeAt > Date.now()) {
    await job.moveToDelayed(resumeAt, token);
    throw new DelayedError();
  }
  const { id, generation } = job.data;
  const attempt = job.attemptsMade + 1;
  const claim = await transaction(async (tx) => {
    const current = await get(id, tx);
    if (
      !current ||
      current.generation !== generation ||
      ["COMPLETED", "DLQ", "FAILED"].includes(current.state)
    )
      return;
    const seq = await transition(tx, id, "PROCESSING", attempt);
    return { row: current, seq };
  });
  if (!claim) return;
  const { row } = claim;
  // A stalled worker may finish after another worker has taken over the same attempt.
  async function ownsAttempt(tx: PoolClient) {
    await get(id, tx);
    const { rows } = await tx.query(
      "SELECT seq FROM events WHERE id=$1 ORDER BY seq DESC LIMIT 1",
      [id],
    );
    return rows[0]?.seq === claim!.seq;
  }
  try {
    const result = await execute(row, id);
    await transaction(async (tx) => {
      if (await ownsAttempt(tx))
        await transition(tx, id, "COMPLETED", attempt, null, result);
    });
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    await transaction(async (tx) => {
      if (!(await ownsAttempt(tx))) return;
      if (attempt >= row.maxAttempts)
        await markDead(tx, { ...row, attempt }, reason);
      else {
        await transition(tx, id, "FAILED", attempt, reason);
        await transition(tx, id, "QUEUED", attempt, reason);
      }
    });
    throw err;
  }
}
const workers = selected.map((type) => {
  const worker = new Worker<Run>(type, processJob, {
    connection,
    prefix,
    concurrency: 1,
    settings: {
      backoffStrategy: (attempts) =>
        Math.round(1000 * 2 ** (attempts - 1) * (0.8 + Math.random() * 0.2)),
    },
  });
  worker.on("error", (e) => console.error(type, e.message));
  return worker;
});

// Recover terminal failures raised by BullMQ outside the processor (e.g. repeated stalls).
let cursor = "";
let scanning = false;
async function reconcile() {
  if (scanning) return;
  scanning = true;
  try {
    const { rows } = await db.query<RecordRow>(
      "SELECT * FROM jobs WHERE state IN ('QUEUED','PROCESSING') AND type=ANY($1) AND id>$2 ORDER BY id LIMIT 100",
      [selected, cursor],
    );
    for (const row of rows) {
      const job = await queues.get(row.type)!.getJob(runId(row));
      if (job && (await job.getState()) === "failed")
        await transaction(async (tx) => {
          const current = await get(row.id, tx);
          if (
            current?.generation === row.generation &&
            ["QUEUED", "PROCESSING"].includes(current.state)
          )
            await markDead(tx, current, job.failedReason);
        });
    }
    cursor = rows.length === 100 ? rows[99].id : "";
  } finally {
    scanning = false;
  }
}
const timer = setInterval(() => reconcile().catch(console.error), 2000);
async function shutdown() {
  clearInterval(timer);
  await Promise.all(workers.map((w) => w.close()));
  await closeQueues();
  await db.end();
}
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
