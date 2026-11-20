import { Worker, type Job } from "bullmq";
import { connection, prefix } from "./config/redis.js";
import { execute } from "./handlers.js";
import { limits, types, runId, type Run, type JobType } from "./jobs.js";
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
function markDead(row: RecordRow, reason: string) {
  transition(row.id, "FAILED", row.attempt, reason);
  schedule(row, row.type, "dead");
}
async function processJob(job: Job<Run>) {
  const { id, generation } = job.data;
  const attempt = job.attemptsMade + 1;
  const claim = transaction(() => {
    const current = get(id);
    if (
      !current ||
      current.generation !== generation ||
      ["COMPLETED", "DLQ", "FAILED"].includes(current.state)
    )
      return;
    transition(id, "PROCESSING", attempt);
    const { seq } = db
      .prepare("SELECT max(seq) AS seq FROM events WHERE id=?")
      .get(id)!;
    return { row: current, seq };
  });
  if (!claim) return;
  const { row } = claim;
  // A stalled worker may finish after another worker has taken over the same attempt.
  function ownsAttempt() {
    return (
      db.prepare("SELECT max(seq) AS seq FROM events WHERE id=?").get(id)!
        .seq === claim!.seq
    );
  }
  try {
    const result = await execute(
      { type: row.type, payload: JSON.parse(row.payload) },
      id,
    );
    transaction(() => {
      if (ownsAttempt()) transition(id, "COMPLETED", attempt, null, result);
    });
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    transaction(() => {
      const current = get(id)!;
      if (!ownsAttempt()) return;
      if (attempt >= row.maxAttempts) markDead(current, reason);
      else {
        transition(id, "FAILED", attempt, reason);
        transition(id, "QUEUED", attempt, reason);
      }
    });
    throw err;
  }
}
const workers = selected.map((type) => {
  const worker = new Worker<Run>(type, processJob, {
    connection,
    prefix,
    maxStartedAttempts: limits[type] + 1,
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
    const rows = db
      .prepare(
        "SELECT * FROM jobs WHERE state IN ('QUEUED','PROCESSING') AND id>? ORDER BY id LIMIT 100",
      )
      .all(cursor) as unknown as RecordRow[];
    for (const row of rows) {
      if (!selected.includes(row.type)) continue;
      const job = await queues.get(row.type)!.getJob(runId(row));
      if (job && (await job.getState()) === "failed")
        transaction(() => {
          const current = get(row.id)!;
          if (
            current.generation === row.generation &&
            ["QUEUED", "PROCESSING"].includes(current.state)
          )
            markDead(current, job.failedReason);
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
}
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
