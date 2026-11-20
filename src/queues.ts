import { Queue } from "bullmq";
import { connection, prefix } from "./config/redis.js";
import { types, limits, runId, type Run, type JobType } from "./jobs.js";
import { db, get, transaction, transition } from "./store.js";

function queue(name: string) {
  const q = new Queue(name, {
    connection: { ...connection, maxRetriesPerRequest: 1 },
    prefix,
  });
  q.on("error", (err) => console.error(name, err.message));
  return q;
}
export const queues = new Map(types.map((type) => [type, queue(type)]));
export const dead = queue("dlq");

interface Command extends Run {
  seq: number;
  type: JobType;
  action: string;
}
export async function dispatch() {
  // Deterministic Redis IDs make a restart after add() safe.
  const commands = db
    .prepare("SELECT * FROM dispatch ORDER BY seq LIMIT 100")
    .all() as unknown as Command[];
  for (const cmd of commands) {
    const { id, generation, type } = cmd;
    const data = { id, generation };
    const jobId = runId(cmd);
    if (cmd.action === "enqueue") {
      await queues.get(type)!.add(type, data, {
        jobId,
        attempts: limits[type],
        backoff: { type: "jittered" },
      });
    } else if (cmd.action === "dead") {
      const row = get(id);
      if (row?.generation === generation && row.state === "FAILED") {
        await dead.add(
          type,
          { ...data, payload: JSON.parse(row.payload), error: row.error },
          { jobId },
        );
        transaction(() => {
          const current = get(id)!;
          if (current.generation === generation && current.state === "FAILED")
            transition(id, "DLQ", current.attempt, current.error);
        });
        if (get(id)?.generation !== generation) await dead.remove(jobId);
      }
    } else {
      await dead.remove(jobId);
    }
    db.prepare("DELETE FROM dispatch WHERE seq=?").run(cmd.seq);
  }
}
export async function closeQueues() {
  await Promise.all([...queues.values(), dead].map((q) => q.close()));
}
