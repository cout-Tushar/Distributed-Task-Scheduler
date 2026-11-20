import { Queue } from "bullmq";
import { connection, prefix } from "./config/redis.js";
import { types, limits, runId, type Run, type JobType } from "./jobs.js";
import { get, transaction, transition } from "./store.js";

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
  for (let i = 0; i < 100; i++) {
    const dispatched = await transaction(async (tx) => {
      // Preserve command order per job while independent jobs dispatch concurrently.
      const { rows } =
        await tx.query<Command>(`SELECT d.* FROM dispatch d WHERE NOT EXISTS
        (SELECT 1 FROM dispatch prior WHERE prior.id=d.id AND prior.seq<d.seq)
        ORDER BY d.seq LIMIT 1 FOR UPDATE OF d SKIP LOCKED`);
      const cmd = rows[0];
      if (!cmd) return false;
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
        const row = await get(id, tx);
        if (row?.generation === generation && row.state === "FAILED") {
          await dead.add(
            type,
            { ...data, payload: row.payload, error: row.error },
            { jobId },
          );
          await transition(tx, id, "DLQ", row.attempt, row.error);
        }
      } else {
        await dead.remove(jobId);
      }
      await tx.query("DELETE FROM dispatch WHERE seq=$1", [cmd.seq]);
      return true;
    });
    if (!dispatched) break;
  }
}
export async function closeQueues() {
  await Promise.all([...queues.values(), dead].map((q) => q.close()));
}
