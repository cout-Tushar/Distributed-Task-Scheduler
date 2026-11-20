import "dotenv/config";
import { db } from "./store.js";
import app from "./app.js";
import { dispatch, closeQueues } from "./queues.js";

const server = app.listen(
  Number(process.env.PORT || 5000),
  process.env.BIND_HOST || "127.0.0.1",
  () => {
    console.log("Scheduler API listening", server.address());
  },
);
let closing = false;
async function pump() {
  try {
    await dispatch();
  } catch (error) {
    console.error("Queue dispatch", error);
  }
  if (!closing) setTimeout(pump, 200);
}
void pump();
function shutdown() {
  closing = true;
  server.close(async () => {
    await closeQueues();
    await db.end();
  });
}
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);
