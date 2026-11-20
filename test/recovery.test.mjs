import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import { promisify } from "node:util";
import { setTimeout as sleep } from "node:timers/promises";
import { Queue } from "bullmq";
import pg from "pg";
import "dotenv/config";

const run = promisify(execFile);
const suffix = randomUUID().replaceAll("-", "");
const container = "scheduler-recovery-" + suffix;
const schema = "recovery_" + suffix;
const prefix = "recovery-" + suffix;
const postgres = new pg.Client({
  connectionString: process.env.DATABASE_URL,
  user: process.env.PGUSER || "scheduler",
  database: process.env.PGDATABASE || "scheduler",
  password: process.env.PGPASSWORD || "scheduler",
  host: process.env.PGHOST || "127.0.0.1",
});
let port;
let containerCreated = false;
let schemaCreated = false;

async function listen(server) {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return server.address().port;
}

async function until(check, description, timeout = 25000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const result = await check();
    if (result) return result;
    await sleep(100);
  }
  assert.fail("Timed out waiting for " + description);
}

before(async () => {
  await postgres.connect();
  await postgres.query("CREATE SCHEMA " + schema);
  schemaCreated = true;
  // Select a dedicated port that stays the same across docker stop/start.
  const reservation = createServer();
  port = await listen(reservation);
  await new Promise((resolve) => reservation.close(resolve));
  await run("docker", [
    "run",
    "-d",
    "--name",
    container,
    "-p",
    `127.0.0.1:${port}:6379`,
    "redis:7-alpine",
  ]);
  containerCreated = true;
});

after(async () => {
  try {
    if (containerCreated) await run("docker", ["rm", "-f", "-v", container]);
  } finally {
    if (schemaCreated)
      await postgres.query("DROP SCHEMA " + schema + " CASCADE");
    await postgres.end();
  }
});

async function kill(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
}

function launch(t, entry, options = {}) {
  const child = spawn(process.execPath, ["dist/" + entry + ".js"], {
    env: {
      ...process.env,
      PORT: "0",
      BIND_HOST: "127.0.0.1",
      REDIS_HOST: "127.0.0.1",
      REDIS_PORT: String(port),
      PGOPTIONS: "-c search_path=" + schema,
      QUEUE_PREFIX: prefix,
      ...options,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let logs = "";
  for (const stream of [child.stdout, child.stderr])
    stream.on("data", (data) => {
      logs = (logs + data).slice(-8000);
    });
  t.after(() => kill(child));
  return { child, logs: () => logs };
}

async function api(t) {
  const process = launch(t, "server");
  const port = await until(() => {
    if (process.child.exitCode !== null) throw new Error(process.logs());
    return /port: (\d+)/.exec(process.logs())?.[1];
  }, "API startup");
  return { ...process, base: "http://127.0.0.1:" + port };
}

async function provider(t, handle) {
  const server = createServer(handle);
  const port = await listen(server);
  t.after(() => {
    server.closeAllConnections();
    return new Promise((resolve) => server.close(resolve));
  });
  return "http://127.0.0.1:" + port;
}

async function submit(api, type, payload) {
  const response = await fetch(api.base + "/jobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ type, payload }),
  });
  assert.equal(response.status, 202);
  return (await response.json()).id;
}
async function read(api, id) {
  const response = await fetch(api.base + "/jobs/" + id);
  assert.equal(response.status, 200);
  return response.json();
}
function completed(api, id, timeout) {
  return until(
    async () => {
      const job = await read(api, id);
      assert.notEqual(job.state, "DLQ", job.error);
      return job.state === "COMPLETED" && job;
    },
    "completion of " + id,
    timeout,
  );
}

test("an open circuit delays a fresh PDF without spending an attempt", async (t) => {
  const requests = [];
  let healthy = false;
  const url = await provider(t, (req, res) => {
    requests.push({ key: req.headers["idempotency-key"], at: Date.now() });
    res.writeHead(healthy ? 200 : 503).end();
  });
  const app = await api(t);
  for (let i = 0; i < 5; i++)
    await submit(app, "pdf-export", { text: "warmup " + i });
  launch(t, "worker", { WORKER_TYPE: "pdf-export", PDF_EXPORT_URL: url });
  await until(() => requests.length >= 5, "five downstream failures");
  const id = await submit(app, "pdf-export", { text: "fresh" });
  const queue = new Queue("pdf-export", {
    prefix,
    connection: { host: "127.0.0.1", port },
  });
  t.after(() => queue.close());
  await until(
    async () => (await queue.getJobState(id + "-1")) === "delayed",
    "circuit deferral",
  );
  const waiting = await read(app, id);
  assert.equal(waiting.attempt, 0);
  assert.deepEqual(
    waiting.history.map((e) => e.state),
    ["QUEUED"],
  );
  healthy = true;
  const job = await completed(app, id);
  assert.equal(job.attempt, 1);
  assert.deepEqual(
    job.history.map((e) => e.state),
    ["QUEUED", "PROCESSING", "COMPLETED"],
  );
  const actual = requests.filter((r) => r.key === id);
  assert.equal(actual.length, 1);
  assert.ok(actual[0].at - requests[4].at >= 14950);
});

test("worker crash after email acceptance produces one provider delivery", async (t) => {
  const calls = [];
  const deliveries = [];
  const url = await provider(t, (req, res) => {
    const key = req.headers["idempotency-key"];
    calls.push(key);
    if (!deliveries.includes(key)) deliveries.push(key);
    // The first response stays in flight until the accepting worker is killed.
    if (calls.length > 1) res.writeHead(200).end();
  });
  const app = await api(t);
  const options = { WORKER_TYPE: "email-send", EMAIL_SEND_URL: url };
  const worker = launch(t, "worker", options);
  const id = await submit(app, "email-send", {
    to: "test@example.org",
    subject: "Receipt",
    body: "Ready",
  });
  await until(() => calls.length === 1, "provider acceptance");
  assert.equal((await read(app, id)).state, "PROCESSING");
  await kill(worker.child);
  launch(t, "worker", options);
  const job = await completed(app, id, 100000);
  assert.deepEqual(
    job.history.map((e) => e.state),
    ["QUEUED", "PROCESSING", "PROCESSING", "COMPLETED"],
  );
  assert.deepEqual(calls, [id, id]);
  assert.deepEqual(deliveries, [id]);
});

test("PostgreSQL outbox survives a Redis outage and an API restart", async (t) => {
  const first = await api(t);
  await run("docker", ["stop", container]);
  const id = await submit(first, "pdf-export", { text: "Offline" });
  assert.equal((await read(first, id)).state, "QUEUED");
  await kill(first.child);
  const restarted = await api(t);
  const outbox = await postgres.query(
    "SELECT action FROM " + schema + ".dispatch WHERE id=$1",
    [id],
  );
  assert.deepEqual(outbox.rows, [{ action: "enqueue" }]);
  const calls = [];
  const url = await provider(t, (req, res) => {
    calls.push(req.headers["idempotency-key"]);
    res.writeHead(200).end();
  });
  await run("docker", ["start", container]);
  launch(t, "worker", { WORKER_TYPE: "pdf-export", PDF_EXPORT_URL: url });
  assert.equal((await completed(restarted, id)).attempt, 1);
  assert.deepEqual(
    calls.filter((key) => key === id),
    [id],
  );
});
