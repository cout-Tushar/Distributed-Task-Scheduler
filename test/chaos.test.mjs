import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { Redis } from "ioredis";
import { Queue } from "bullmq";

async function until(fn) {
  for (let i = 0; i < 250; i++) {
    const value = await fn();
    if (value) return value;
    await sleep(80);
  }
  assert.fail("Timed out waiting for scheduler state");
}

test(
  "worker retries a failing downstream, parks the job and replays it once",
  { timeout: 55000 },
  async (t) => {
    const connection = {
      host: process.env.REDIS_HOST || "127.0.0.1",
      port: Number(process.env.REDIS_PORT || 6379),
      maxRetriesPerRequest: 1,
    };
    const probe = new Redis({
      ...connection,
      lazyConnect: true,
      retryStrategy: () => null,
    });
    probe.on("error", () => {});
    try {
      await probe.connect();
      await probe.ping();
    } finally {
      probe.disconnect();
    }
    const dir = await mkdtemp(join(tmpdir(), "scheduler-"));
    const calls = [];
    const downstream = createServer((req, res) => {
      calls.push({ at: Date.now(), key: req.headers["idempotency-key"] });
      res.writeHead(calls.length <= 3 ? 503 : 200).end();
    });
    downstream.listen(0, "127.0.0.1");
    await once(downstream, "listening");
    const prefix = "chaos-" + randomUUID();
    const env = {
      ...process.env,
      PORT: "0",
      QUEUE_PREFIX: prefix,
      JOB_DB: join(dir, "jobs.sqlite"),
      WORKER_TYPE: "pdf-export",
      PDF_EXPORT_URL: "http://127.0.0.1:" + downstream.address().port,
    };
    const children = [];
    const queues = ["pdf-export", "dlq", "email-send", "video-transcode"].map(
      (name) => new Queue(name, { connection, prefix }),
    );
    t.after(async () => {
      await Promise.all(
        children.map(async (child) => {
          if (child.exitCode === null && child.signalCode === null) {
            const done = once(child, "exit");
            child.kill();
            await done;
          }
        }),
      );
      downstream.closeAllConnections();
      downstream.close();
      try {
        await Promise.all(queues.map((q) => q.obliterate({ force: true })));
      } finally {
        await Promise.all(queues.map((q) => q.close()));
        await rm(dir, { recursive: true, force: true });
      }
    });
    const launch = (entry) => {
      const child = spawn(process.execPath, ["dist/" + entry + ".js"], {
        env,
        stdio: ["ignore", "pipe", "inherit"],
      });
      children.push(child);
      return child;
    };
    const api = launch("server");
    let output = "";
    api.stdout.on("data", (data) => {
      output += data;
    });
    const port = await until(() => /port: (\d+)/.exec(output)?.[1]);
    const base = "http://127.0.0.1:" + port;
    const response = await fetch(base + "/jobs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        type: "pdf-export",
        payload: { text: "Receipt" },
      }),
    });
    assert.equal(response.status, 202);
    const { id } = await response.json();
    const read = async () => (await fetch(base + "/jobs/" + id)).json();
    assert.equal((await read()).state, "QUEUED");
    launch("worker");
    const failed = await until(async () => {
      const r = await read();
      return r.state === "DLQ" && r;
    });
    assert.deepEqual(
      failed.history.map((e) => e.state),
      [
        "QUEUED",
        "PROCESSING",
        "FAILED",
        "QUEUED",
        "PROCESSING",
        "FAILED",
        "QUEUED",
        "PROCESSING",
        "FAILED",
        "DLQ",
      ],
    );
    assert.equal(failed.attempt, 3);
    assert.equal(calls.length, 3);
    for (let i = 1; i < 3; i++) {
      const delay = calls[i].at - calls[i - 1].at;
      assert.ok(
        delay >= 800 * 2 ** (i - 1) - 10 && delay <= 1000 * 2 ** (i - 1) + 650,
        "observed backoff " + delay,
      );
    }
    assert.equal(await queues[1].getJobState(id + "-1"), "waiting");
    const replay = () =>
      fetch(base + "/admin/jobs/" + id + "/retry", { method: "POST" });
    assert.deepEqual(
      (await Promise.all([replay(), replay()])).map((r) => r.status).sort(),
      [202, 409],
    );
    const completed = await until(async () => {
      const r = await read();
      return r.state === "COMPLETED" && r;
    });
    assert.equal(completed.generation, 2);
    assert.equal(completed.attempt, 1);
    assert.equal(await queues[1].getJobState(id + "-1"), "unknown");
    assert.deepEqual(
      calls.map((c) => c.key),
      Array(4).fill(id),
    );
  },
);
