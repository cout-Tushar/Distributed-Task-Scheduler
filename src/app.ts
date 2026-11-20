import express, { type ErrorRequestHandler } from "express";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { db, create, retry } from "./store.js";
import { submission, states } from "./jobs.js";

import { saveVideo } from "./upload.js";

const app = express();
app.use(express.static(fileURLToPath(new URL("../public/", import.meta.url))));
app.use(express.json({ limit: "128kb" }));
app.get("/health", (_req, res) =>
  res.json({ message: "Server Health is Good", success: true }),
);
app.post(["/jobs", "/jobs/video-upload"], async (req, res) => {
  const key = req.get("Idempotency-Key");
  if (
    key !== undefined &&
    (!/^[\x21-\x7e]{1,255}$/.test(key) ||
      req.headersDistinct["idempotency-key"]?.length !== 1)
  )
    return res.status(400).json({
      error: "Provide one Idempotency-Key of 1 to 255 visible ASCII characters",
    });
  let body = req.body;
  if (req.path.replace(/\/$/, "") === "/jobs/video-upload") {
    try {
      body = {
        type: "video-transcode",
        payload: { source: await saveVideo(req) },
      };
    } catch (error) {
      req.resume();
      if (
        error instanceof Error &&
        "status" in error &&
        typeof error.status === "number"
      )
        return res.status(error.status).json({ error: error.message });
      throw error;
    }
  }
  const input = submission.safeParse(body);
  if (!input.success)
    return res.status(400).json({ error: input.error.issues });
  const id = await create(input.data, key);
  if (id === null)
    return res.status(409).json({
      error: "Idempotency-Key already used for a different job submission",
    });
  return res
    .location("/jobs/" + id)
    .status(202)
    .json({ id, state: "QUEUED" });
});
app.get("/jobs/:id", async (req, res) => {
  const { rows } = await db.query(
    `SELECT j.*, COALESCE(
    (SELECT json_agg(json_build_object('generation',generation,'state',state,'attempt',attempt,'at',at,'detail',detail) ORDER BY seq)
     FROM events WHERE id=j.id),'[]') AS history FROM jobs j WHERE j.id=$1`,
    [req.params.id],
  );
  if (!rows.length) return res.sendStatus(404);
  return res.json(rows[0]);
});
app.get<{ id: string }>(
  ["/jobs/:id/pdf", "/jobs/:id/eml", "/jobs/:id/mp4"],
  async (req, res) => {
    const email = req.path.endsWith("/eml");
    const video = req.path.endsWith("/mp4");
    const ext = video ? ".mp4" : email ? ".eml" : ".pdf";
    const { rows } = await db.query(
      "SELECT id FROM jobs WHERE id=$1 AND type=$2 AND state='COMPLETED' AND result LIKE $3",
      [
        req.params.id,
        video ? "video-transcode" : email ? "email-send" : "pdf-export",
        "%" + ext,
      ],
    );
    if (!rows.length || !/^[0-9a-f-]{36}$/i.test(req.params.id))
      return res.sendStatus(404);
    if (email) res.attachment(req.params.id + ext);
    res.sendFile(req.params.id + ext, {
      root: resolve(process.env.ARTIFACT_DIR || "data/artifacts"),
    });
  },
);
app.get("/admin/jobs", async (req, res) => {
  const state = req.query.state;
  if (state !== undefined && !states.some((s) => s === state))
    return res.status(400).json({ error: "Invalid state", states });
  const offset = Number(req.query.offset ?? 0);
  if (!Number.isSafeInteger(offset) || offset < 0) return res.sendStatus(400);
  const { rows } = await db.query(
    `SELECT id,type,state,attempt,"maxAttempts",generation,"createdAt","updatedAt",error
    FROM jobs WHERE ($1::text IS NULL OR state=$1) ORDER BY "createdAt" DESC,id LIMIT 100 OFFSET $2`,
    [state ?? null, offset],
  );
  return res.json(rows);
});
app.post("/admin/jobs/:id/retry", async (req, res) => {
  const status = await retry(req.params.id);
  if (status === 404) return res.sendStatus(404);
  if (status === 409)
    return res.status(409).json({ error: "Only DLQ jobs can be retried" });
  return res.status(202).json({ id: req.params.id, state: "QUEUED" });
});
const errors: ErrorRequestHandler = (err, _req, res, _next) => {
  if (err.status === 400 || err.status === 413)
    return res.status(err.status).json({ error: "Invalid request body" });
  console.error(err);
  return res.status(500).json({ error: "Internal server error" });
};
app.use(errors);
export default app;
