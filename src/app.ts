import express, { type ErrorRequestHandler } from "express";
import { db, create, retry } from "./store.js";
import { submission, states } from "./jobs.js";

const app = express();
app.use(express.json({ limit: "128kb" }));
app.get("/health", (_req, res) =>
  res.json({ message: "Server Health is Good", success: true }),
);
app.post("/jobs", async (req, res) => {
  const input = submission.safeParse(req.body);
  if (!input.success)
    return res.status(400).json({ error: input.error.issues });
  const key = req.get("Idempotency-Key");
  if (
    key !== undefined &&
    (!/^[\x21-\x7e]{1,255}$/.test(key) ||
      req.headersDistinct["idempotency-key"]?.length !== 1)
  )
    return res
      .status(400)
      .json({
        error:
          "Provide one Idempotency-Key of 1 to 255 visible ASCII characters",
      });
  const id = await create(input.data, key);
  if (id === null)
    return res
      .status(409)
      .json({
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
