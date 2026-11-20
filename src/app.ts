import express, { type ErrorRequestHandler } from "express";
import { db, create, get, retry } from "./store.js";
import { submission, states } from "./jobs.js";

const app = express();
app.use(express.json({ limit: "128kb" }));
app.get("/health", (_req, res) =>
  res.json({ message: "Server Health is Good", success: true }),
);
app.post("/jobs", (req, res) => {
  const input = submission.safeParse(req.body);
  if (!input.success)
    return res.status(400).json({ error: input.error.issues });
  const id = create(input.data);
  return res
    .location("/jobs/" + id)
    .status(202)
    .json({ id, state: "QUEUED" });
});
app.get("/jobs/:id", (req, res) => {
  const row = get(req.params.id);
  if (!row) return res.sendStatus(404);
  const history = db
    .prepare(
      "SELECT generation,state,attempt,at,detail FROM events WHERE id=? ORDER BY seq",
    )
    .all(row.id);
  return res.json({ ...row, payload: JSON.parse(row.payload), history });
});
app.get("/admin/jobs", (req, res) => {
  const state = req.query.state;
  if (state !== undefined && !states.some((s) => s === state))
    return res.status(400).json({ error: "Invalid state", states });
  const offset = Number(req.query.offset ?? 0);
  if (!Number.isSafeInteger(offset) || offset < 0) return res.sendStatus(400);
  const rows = db
    .prepare(
      "SELECT id,type,state,attempt,maxAttempts,generation,createdAt,updatedAt,error FROM jobs WHERE (? IS NULL OR state=?) ORDER BY createdAt DESC,id LIMIT 100 OFFSET ?",
    )
    .all((state as string) ?? null, (state as string) ?? null, offset);
  return res.json(rows);
});
app.post("/admin/jobs/:id/retry", (req, res) => {
  if (!get(req.params.id)) return res.sendStatus(404);
  if (!retry(req.params.id))
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
