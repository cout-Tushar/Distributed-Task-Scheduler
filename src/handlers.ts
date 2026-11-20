import { mkdir, writeFile, rename, rm } from "node:fs/promises";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { randomUUID } from "node:crypto";
import type { Submission, JobType } from "./jobs.js";

function pdf(text: string) {
  const escaped = text.replace(/[()\\]/g, "\\$&");
  const stream = "BT /F1 12 Tf 50 750 Td (" + escaped + ") Tj ET";
  const objects = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
    "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 1600 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    "<< /Length " + stream.length + " >>\nstream\n" + stream + "\nendstream",
  ];
  let file = "%PDF-1.4\n";
  const offsets = ["0000000000 65535 f "];
  objects.forEach((object, i) => {
    offsets.push(String(file.length).padStart(10, "0") + " 00000 n ");
    file += i + 1 + " 0 obj\n" + object + "\nendobj\n";
  });
  const start = file.length;
  return (
    file +
    "xref\n0 6\n" +
    offsets.join("\n") +
    "\ntrailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n" +
    start +
    "\n%%EOF\n"
  );
}

// Each queue has one processor per worker process, so only one call can probe it.
const breakers = new Map<
  JobType,
  { failures: number; openUntil: number }
>();
export function circuitOpenUntil(type: JobType) {
  return breakers.get(type)?.openUntil ?? 0;
}
async function callService(url: string, input: Submission, id: string) {
  const circuit = breakers.get(input.type) || {
    failures: 0,
    openUntil: 0,
  };
  breakers.set(input.type, circuit);
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": id },
      body: JSON.stringify(input.payload),
      signal: AbortSignal.timeout(30000),
    });
    await response.body?.cancel();
    if (!response.ok) throw new Error("Downstream HTTP " + response.status);
    circuit.failures = circuit.openUntil = 0;
    return "HTTP " + response.status;
  } catch (err) {
    if (++circuit.failures >= 5) circuit.openUntil = Date.now() + 15000;
    throw err;
  }
}
export async function execute(input: Submission, id: string) {
  const url =
    process.env[input.type.toUpperCase().replaceAll("-", "_") + "_URL"];
  if (url) return callService(url, input, id);
  const dir = process.env.ARTIFACT_DIR || "data/artifacts";
  await mkdir(dir, { recursive: true });
  const extension = {
    "pdf-export": "pdf",
    "email-send": "eml",
    "video-transcode": "mp4",
  }[input.type];
  const output = join(dir, id + "." + extension);
  const tmp = output + "." + randomUUID() + "." + extension;
  try {
    switch (input.type) {
      case "pdf-export":
        await writeFile(tmp, pdf(input.payload.text));
        break;
      case "email-send": {
        const { to, subject, body } = input.payload;
        await writeFile(
          tmp,
          `To: ${to}\r\nSubject: ${subject}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n${body}`,
        );
        break;
      }
      case "video-transcode":
        await promisify(execFile)("ffmpeg", [
          "-nostdin",
          "-loglevel",
          "error",
          "-i",
          join(process.env.MEDIA_DIR || "data/media", input.payload.source),
          "-c:v",
          "libx264",
          "-c:a",
          "aac",
          tmp,
        ]);
    }
    await rename(tmp, output);
    return output;
  } finally {
    await rm(tmp, { force: true });
  }
}
