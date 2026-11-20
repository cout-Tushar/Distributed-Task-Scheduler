import type { Request } from "express";
import { createHash, randomUUID } from "node:crypto";
import { link, mkdir, open, rm } from "node:fs/promises";
import { join } from "node:path";

const maxBytes = 100 * 1024 * 1024;
const extensions = new Map([
  ["video/mp4", "mp4"],
  ["video/quicktime", "mov"],
  ["video/webm", "webm"],
]);

export async function saveVideo(req: Request) {
  const ext = extensions.get(req.get("content-type")?.toLowerCase() ?? "");
  if (!ext)
    throw Object.assign(new Error("Choose an MP4, MOV, or WebM video."), {
      status: 415,
    });
  const tooLarge = () =>
    Object.assign(new Error("Video exceeds the 100 MiB upload limit."), {
      status: 413,
    });
  if (Number(req.get("content-length")) > maxBytes) throw tooLarge();

  const dir = process.env.MEDIA_DIR || "data/media";
  await mkdir(dir, { recursive: true });
  const temp = join(dir, randomUUID() + ".part");
  const hash = createHash("sha256");
  const file = await open(temp, "wx");
  try {
    let bytes = 0;
    async function* chunks() {
      for await (const chunk of req.iterator({ destroyOnReturn: false })) {
        bytes += chunk.length;
        if (bytes > maxBytes) throw tooLarge();
        hash.update(chunk);
        yield chunk;
      }
    }
    await file.writeFile(chunks());
    if (!bytes)
      throw Object.assign(new Error("Choose a nonempty video file."), {
        status: 400,
      });
    const source = hash.digest("hex") + "." + ext;
    // Publish only complete files. Identical retries produce the same job payload.
    try {
      await link(temp, join(dir, source));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    return source;
  } finally {
    await file.close();
    await rm(temp, { force: true });
  }
}
