import { z } from "zod";

export const limits = {
  "pdf-export": 3,
  "email-send": 5,
  "video-transcode": 2,
} as const;
export type JobType = keyof typeof limits;
export const types = Object.keys(limits) as JobType[];
export const states = [
  "QUEUED",
  "PROCESSING",
  "COMPLETED",
  "FAILED",
  "DLQ",
] as const;
export type State = (typeof states)[number];
const line = z
  .string()
  .trim()
  .min(1)
  .max(200)
  .regex(/^[ -~]+$/);
export const submission = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("pdf-export"),
    payload: z.object({ text: line }),
  }),
  z.object({
    type: z.literal("email-send"),
    payload: z.object({
      to: z.email(),
      subject: line,
      body: z.string().min(1).max(32000),
    }),
  }),
  z.object({
    type: z.literal("video-transcode"),
    payload: z.object({
      source: z.string().regex(/^[\w-]+\.(mov|mp4|webm)$/i),
    }),
  }),
]);
export type Submission = z.infer<typeof submission>;
export interface Run {
  id: string;
  generation: number;
}
export const runId = (run: Run) => run.id + "-" + run.generation;
