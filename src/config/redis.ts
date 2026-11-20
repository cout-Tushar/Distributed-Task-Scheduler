import "dotenv/config";

export const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
};
export const prefix = process.env.QUEUE_PREFIX || "scheduler";
