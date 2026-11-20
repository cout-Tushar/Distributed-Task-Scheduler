import { DatabaseSync } from "node:sqlite";
import { db, transaction } from "./store.js";

const file = process.argv[2] || process.env.JOB_DB || "data/jobs.sqlite";
try {
  const source = new DatabaseSync(file, { readOnly: true });
  try {
    source.exec("BEGIN");
    const tables = [
      {
        name: "jobs",
        columns: [
          "id",
          "type",
          "payload",
          "state",
          "attempt",
          "maxAttempts",
          "generation",
          "createdAt",
          "updatedAt",
          "error",
          "result",
        ],
        order: "id",
      },
      {
        name: "events",
        columns: [
          "seq",
          "id",
          "generation",
          "state",
          "attempt",
          "at",
          "detail",
        ],
        order: "seq",
      },
      {
        name: "dispatch",
        columns: ["seq", "id", "generation", "type", "action"],
        order: "seq",
      },
    ];
    const counts = await transaction(async (tx) => {
      await tx.query(
        "LOCK TABLE jobs,events,dispatch IN ACCESS EXCLUSIVE MODE",
      );
      if ((await tx.query("SELECT 1 FROM jobs LIMIT 1")).rowCount)
        throw new Error("Import requires an empty PostgreSQL job store.");
      const counts: Record<string, number> = {};
      for (const { name, columns, order } of tables) {
        const fields = columns.map((c) => '"' + c + '"').join(",");
        const params = columns.map((_, i) => "$" + (i + 1)).join(",");
        counts[name] = 0;
        for (const row of source
          .prepare(`SELECT ${fields} FROM ${name} ORDER BY ${order}`)
          .iterate()) {
          await tx.query(
            `INSERT INTO ${name} (${fields}) VALUES (${params})`,
            columns.map((c) => row[c]),
          );
          counts[name]++;
        }
        if (order === "seq" && counts[name]) {
          await tx.query(
            `SELECT setval(pg_get_serial_sequence($1,'seq'),max(seq)) FROM ${name}`,
            [name],
          );
        }
      }
      return counts;
    });
    console.log("Imported SQLite records", counts);
  } finally {
    source.close();
  }
} finally {
  await db.end();
}
