# Distributed task scheduler

Start PostgreSQL, Redis, the HTTP API and one worker per queue with Docker Compose.

```sh
docker compose up --build -d
docker compose up -d --scale pdf-worker=3
```

Open the [job console](http://127.0.0.1:5000/) to submit a PDF export, inspect its history, and open the output. Select Email to compose a message and download its .eml file with the default worker. Choose **Video upload** to transcode an MP4, MOV, or WebM clip up to 100 MiB. After completion, select **Open MP4**. Uploaded inputs are retained in the shared media directory for retries. Select the video failure demo to watch a missing input trigger retries and dead-letter handling. The failure demo requires the video worker.

To submit a job in PowerShell, use the example below. The API returns HTTP 202 and a status URL after committing the job to PostgreSQL. Workers can be stopped during submission.

```powershell
$api = 'http://127.0.0.1:5000'
$headers = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$job = Invoke-RestMethod "$api/jobs" -Method Post -Headers $headers -ContentType application/json -Body '{"type":"pdf-export","payload":{"text":"Quarterly report"}}'
Invoke-RestMethod "$api/jobs/$($job.id)" | ConvertTo-Json -Depth 5
```

Reuse the same header when retrying a submission. Matching job types and validated payloads return the original HTTP 202 receipt and status URL, even after completion or an API restart. Use the status URL for the current state. Changing the job type or payload under the same key returns HTTP 409. Keys are case-sensitive, accept 1–255 visible ASCII characters, and are unique across the scheduler for the lifetime of the job. Omitting the header creates a fresh job on every submission.

| Queue | Example payload | Attempts | Default output |
| --- | --- | ---: | --- |
| pdf-export | `{"text":"Quarterly report"}` | 3 | One-page PDF with up to 200 printable ASCII characters |
| email-send | `{"to":"ops@example.org","subject":"Report","body":"Ready"}` | 5 | Local .eml spool file |
| video-transcode | `{"source":"clip.mov"}` | 2 | FFmpeg MP4 with H.264 video and AAC audio when present |

The status record contains the output path. Compose stores artifacts under `/app/data/artifacts` in the shared volume. To retrieve a PDF, run `docker compose cp api:/app/data/artifacts/<id>.pdf ./report.pdf`. For video input, run `docker compose exec video-worker mkdir -p /app/data/media` and `docker compose cp ./clip.mov video-worker:/app/data/media/clip.mov` before submission.

Set `EMAIL_SEND_URL` to deliver through an HTTP mail provider that accepts the documented payload. The PDF and video workers also support `PDF_EXPORT_URL` and `VIDEO_TRANSCODE_URL`. Each adapter posts JSON with a 30-second timeout. A successful HTTP response completes the job. The provider should honor the stable job ID in the `Idempotency-Key` header because a worker can restart after the external side effect. After five consecutive downstream failures, the worker opens its circuit for 15 seconds before probing again. While the circuit is open, the worker delays jobs in BullMQ until the cooldown ends. A fresh job stays QUEUED with attempt zero until processing begins.

## Inspection and replay

| Route | Behavior |
| --- | --- |
| `GET /jobs/:id` | Payload, current state, execution result, and ordered transition history across replays |
| `GET /admin/jobs?state=DLQ&offset=0` | JSON operator view, up to 100 records per page. Omit the state to see all jobs |
| `POST /admin/jobs/:id/retry` | Replay a dead-lettered job with a fresh attempt budget. Concurrent duplicate replay returns HTTP 409 |
| `GET /health` | API process health |

The API and admin routes share a loopback-bound port and assume a trusted local operator. A worker records PROCESSING before invoking its handler. Each exception records FAILED. A retry returns the record to QUEUED while BullMQ waits for 80–100% of the exponential delay (1 second, 2 seconds, 4 seconds, and so on). On exhaustion, the worker commits the failure and a pending DLQ publication in the same transaction. The dispatcher marks the record DLQ after Redis acknowledges the entry.

PostgreSQL stores records, events and pending Redis operations. The API drains those operations in the background using a distinct BullMQ ID per job and replay generation. Repeated dispatch after a crash uses the same ID. Redis uses append-only persistence. Workers also reconcile terminal failures caused by stalled jobs. Workers on separate hosts can share PostgreSQL and Redis. The built-in file handlers also require access to the same media and artifact storage.

## PostgreSQL and existing SQLite data

Compose keeps database records in the `postgres_data` volume. The default local database, user and password are all `scheduler`. Set `POSTGRES_PASSWORD` before initializing the volume to choose a different password. For a local Node process, use `DATABASE_URL` or the usual `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD` and `PGDATABASE` settings.

To import an existing installation, stop its API and workers, keep its Redis data and queue prefix, and start PostgreSQL. From the project directory run `npm run build` followed by `node dist/import-sqlite.js path/to/jobs.sqlite`. For a legacy Compose volume, run `docker compose build api` followed by `docker compose run --rm api node dist/import-sqlite.js /app/data/jobs.sqlite`. The importer opens the source read-only and copies jobs, history and pending dispatches into an empty target in one transaction. Start the new API and workers after import.

## Local development and chaos test

Use Node.js 26, install the locked packages with `npm ci`, and start PostgreSQL and Redis with `docker compose up -d postgres redis`. Run `npm run build`, then `npm start` and `npm run worker` in separate terminals. A local worker consumes all three queues by default. Set `WORKER_TYPE` to select one. Local video processing requires FFmpeg on PATH.

```sh
npm test
```

The test creates a temporary PostgreSQL schema, isolated Redis queues, separate API and worker processes, and an HTTP endpoint that fails three times. It checks the observed retry intervals, persisted history, Redis DLQ membership and concurrent manual replay. Use `REDIS_HOST` and `REDIS_PORT` for a non-default Redis instance.

Run `npm run test:recovery` with PostgreSQL running and Docker available to exercise circuit deferral, a worker crash after email acceptance, and a Redis outage across an API restart. The suite creates its own Redis container and temporary database schema. The crash test waits for BullMQ to recover the stalled job. Its mock mail provider deduplicates deliveries using the job ID.
