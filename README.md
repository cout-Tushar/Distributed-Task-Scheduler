# Distributed task scheduler

Start Redis, the HTTP API and one worker per queue with Docker Compose.

```sh
docker compose up --build -d
docker compose up -d --scale pdf-worker=3
```

Submit a job in PowerShell. The API returns HTTP 202 and a status URL after committing the job to SQLite. Workers can be stopped during submission.

```powershell
$api = 'http://127.0.0.1:5000'
$job = Invoke-RestMethod "$api/jobs" -Method Post -ContentType application/json -Body '{"type":"pdf-export","payload":{"text":"Quarterly report"}}'
Invoke-RestMethod "$api/jobs/$($job.id)" | ConvertTo-Json -Depth 5
```

| Queue | Example payload | Attempts | Default output |
| --- | --- | ---: | --- |
| pdf-export | `{"text":"Quarterly report"}` | 3 | One-page PDF with up to 200 printable ASCII characters |
| email-send | `{"to":"ops@example.org","subject":"Report","body":"Ready"}` | 5 | Local .eml spool file |
| video-transcode | `{"source":"clip.mov"}` | 2 | FFmpeg MP4 with H.264 video and AAC audio when present |

The status record contains the output path. Compose stores artifacts under `/app/data/artifacts` in the shared volume. To retrieve a PDF, run `docker compose cp api:/app/data/artifacts/<id>.pdf ./report.pdf`. For video input, run `docker compose exec video-worker mkdir -p /app/data/media` and `docker compose cp ./clip.mov video-worker:/app/data/media/clip.mov` before submission.

Set `EMAIL_SEND_URL` to deliver through an HTTP mail provider that accepts the documented payload. The PDF and video workers also support `PDF_EXPORT_URL` and `VIDEO_TRANSCODE_URL`. Each adapter posts JSON with a 30-second timeout. A successful HTTP response completes the job. The provider should honor the stable job ID in the `Idempotency-Key` header because a worker can restart after the external side effect. After five consecutive downstream failures, the worker opens its circuit for 15 seconds before probing again.

## Inspection and replay

| Route | Behavior |
| --- | --- |
| `GET /jobs/:id` | Payload, current state, execution result, and ordered transition history across replays |
| `GET /admin/jobs?state=DLQ&offset=0` | JSON operator view, up to 100 records per page. Omit the state to see all jobs |
| `POST /admin/jobs/:id/retry` | Replay a dead-lettered job with a fresh attempt budget. Concurrent duplicate replay returns HTTP 409 |
| `GET /health` | API process health |

The API and admin routes share a loopback-bound port and assume a trusted local operator. A worker records PROCESSING before invoking its handler. Each exception records FAILED. A retry returns the record to QUEUED while BullMQ waits for 80–100% of the exponential delay (1 second, 2 seconds, 4 seconds, and so on). On exhaustion, the worker commits the failure and a pending DLQ publication in the same transaction. The dispatcher marks the record DLQ after Redis acknowledges the entry.

SQLite stores records, events and pending Redis operations. The API drains those operations in the background using a distinct BullMQ ID per job and replay generation. Repeated dispatch after a crash uses the same ID. Redis uses append-only persistence. Workers also reconcile terminal failures caused by stalled jobs. The SQLite WAL volume supports multiple local processes on one Docker host. Multi-host deployment requires a shared transactional database service.

## Local development and chaos test

Use Node.js 26, install the locked packages with `npm ci`, and start Redis with `docker compose up -d redis`. Run `npm run build`, then `npm start` and `npm run worker` in separate terminals. A local worker consumes all three queues by default. Set `WORKER_TYPE` to select one. Local video processing requires FFmpeg on PATH.

```sh
npm test
```

The test starts a temporary SQLite database, isolated Redis queues, separate API and worker processes, and an HTTP endpoint that fails three times. It checks the observed retry intervals, persisted history, Redis DLQ membership and concurrent manual replay. Use `REDIS_HOST` and `REDIS_PORT` for a non-default Redis instance.
