# Streamline Ingestion Runner

Background worker for [Streamline](https://watchpickr.com). Runs on GitHub Actions, processes the shared Postgres ingestion queue, and keeps the catalog growing on a schedule.

The public API runs separately on Render — this repo only handles scheduled jobs and queue processing.

## Setup

1. Add Actions secrets from `.env.example` (`DATABASE_URL` and the listed API keys).
2. `npm ci && npm run build`
3. Run **Process ingestion queue** manually once before relying on cron schedules.

Local worker:

```bash
cp .env.example .env
npm run build
RUN_CRON_SCHEDULER=false npm run start:worker
```

## Notes

- Keep `prisma/schema.prisma` in sync with the main Streamline backend.
- Do not commit `.env` or real credentials.

## Queue lane throughput and logs

GitHub's scheduled cron is routinely delayed by hours, so the queue lane no longer depends on `*/15` firing on time:

- Each burst runs for 53 minutes (`INGESTION_WORKER_BURST_SECONDS=3180`) and then stops gracefully.
- When the burst ends with jobs still queued, the workflow dispatches the next burst itself (`Continue queue processing` step). Chaining stops when the queue drains or after `INGESTION_WORKER_MAX_CHAIN_HOPS` hops (default 24). Set it to `0` to disable chaining.
- Every run emits an `ingestion_worker_summary` JSON line every minute and once more when it stops, plus an `ingestion_job_failed` line for each failure.

Check a run without opening the UI:

```bash
gh run view <run-id> --log | grep -E 'ingestion_worker_summary|ingestion_job_failed'
```

The final summary is also written to `worker-summary.json` and printed by the workflow, so a run with `processed: 0` is clearly visible as idle rather than a silent success.

## License

MIT
