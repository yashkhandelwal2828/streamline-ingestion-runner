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

## License

MIT
