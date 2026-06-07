# Streamline Ingestion Runner

GitHub Actions–based ingestion worker for [Streamline](https://watchpickr.com). Processes the shared PostgreSQL `ingestion_jobs` queue, grows the TMDB catalog (Hindi-first after 2010 by default), and enriches stored titles with OMDb ratings on a separate daily budget.

The Streamline **API** runs API-only on Render. This repository owns scheduled enqueue scripts and queue processing — it never calls `POST /api/ingest/drain`.

## Architecture

| Component | Runs on | Purpose |
|-----------|---------|---------|
| `process-queue.yml` | GitHub Actions (~every 45 min) | `npm run start:worker` with `RUN_CRON_SCHEDULER=false` |
| `enqueue-daily.yml` | `10 */6 * * *` | Top up `CATALOG_REFRESH` jobs to daily target |
| `enqueue-delta.yml` | `20 */6 * * *` | High-priority `DISCOVER` delta refresh |
| `maintenance.yml` | `30 2 * * *` / `0 3 * * *` | Snapshot cleanup + OMDb `DEEP_ENRICH` enqueue |

## Setup

### 1. GitHub secrets

In **Settings → Secrets and variables → Actions**, add:

- `DATABASE_URL` — Postgres URL with `sslmode=require` and a small pool (e.g. `connection_limit=2`)
- `TMDB_API_KEY`
- `OMDB_API_KEY`

Never commit real credentials. Copy `.env.example` to `.env` for local runs only.

### 2. Local development

```bash
npm ci
cp .env.example .env   # fill in placeholders
npm run build
RUN_CRON_SCHEDULER=false npm run start:worker
```

Scheduler scripts (one-shot):

```bash
npm run ingestion:enqueue-daily
npm run ingestion:enqueue-delta
npm run ingestion:cleanup
npm run ingestion:enqueue-omdb
```

### 3. Enable workflows

Push to GitHub, add secrets, then enable Actions schedules. Start with **Process ingestion queue** (manual dispatch) before enabling all cron workflows.

## Configuration

Key environment variables (see `.env.example` for the full list):

| Variable | Default | Purpose |
|----------|---------|---------|
| `INGESTION_DAILY_TARGET` | `20000` | Net-new titles goal per UTC day |
| `INGESTION_QUEUE_HIGH_WATERMARK` | `10000` | Stop enqueueing when backlog reaches this |
| `INGESTION_MIN_RELEASE_YEAR` | `2010` | Prioritize titles from this year onward |
| `INGESTION_LANGUAGE_WEIGHTS` | `hi:100,en:20,...` | Weighted language discovery lanes |
| `TMDB_REQUESTS_PER_SECOND` | `8` | Sustained TMDB rate (backs off on 429) |
| `INGESTION_OMDB_DAILY_LIMIT` | `950` | Regular OMDb enrichment cap per day |

## Schema sync

`prisma/schema.prisma` must stay aligned with the [streamline-backend](https://github.com/your-org/streamline-backend) API repository. Job payloads and ingestion tables are shared contracts.

## Security

- Public-repo safe: no secrets in source, workflows, or logs
- Do not call protected API drain endpoints from CI
- Redacted TMDB URLs in error logs

## License

MIT
