# Bundle B — Richer Movie Details (Ingestion Runner)

**Repo:** Streamline Ingestion Runner (`streamline-ingestion-runner`)  
**Companion docs:**
- Backend (schema owner): `streamline-backend/.../docs/implementation/bundle-b-richer-movie-details.md`
- Frontend: `streamline-swipe-view/docs/implementation/bundle-b-richer-movie-details.md`

**Scope:** Mirror backend ingestion changes so `DEEP_ENRICH` jobs write `plotFull` and `NORMALIZE` writes `tagline`. Extend OMDb enqueue to backfill plots. **No API routes in this repo.**

---

## 1. Role of this repo

The runner shares the **same Postgres database** as the Render API. It processes:

| Job type | Bundle B change |
|----------|-----------------|
| `NORMALIZE` | Persist `tagline` from TMDB details |
| `DEEP_ENRICH` | Persist `plotFull` from OMDb `Plot` (already fetched with `plot=full`) |
| `ingestion:enqueue-omdb` | Queue titles missing `imdbRating` **or** `plotFull` |

GitHub Actions workflows (`process-queue`, `maintenance` OMDb enqueue) pick up changes automatically after deploy — no workflow YAML edits required for Bundle B.

---

## 2. Prerequisites (blocking)

1. **Backend migration applied** on shared DB — columns `Movie.plotFull` and `Movie.tagline` must exist before runner writes them.
2. Copy migration SQL from backend repo OR run identical `prisma migrate deploy` here after syncing `schema.prisma`.

```bash
# Verify columns exist (psql or Neon console)
\d "Movie"
# Expect: plotFull | text | nullable
#         tagline  | text | nullable
```

---

## 3. Cross-repo contract

Read the backend doc §2 for the full API contract. This repo only implements **persistence**:

| Column | Written by | Source |
|--------|------------|--------|
| `tagline` | `NORMALIZE` / `buildMovieRecordFromTmdb` | `details.tagline` |
| `plotFull` | `DEEP_ENRICH` / `buildDeepEnrichUpdate` | `omdbData.Plot` via `parseOmdbPlot` |
| `overview` | unchanged | TMDB `overview` |

**Shared helper:** Copy `src/services/movieSynopsis.ts` from backend (or duplicate `parseOmdbPlot` inline in pipeline — prefer copying the file to keep repos aligned).

---

## 4. Implementation tasks

### Task R0 — Sync Prisma schema

**File:** `prisma/schema.prisma`

Add to `Movie` model (must match backend exactly):

```prisma
plotFull  String?
tagline   String?
```

```bash
npm run build   # prisma generate + tsc
```

If backend already applied migration to shared DB, you only need schema sync + generate — **do not create a second migration** with different timestamp unless deploying to a fresh DB.

---

### Task R1 — Copy synopsis helper

**New file:** `src/services/movieSynopsis.ts`

Copy from backend repo after Task B2. Export at minimum:

- `parseOmdbPlot(plot: unknown): string | null`
- `buildDisplaySynopsis` (optional here — API uses it; runner only needs `parseOmdbPlot`)

---

### Task R2 — TMDB normalize: `tagline`

**File:** `src/services/ingestion/pipeline.ts` — function `buildMovieRecordFromTmdb`

Add to returned object:

```typescript
tagline:
  typeof details.tagline === 'string' && details.tagline.trim().length > 0
    ? details.tagline.trim()
    : null,
```

TMDB movie/TV detail responses include `tagline` without extra `append_to_response` params.

---

### Task R3 — OMDb enrich: `plotFull`

**File:** `src/services/ingestion/pipeline.ts` — function `buildDeepEnrichUpdate`

```typescript
import { parseOmdbPlot } from '../movieSynopsis';

const buildDeepEnrichUpdate = (omdbData: Record<string, any>) => {
  const rtRatingString = omdbData.Ratings?.find((entry: any) => entry.Source === 'Rotten Tomatoes')?.Value;
  const imdbVotesNumeric = /* existing logic */;
  const plotFull = parseOmdbPlot(omdbData.Plot);

  return {
    imdbRating: /* existing */,
    rottenTomatoesRating: /* existing */,
    metascore: /* existing */,
    imdbVotes: /* existing */,
    imdbVotesNumeric: /* existing */,
    ...(plotFull ? { plotFull } : {}),
  };
};
```

`processDeepEnrichJob` (~line 1068) already calls `buildDeepEnrichUpdate` and `store.updateMovieById` — no change needed there.

`src/services/omdb.ts` already uses `plot: 'full'` — no change.

---

### Task R4 — OMDb enqueue backfill query

**File:** `src/services/ingestion/omdbEnqueue.ts` — `findMoviesNeedingOmdbEnrichment`

Replace `where` clause:

```typescript
where: {
  imdbId: { not: null },
  OR: [
    { imdbRating: null },
    { plotFull: null },
  ],
},
orderBy: [
  { imdbRating: 'asc' },
  { releaseDate: 'desc' },
  { updatedAt: 'asc' },
],
```

**Behavior:**
- New titles without ratings: queued first (existing behavior).
- Titles with ratings but no `plotFull`: queued until plot backfill completes.
- Daily cap still enforced by `getOmdbDailyLimit()` (~950/day) in `enqueueOmdbBackfill`.
- Dedupe key `deep_enrich:movie:{id}` prevents duplicate active jobs.

---

### Task R5 — Tests

Copy or adapt tests from backend:

**File:** `tests/ingestion-pipeline.test.js` (if present) or add `tests/movie-synopsis.test.js`

Minimum cases:
1. `parseOmdbPlot('N/A')` → `null`
2. `buildDeepEnrichUpdate` includes `plotFull` for multi-sentence plot
3. Enqueue selects movie with `imdbRating: 7.5, plotFull: null`

```bash
node --test tests/movie-synopsis.test.js
node --test tests/ingestion-pipeline.test.js
npm run build
```

---

## 5. Files touched (checklist)

| File | Action |
|------|--------|
| `prisma/schema.prisma` | Sync columns |
| `src/services/movieSynopsis.ts` | **New** (copy from backend) |
| `src/services/ingestion/pipeline.ts` | `tagline` + `plotFull` |
| `src/services/ingestion/omdbEnqueue.ts` | Extended WHERE |
| `tests/*` | New/updated cases |

**No changes needed:**
- `.github/workflows/*` (unless you want a one-off manual workflow_dispatch for plot backfill — optional)
- `src/worker.ts`
- `src/services/tmdb.ts` (tagline already in details response)

---

## 6. Local verification

```bash
cp .env.example .env
# Fill DATABASE_URL, TMDB_API_KEY, OMDB_API_KEY

npm ci && npm run build

# Dry-run enqueue (does not process jobs)
npm run ingestion:enqueue-omdb

# Process a short burst
RUN_CRON_SCHEDULER=false timeout 120 npm run start:worker
```

**Inspect one enriched row:**

```sql
SELECT id, title, length(overview) AS overview_len, length("plotFull") AS plot_len, "tagline"
FROM "Movie"
WHERE "plotFull" IS NOT NULL
ORDER BY "updatedAt" DESC
LIMIT 5;
```

Expect `plot_len` > `overview_len` for many Hollywood titles; Hindi/regional may still be short or null (OMDb coverage varies).

---

## 7. Production rollout

| Step | Action |
|------|--------|
| 1 | Confirm backend migration on shared Postgres |
| 2 | Merge runner PR (schema + pipeline + omdbEnqueue) |
| 3 | `npm run build` in CI passes |
| 4 | Wait for `maintenance` workflow OMDb enqueue (`0 3 * * *`) OR manual dispatch |
| 5 | `process-queue` / `process-enrich` workflows drain `DEEP_ENRICH` jobs |
| 6 | Monitor OMDb budget logs: `OMDb enqueue complete: created=N` |

**Backfill ETA:** ~950 plots/day. Catalog with 87k titles that already have ratings → ~90 days for full plot backfill if every row has OMDb data. Titles without OMDb plot stay on TMDB `overview`.

**Priority tuning (optional, post-Bundle B):** Add `plotFull: null` + `imdbVotesNumeric desc` ordering to enqueue Bollywood blockbusters first — not required for MVP.

---

## 8. Observability

Log lines to watch in GitHub Actions:

```
OMDb enqueue complete: created=..., skipped=..., remainingBudget=...
```

Worker completion payload for enrich jobs should show `imdbId` without `skipped` when plot is saved.

**Health query (run weekly):**

```sql
SELECT
  COUNT(*) FILTER (WHERE "plotFull" IS NOT NULL) AS with_plot,
  COUNT(*) FILTER (WHERE "imdbId" IS NOT NULL AND "imdbRating" IS NOT NULL AND "plotFull" IS NULL) AS rated_missing_plot,
  COUNT(*) FILTER (WHERE "tagline" IS NOT NULL) AS with_tagline
FROM "Movie";
```

---

## 9. Sync discipline with backend

After every backend ingestion change:

1. Diff `prisma/schema.prisma`
2. Diff `src/services/ingestion/pipeline.ts` and `omdbEnqueue.ts`
3. Diff `src/services/movieSynopsis.ts`

Long-term: shared npm package or CI schema-diff check (see `docs/streamline-ingestion-runner-prd.md`).

---

## 10. Rollback

Revert runner PR. API and DB columns remain; enrich jobs stop writing `plotFull`. No data loss for existing plots.
