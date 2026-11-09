import type { PrismaClient } from '@prisma/client';
import { buildFetchJobData } from './jobQueue';
import type { ExportEntry } from '../tmdb';

/**
 * Daily catalog planner. Spends a fixed TMDB title budget across three lanes, newest first:
 *   new      – popular titles released in the last few months (and upcoming), worldwide plus Indian languages
 *   repair   – rows saved as stubs (charts, search) or without artwork, so filters have genres/runtime/etc.
 *   backfill – every remaining TMDB title, most popular first, resuming from a checkpoint each run
 * Unused share flows to the next lane. New titles stop when the database nears its storage cap.
 */

export const PLANNER_SOURCES = { new: 'catalog_new', repair: 'catalog_repair', backfill: 'catalog_backfill' } as const;
const PRIORITY = { new: 260, repair: 240, backfill: 210 } as const;
const CHECKPOINT_KEY = 'catalog_planner_checkpoint';
const INDIAN_LANGUAGES = ['hi', 'ta', 'te', 'ml', 'kn', 'bn', 'mr', 'pa', 'gu'];
const DAY_MS = 86_400_000;

export type PlannerConfig = {
  dailyBudget: number;
  runsPerDay: number;
  shares: { new: number; repair: number };
  newWindowDays: number;
  upcomingDays: number;
  maxDiscoverPages: number;
  dbSoftLimitBytes: number;
  maxExportScanPerRun: number;
};

const intEnv = (value: string | undefined, fallback: number, min = 0) => {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) && parsed >= min ? parsed : fallback;
};

export const readPlannerConfig = (env: Record<string, string | undefined> = process.env): PlannerConfig => ({
  dailyBudget: intEnv(env.INGESTION_TMDB_DAILY_TITLE_BUDGET, 4000, 1),
  runsPerDay: intEnv(env.INGESTION_PLANNER_RUNS_PER_DAY, 4, 1),
  shares: {
    new: intEnv(env.INGESTION_PLANNER_NEW_SHARE_PCT, 40) / 100,
    repair: intEnv(env.INGESTION_PLANNER_REPAIR_SHARE_PCT, 15) / 100,
  },
  newWindowDays: intEnv(env.INGESTION_PLANNER_NEW_WINDOW_DAYS, 120, 1),
  upcomingDays: intEnv(env.INGESTION_PLANNER_UPCOMING_DAYS, 60),
  maxDiscoverPages: intEnv(env.INGESTION_PLANNER_MAX_DISCOVER_PAGES, 15, 1),
  dbSoftLimitBytes: intEnv(env.INGESTION_DB_SOFT_LIMIT_MB, 450, 1) * 1024 * 1024,
  maxExportScanPerRun: intEnv(env.INGESTION_PLANNER_MAX_EXPORT_SCAN, 150_000, 1000),
});

/** Budget for this run: what is left today, spread over the runs still to come. */
export const runBudget = (config: PlannerConfig, usedToday: number, now: Date) => {
  const remaining = Math.max(0, config.dailyBudget - usedToday);
  const hoursLeft = 24 - now.getUTCHours();
  const runsLeft = Math.max(1, Math.ceil((hoursLeft / 24) * config.runsPerDay));
  return Math.min(remaining, Math.ceil(remaining / runsLeft));
};

export const laneTargets = (budget: number, shares: PlannerConfig['shares']) => {
  const newTarget = Math.floor(budget * shares.new);
  const repairTarget = Math.floor(budget * shares.repair);
  return { new: newTarget, repair: repairTarget };
};

/** Walks a popularity-sorted export from `offset`, wrapping once, and returns ids not in `isMissing`. */
export const scanExport = async (
  entries: ExportEntry[],
  offset: number,
  target: number,
  maxScan: number,
  findMissing: (ids: number[]) => Promise<number[]>,
  chunkSize = 1000,
) => {
  const picked: number[] = [];
  if (entries.length === 0 || target <= 0) return { picked, nextOffset: 0, scanned: 0 };
  let position = Math.max(0, offset) % entries.length;
  let scanned = 0;
  while (picked.length < target && scanned < Math.min(maxScan, entries.length)) {
    const size = Math.min(chunkSize, entries.length - position, maxScan - scanned);
    const ids = entries.slice(position, position + size).map((entry) => entry.id);
    scanned += ids.length;
    position = (position + ids.length) % entries.length;
    const missing = new Set(await findMissing(ids));
    for (const id of ids) {
      if (missing.has(id)) picked.push(id);
      if (picked.length >= target) break;
    }
  }
  return { picked, nextOffset: position, scanned };
};

export type PlannerDeps = {
  prisma: PrismaClient;
  enqueue: (data: Record<string, any>) => Promise<unknown>;
  discover: (mediaType: 'movie' | 'tv', page: number, options: { from: string; to: string; language?: string }) => Promise<Array<{ id: number; poster_path?: string | null }>>;
  exportEntries: (mediaType: 'movie' | 'tv', now: Date) => Promise<{ dateKey: string; entries: ExportEntry[] }>;
  now?: () => Date;
  log?: (event: string, data: Record<string, unknown>) => void;
};

const isoDay = (date: Date) => date.toISOString().slice(0, 10);
const startOfUtcDay = (now: Date) => new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));

const findMissingIds = async (prisma: PrismaClient, ids: number[], mediaType: 'movie' | 'tv') => {
  if (ids.length === 0) return [];
  const rows = await prisma.movie.findMany({ where: { mediaType, tmdbId: { in: ids } }, select: { tmdbId: true } });
  const existing = new Set(rows.map((row) => row.tmdbId));
  return ids.filter((id) => !existing.has(id));
};

const enqueueFetch = async (deps: PlannerDeps, lane: keyof typeof PLANNER_SOURCES, mediaType: 'movie' | 'tv', tmdbId: number, now: Date) =>
  deps.enqueue(buildFetchJobData({ source: PLANNER_SOURCES[lane], tmdbId, mediaType, region: 'IN', priority: PRIORITY[lane], createdAt: now }));

export const runCatalogPlanner = async (deps: PlannerDeps, config: PlannerConfig = readPlannerConfig()) => {
  const now = deps.now ? deps.now() : new Date();
  const log = deps.log ?? ((event, data) => console.log(JSON.stringify({ event, ...data })));
  const { prisma } = deps;

  const usedToday = await prisma.ingestionJob.count({ where: { type: 'FETCH', createdAt: { gte: startOfUtcDay(now) } } });
  const budget = runBudget(config, usedToday, now);
  const [{ bytes }] = await prisma.$queryRawUnsafe<Array<{ bytes: bigint }>>('SELECT pg_database_size(current_database()) AS bytes');
  const dbBytes = Number(bytes);
  const storageFull = dbBytes >= config.dbSoftLimitBytes;
  const summary: Record<string, unknown> = { budget, usedToday, dbMegabytes: Math.round(dbBytes / 1048576), storageFull };
  if (budget === 0) {
    summary.skippedReason = 'daily_budget_used';
    log('catalog_planner_summary', summary);
    return summary;
  }

  const targets = laneTargets(budget, config.shares);
  let spent = 0;

  // 1. New and upcoming titles, newest relevant first. Worldwide pass, then each Indian language.
  let newCount = 0;
  if (!storageFull) {
    const from = isoDay(new Date(now.getTime() - config.newWindowDays * DAY_MS));
    const to = isoDay(new Date(now.getTime() + config.upcomingDays * DAY_MS));
    const passes: Array<{ mediaType: 'movie' | 'tv'; language?: string }> = [];
    for (const language of [undefined, ...INDIAN_LANGUAGES]) {
      passes.push({ mediaType: 'movie', language }, { mediaType: 'tv', language });
    }
    const seen = new Set<string>();
    for (const pass of passes) {
      let fullyKnownPages = 0;
      for (let page = 1; page <= config.maxDiscoverPages && newCount < targets.new; page += 1) {
        const results = (await deps.discover(pass.mediaType, page, { from, to, language: pass.language }))
          .filter((item) => item.poster_path && !seen.has(`${pass.mediaType}:${item.id}`));
        if (results.length === 0) break;
        results.forEach((item) => seen.add(`${pass.mediaType}:${item.id}`));
        const missing = await findMissingIds(prisma, results.map((item) => item.id), pass.mediaType);
        fullyKnownPages = missing.length === 0 ? fullyKnownPages + 1 : 0;
        for (const id of missing) {
          if (newCount >= targets.new) break;
          await enqueueFetch(deps, 'new', pass.mediaType, id, now);
          newCount += 1;
        }
        // Three pages in a row we already have: this pass is caught up.
        if (fullyKnownPages >= 3) break;
      }
      if (newCount >= targets.new) break;
    }
  }
  spent += newCount;

  // 2. Repair stubs (never fetched in full) and recent titles still missing artwork. Each row at most weekly.
  const repairTarget = Math.min(targets.repair + (storageFull ? targets.new : 0), budget - spent);
  const repairRows = repairTarget > 0
    ? await prisma.$queryRawUnsafe<Array<{ tmdbId: number; mediaType: string }>>(
      `SELECT "tmdbId", "mediaType" FROM "Movie"
       WHERE "updatedAt" < NOW() - INTERVAL '7 days'
         AND (
           (cardinality(genres) = 0 AND "runtimeMinutes" IS NULL AND "totalSeasons" IS NULL AND cardinality(director) = 0)
           OR (("posterPath" IS NULL OR "posterPath" = '') AND "releaseDate" > NOW() - INTERVAL '2 years')
         )
       ORDER BY "createdAt" DESC, "releaseDate" DESC NULLS LAST
       LIMIT $1`, repairTarget)
    : [];
  for (const row of repairRows) {
    await enqueueFetch(deps, 'repair', row.mediaType === 'tv' ? 'tv' : 'movie', row.tmdbId, now);
  }
  spent += repairRows.length;

  // 3. Backfill everything else, most popular first, resuming where the last run stopped.
  let backfillCount = 0;
  const backfillTarget = storageFull ? 0 : budget - spent;
  if (backfillTarget > 0) {
    const checkpointRow = await prisma.ingestionSnapshot.findFirst({ where: { source: 'INTERNAL', externalId: CHECKPOINT_KEY }, orderBy: { createdAt: 'desc' } });
    const checkpoint = (checkpointRow?.payload ?? {}) as Record<string, number>;
    const nextCheckpoint: Record<string, number> = { ...checkpoint };
    // Movies get 60%, TV 40%: TMDB lists about five films per series.
    const split = { movie: Math.ceil(backfillTarget * 0.6), tv: Math.floor(backfillTarget * 0.4) };
    for (const mediaType of ['movie', 'tv'] as const) {
      const exported = await deps.exportEntries(mediaType, now);
      const scan = await scanExport(exported.entries, checkpoint[`${mediaType}Offset`] ?? 0, split[mediaType], config.maxExportScanPerRun,
        (ids) => findMissingIds(prisma, ids, mediaType));
      for (const id of scan.picked) await enqueueFetch(deps, 'backfill', mediaType, id, now);
      backfillCount += scan.picked.length;
      nextCheckpoint[`${mediaType}Offset`] = scan.nextOffset;
      nextCheckpoint[`${mediaType}Total`] = exported.entries.length;
      summary[`${mediaType}Export`] = { scanned: scan.scanned, offset: scan.nextOffset, total: exported.entries.length };
    }
    await prisma.ingestionSnapshot.deleteMany({ where: { source: 'INTERNAL', externalId: CHECKPOINT_KEY } });
    await prisma.ingestionSnapshot.create({ data: {
      source: 'INTERNAL', externalId: CHECKPOINT_KEY, region: 'IN', payload: nextCheckpoint,
      expiresAt: new Date(now.getTime() + 20 * 365 * DAY_MS),
    } });
  }
  spent += backfillCount;

  Object.assign(summary, { enqueued: { new: newCount, repair: repairRows.length, backfill: backfillCount }, spent });
  log('catalog_planner_summary', summary);
  return summary;
};
