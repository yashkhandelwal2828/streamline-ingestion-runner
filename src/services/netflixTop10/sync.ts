import { buildFetchJobData } from '../ingestion/jobQueue';
import { parseNetflixTsv, type NetflixParseResult, type NetflixTop10Row } from './parse';
import { normalizeTitleKey, pickBestCandidate, type TmdbSearchCandidate } from './matcher';

export const NETFLIX_TOP10_SOURCE = 'netflix_top10';
export const NETFLIX_COUNTRIES_URL = 'https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv';
export const NETFLIX_GLOBAL_URL = 'https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv';
/** Unmatched titles are searched again after this long (TMDB may add them later). */
export const UNMATCHED_RECHECK_MS = 14 * 24 * 60 * 60 * 1000;
/** Above the pipeline's deep-enrich threshold so OMDb ratings are fetched for chart titles. */
export const NETFLIX_FETCH_PRIORITY = 250;

type Delegate = Record<string, (args: any) => Promise<any>>;

export type NetflixSyncDeps = {
  prisma: {
    netflixTop10Entry: Delegate;
    netflixTitleMatch: Delegate;
    movie: Delegate;
    $transaction: (operations: any[]) => Promise<any>;
  };
  store: { enqueueJob: (data: Record<string, any>) => Promise<any> };
  fetchText: (url: string) => Promise<string>;
  search: (title: string, mediaType: 'movie' | 'tv') => Promise<TmdbSearchCandidate[]>;
  now?: () => Date;
  log?: (message: string, data?: Record<string, unknown>) => void;
};

export type NetflixSyncOptions = {
  weeks?: number;
  dryRun?: boolean;
  countriesUrl?: string;
  globalUrl?: string;
  /** Safety valve on TMDB searches per run (a first multi-week backfill can need thousands). */
  maxSearches?: number;
};

export type NetflixSyncSummary = {
  weeks: string[];
  scopes: number;
  entries: number;
  skippedRows: number;
  uniqueTitles: number;
  cachedMatches: number;
  searched: number;
  searchesDeferred: number;
  matchedTitles: number;
  unmatchedTitles: number;
  entryMatchRate: number;
  entriesWritten: number;
  fetchJobsEnqueued: number;
  dryRun: boolean;
};

const titleId = (row: Pick<NetflixTop10Row, 'showTitle' | 'mediaType'>) => `${row.mediaType}\u0000${normalizeTitleKey(row.showTitle)}`;

export const syncNetflixTop10 = async (deps: NetflixSyncDeps, options: NetflixSyncOptions = {}): Promise<NetflixSyncSummary> => {
  const now = deps.now ?? (() => new Date());
  const log = deps.log ?? (() => undefined);
  const weeksToKeep = options.weeks ?? 1;
  const dryRun = options.dryRun ?? false;
  const maxSearches = options.maxSearches ?? 3000;

  const [countriesText, globalText] = await Promise.all([
    deps.fetchText(options.countriesUrl ?? NETFLIX_COUNTRIES_URL),
    deps.fetchText(options.globalUrl ?? NETFLIX_GLOBAL_URL),
  ]);
  const countries: NetflixParseResult = parseNetflixTsv(countriesText, 'countries', { weeks: weeksToKeep });
  const global: NetflixParseResult = parseNetflixTsv(globalText, 'global', { weeks: weeksToKeep });
  const rows = [...countries.rows, ...global.rows];
  if (rows.length === 0) {
    throw new Error('Netflix Top 10 files parsed but contained no usable rows; refusing to continue');
  }

  // Unique titles, remembering the earliest chart week so the matcher can reject later releases.
  const titles = new Map<string, { title: string; mediaType: 'movie' | 'tv'; key: string; earliestWeek: string }>();
  for (const row of rows) {
    const id = titleId(row);
    const existing = titles.get(id);
    if (!existing) {
      titles.set(id, { title: row.showTitle, mediaType: row.mediaType, key: normalizeTitleKey(row.showTitle), earliestWeek: row.week });
    } else if (row.week < existing.earliestWeek) {
      existing.earliestWeek = row.week;
    }
  }

  const tmdbIdByTitle = new Map<string, number | null>();
  let cachedMatches = 0;
  let searched = 0;
  let searchesDeferred = 0;

  if (!dryRun) {
    const cached: Array<{ titleKey: string; mediaType: string; tmdbId: number | null; status: string; checkedAt: Date }> =
      await deps.prisma.netflixTitleMatch.findMany({
        where: { OR: [...titles.values()].map((entry) => ({ titleKey: entry.key, mediaType: entry.mediaType })) },
      });
    const cacheById = new Map<string, (typeof cached)[number]>(
      cached.map((entry): [string, (typeof cached)[number]] => [`${entry.mediaType}\u0000${entry.titleKey}`, entry]),
    );
    const recheckBefore = now().getTime() - UNMATCHED_RECHECK_MS;

    for (const [id, entry] of titles) {
      const hit = cacheById.get(id);
      if (hit && (hit.status === 'MATCHED' || hit.checkedAt.getTime() > recheckBefore)) {
        tmdbIdByTitle.set(id, hit.status === 'MATCHED' ? hit.tmdbId : null);
        cachedMatches += 1;
        continue;
      }
      if (searched >= maxSearches) {
        searchesDeferred += 1;
        tmdbIdByTitle.set(id, null);
        continue;
      }
      searched += 1;
      const candidates = await deps.search(entry.title, entry.mediaType);
      const best = pickBestCandidate(candidates, { title: entry.title, weekDate: entry.earliestWeek });
      tmdbIdByTitle.set(id, best?.tmdbId ?? null);
      await deps.prisma.netflixTitleMatch.upsert({
        where: { titleKey_mediaType: { titleKey: entry.key, mediaType: entry.mediaType } },
        update: { tmdbId: best?.tmdbId ?? null, status: best ? 'MATCHED' : 'UNMATCHED', matchedTitle: best?.matchedTitle ?? null, checkedAt: now() },
        create: { titleKey: entry.key, mediaType: entry.mediaType, tmdbId: best?.tmdbId ?? null, status: best ? 'MATCHED' : 'UNMATCHED', matchedTitle: best?.matchedTitle ?? null, checkedAt: now() },
      });
    }
  }

  const matchedRows = dryRun ? 0 : rows.filter((row) => tmdbIdByTitle.get(titleId(row)) != null).length;
  const matchedTitles = [...tmdbIdByTitle.values()].filter((value) => value != null).length;

  let entriesWritten = 0;
  let fetchJobsEnqueued = 0;

  if (!dryRun) {
    // Replace each (scope, week) atomically so reruns are idempotent and a corrected file fully supersedes.
    const groups = new Map<string, NetflixTop10Row[]>();
    for (const row of rows) {
      const key = `${row.scope}|${row.week}`;
      const group = groups.get(key);
      if (group) group.push(row); else groups.set(key, [row]);
    }

    const operations: any[] = [];
    for (const [key, group] of groups) {
      const [scope, week] = key.split('|');
      const weekDate = new Date(`${week}T00:00:00.000Z`);
      operations.push(deps.prisma.netflixTop10Entry.deleteMany({ where: { scope, week: weekDate } }));
      operations.push(deps.prisma.netflixTop10Entry.createMany({
        data: group.map((row) => ({
          scope: row.scope,
          week: weekDate,
          category: row.category,
          rank: row.rank,
          weeksInTop10: row.weeksInTop10,
          showTitle: row.showTitle,
          seasonTitle: row.seasonTitle,
          mediaType: row.mediaType,
          tmdbId: tmdbIdByTitle.get(titleId(row)) ?? null,
        })),
      }));
      entriesWritten += group.length;
    }
    await deps.prisma.$transaction(operations);

    // Titles missing from the catalog go through the normal FETCH -> NORMALIZE -> DEEP_ENRICH pipeline
    // so posters and ratings come from TMDB/OMDb exactly as for every other title.
    for (const mediaType of ['movie', 'tv'] as const) {
      const ids = [...new Set(rows
        .filter((row) => row.mediaType === mediaType)
        .map((row) => tmdbIdByTitle.get(titleId(row)))
        .filter((value): value is number => typeof value === 'number'))];
      if (ids.length === 0) continue;
      const existing: Array<{ tmdbId: number }> = await deps.prisma.movie.findMany({
        where: { mediaType, tmdbId: { in: ids } },
        select: { tmdbId: true },
      });
      const have = new Set(existing.map((movie) => movie.tmdbId));
      for (const tmdbId of ids.filter((id) => !have.has(id))) {
        await deps.store.enqueueJob(buildFetchJobData({
          source: NETFLIX_TOP10_SOURCE,
          tmdbId,
          mediaType,
          region: 'GLOBAL',
          priority: NETFLIX_FETCH_PRIORITY,
          createdAt: now(),
        }));
        fetchJobsEnqueued += 1;
      }
    }
  }

  const summary: NetflixSyncSummary = {
    weeks: [...new Set([...countries.weeks, ...global.weeks])].sort().reverse(),
    scopes: new Set(rows.map((row) => row.scope)).size,
    entries: rows.length,
    skippedRows: countries.skippedRows + global.skippedRows,
    uniqueTitles: titles.size,
    cachedMatches,
    searched,
    searchesDeferred,
    matchedTitles,
    unmatchedTitles: dryRun ? 0 : titles.size - matchedTitles,
    entryMatchRate: rows.length ? Number((matchedRows / rows.length).toFixed(4)) : 0,
    entriesWritten,
    fetchJobsEnqueued,
    dryRun,
  };
  log('netflix_top10_sync_summary', summary);
  return summary;
};
