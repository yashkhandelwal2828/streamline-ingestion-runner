import { PrismaClient } from '@prisma/client';
import { getRatings } from '../omdb';
import { discoverMovies, discoverTV, getDailyExportIds, getMovieDetails, getTVDetails } from '../tmdb';
import {
  buildDeepEnrichJobData,
  buildFetchJobData,
  buildNormalizeJobData,
  buildPlatformAvailabilityRecords,
  buildRawSnapshotData,
  createPrismaIngestionStore,
  normalizeRegion,
} from './jobQueue';
import { buildLanguageTargets, getIngestionLanguageMode, getIngestionLanguageWeights, LanguageWeight } from './languageConfig';
import { getOmdbDailyLimit, getOmdbRetryReserve, isFallbackToGlobalEnabled } from './queueConfig';

type IngestionJob = {
  id: string;
  type: string;
  source: string;
  region: string;
  priority: number;
  attempts: number;
  maxAttempts: number;
  shadowMode?: boolean;
  payload: Record<string, any>;
};

type IngestionStore = {
  claimNextJob: (now?: Date) => Promise<IngestionJob | null>;
  recoverStaleRunningJobs?: (
    now?: Date,
    options?: { staleAfterMinutes?: number; limit?: number },
  ) => Promise<number>;
  completeJob: (jobId: string, details?: Record<string, any>) => Promise<any>;
  rescheduleJob: (jobId: string, details: { errorMessage: string; availableAt: Date }) => Promise<any>;
  deferJob?: (jobId: string, details: { availableAt: Date; reason?: string | null }) => Promise<any>;
  deadLetterJob: (jobId: string, details: { errorMessage: string }) => Promise<any>;
  createSnapshot: (snapshot: Record<string, any>) => Promise<any>;
  getSnapshotById?: (snapshotId: string) => Promise<{ id: string; payload: Record<string, any> } | null>;
  deleteSnapshotById?: (snapshotId: string) => Promise<number>;
  enqueueJob: (job: Record<string, any>) => Promise<any>;
  upsertMovie?: (movie: Record<string, any>) => Promise<{ id: number } & Record<string, any>>;
  getMovieCount?: () => Promise<number>;
  listExistingTmdbIds?: (tmdbIds: number[], mediaType: 'movie' | 'tv') => Promise<number[]>;
  getInternalState?: (key: string) => Promise<{ value: Record<string, any>; updatedAt: Date } | null>;
  setInternalState?: (key: string, value: unknown, now?: Date) => Promise<any>;
  getOmdbUsage?: (now?: Date) => Promise<{ regular: number; retries: number; total: number }>;
  replacePlatformAvailability?: (movieId: number, records: Array<Record<string, any>>) => Promise<any>;
  findMovieById?: (movieId: number) => Promise<Record<string, any> | null>;
  updateMovieById?: (movieId: number, data: Record<string, any>) => Promise<any>;
  updateMovieByTmdbId?: (tmdbId: number, mediaType: 'movie' | 'tv', data: Record<string, any>) => Promise<any>;
};

type PipelineDeps = {
  now?: () => Date;
  discoverMovies?: typeof discoverMovies;
  discoverTV?: typeof discoverTV;
  getDailyExportIds?: typeof getDailyExportIds;
  getMovieDetails?: typeof getMovieDetails;
  getTVDetails?: typeof getTVDetails;
  getRatings?: typeof getRatings;
};

const HIGH_INTENT_PRIORITY = 300;
const DEEP_ENRICH_PRIORITY_THRESHOLD = 200;
const RETRYABLE_PRISMA_CONNECTION_ERROR_CODES = new Set(['P1001', 'P1017', 'P2024']);
const RETRYABLE_PRISMA_CONNECTION_ERROR_MESSAGES = [
  "can't reach database server",
  'engine is not yet connected',
  'p1001',
  'server has closed the connection',
  'connection terminated',
  'connection closed',
];
const POOL_TIMEOUT_RETRY_COOLDOWN_MS = 2_000;
const DATABASE_UNAVAILABLE_RETRY_COOLDOWN_MS = 60 * 1000;
const STORAGE_LIMIT_RETRY_COOLDOWN_MS = 5 * 60 * 1000;

const parseBooleanEnv = (
  value: string | undefined,
  defaultValue: boolean,
) => {
  if (value === undefined) {
    return defaultValue;
  }

  const normalized = String(value).trim().toLowerCase();
  if (['true', '1', 'yes', 'on'].includes(normalized)) {
    return true;
  }
  if (['false', '0', 'no', 'off'].includes(normalized)) {
    return false;
  }

  return defaultValue;
};

const shouldStoreOmdbSnapshots = (env: Record<string, string | undefined> = process.env) => {
  return parseBooleanEnv(env.INGESTION_STORE_OMDB_SNAPSHOTS, false);
};

const shouldDeleteTmdbSnapshotAfterNormalize = (env: Record<string, string | undefined> = process.env) => {
  return parseBooleanEnv(env.INGESTION_DELETE_TMDB_SNAPSHOT_AFTER_NORMALIZE, true);
};

const getIngestionMinReleaseYear = (env: Record<string, string | undefined> = process.env) => {
  const parsed = Number.parseInt(String(env.INGESTION_MIN_RELEASE_YEAR ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed < 1900 || parsed > 2100) {
    return null;
  }

  return parsed;
};

const getLanguageDeepScanMaxPages = (env: Record<string, string | undefined> = process.env) => {
  const parsed = Number.parseInt(String(env.INGESTION_LANGUAGE_DEEP_SCAN_MAX_PAGES ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 100;
  }

  return Math.min(parsed, 500);
};

const getLanguageBroadScanMaxPages = (env: Record<string, string | undefined> = process.env) => {
  const parsed = Number.parseInt(String(env.INGESTION_LANGUAGE_BROAD_SCAN_MAX_PAGES ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 250;
  }

  return Math.min(parsed, 500);
};

const isReleaseDateBeforeMinYear = (releaseDate: Date | null, minYear: number | null) => {
  if (!releaseDate || minYear === null) {
    return false;
  }

  return releaseDate.getUTCFullYear() < minYear;
};

const defaultDeps: Required<PipelineDeps> = {
  now: () => new Date(),
  discoverMovies,
  discoverTV,
  getDailyExportIds,
  getMovieDetails,
  getTVDetails,
  getRatings,
};

const BOOTSTRAP_STATE_KEY = 'bootstrap_export_checkpoint';

const getBackoffDate = (now: Date, attempts: number) => {
  const next = new Date(now);
  next.setUTCMinutes(next.getUTCMinutes() + Math.min(attempts * 5, 30));
  return next;
};

const discoverIds = async (
  discover: (page: number, options?: Record<string, any>) => Promise<Array<{ id: number }>>,
  pagesToScan: number,
  targetCount: number,
  options: Record<string, any> = {},
) => {
  const ids: number[] = [];

  for (let page = 1; page <= pagesToScan && ids.length < targetCount; page += 1) {
    const results = await discover(page, options);
    for (const item of results ?? []) {
      if (ids.length >= targetCount) {
        break;
      }

      ids.push(item.id);
    }
  }

  return Array.from(new Set(ids));
};

const discoverMissingIds = async (
  store: IngestionStore,
  discover: (page: number, options?: Record<string, any>) => Promise<Array<{ id: number }>>,
  mediaType: 'movie' | 'tv',
  pagesToScan: number,
  targetCount: number,
  options: Record<string, any> = {},
  seen: Set<number> = new Set(),
) => {
  if (targetCount <= 0) {
    return [];
  }

  if (!store.listExistingTmdbIds) {
    const discoveredIds = await discoverIds(discover, pagesToScan, targetCount, options);
    return discoveredIds.filter((id) => {
      if (seen.has(id)) {
        return false;
      }
      seen.add(id);
      return true;
    });
  }

  const ids: number[] = [];

  for (let page = 1; page <= pagesToScan && ids.length < targetCount; page += 1) {
    const results = await discover(page, options);
    const pageIds = Array.from(
      new Set(
        (results ?? [])
          .map((item) => item.id)
          .filter((id) => Number.isInteger(id) && !seen.has(id)),
      ),
    );

    if (pageIds.length === 0) {
      continue;
    }

    const missingIds = await listMissingTmdbIds(store, pageIds, mediaType);
    for (const id of missingIds) {
      if (ids.length >= targetCount) {
        break;
      }

      if (!seen.has(id)) {
        seen.add(id);
        ids.push(id);
      }
    }
  }

  return ids;
};

const buildRecentFirstDiscoveryPasses = (
  baseOptions: Record<string, any>,
  minReleaseYear: number | null,
  now: Date,
) => {
  const passes: Array<Record<string, any>> = [
    {
      ...baseOptions,
      sortBy: baseOptions.sortBy ?? 'popularity.desc',
    },
  ];

  if (minReleaseYear === null) {
    return passes;
  }

  const releaseDateGte = `${minReleaseYear}-01-01`;
  const releaseDateLte = baseOptions.releaseDateLte ?? now.toISOString().slice(0, 10);
  const broadPass = {
    ...baseOptions,
    releaseDateGte,
    releaseDateLte,
    sortBy: 'primary_release_date.desc',
  };

  const matchesExistingPass =
    passes[0].releaseDateGte === broadPass.releaseDateGte &&
    passes[0].releaseDateLte === broadPass.releaseDateLte &&
    passes[0].sortBy === broadPass.sortBy;

  if (!matchesExistingPass) {
    passes.push(broadPass);
  }

  return passes;
};

const discoverIdsWithLanguageWeights = async (
  store: IngestionStore,
  discover: (page: number, options?: Record<string, any>) => Promise<Array<{ id: number }>>,
  mediaType: 'movie' | 'tv',
  pagesToScan: number,
  targetCount: number,
  languageWeights: LanguageWeight[],
  baseOptions: Record<string, any> = {},
  options: { minReleaseYear?: number | null; now?: Date } = {},
) => {
  if (targetCount <= 0) {
    return [];
  }

  const targets = buildLanguageTargets(targetCount, languageWeights);
  if (targets.length === 0) {
    return discoverMissingIds(store, discover, mediaType, pagesToScan, targetCount, baseOptions);
  }

  const ids: number[] = [];
  const seen = new Set<number>();
  const languagePagesToScan = Math.max(pagesToScan, getLanguageDeepScanMaxPages());
  const broadLanguagePagesToScan = Math.max(languagePagesToScan, getLanguageBroadScanMaxPages());
  const discoveryPasses = buildRecentFirstDiscoveryPasses(
    baseOptions,
    options.minReleaseYear ?? null,
    options.now ?? new Date(),
  );

  for (const target of targets) {
    let acceptedForLanguage = 0;
    for (const discoveryPass of discoveryPasses) {
      if (ids.length >= targetCount) {
        break;
      }

      const remainingForLanguage = target.target - acceptedForLanguage;
      if (remainingForLanguage <= 0) {
        break;
      }

      const isBroadMinYearPass =
        options.minReleaseYear !== null &&
        options.minReleaseYear !== undefined &&
        discoveryPass.releaseDateGte === `${options.minReleaseYear}-01-01` &&
        discoveryPass.sortBy === 'primary_release_date.desc';

      const discoveredIds = await discoverMissingIds(
        store,
        discover,
        mediaType,
        isBroadMinYearPass ? broadLanguagePagesToScan : languagePagesToScan,
        remainingForLanguage,
        {
          ...discoveryPass,
          withOriginalLanguage: target.code,
        },
        seen,
      );

      for (const id of discoveredIds) {
        ids.push(id);
        acceptedForLanguage += 1;
      }
    }
  }

  if (ids.length < targetCount && isFallbackToGlobalEnabled()) {
    const fallbackIds = await discoverMissingIds(
      store,
      discover,
      mediaType,
      options.minReleaseYear !== null && options.minReleaseYear !== undefined
        ? broadLanguagePagesToScan
        : pagesToScan,
      targetCount - ids.length,
      baseOptions,
      seen,
    );
    for (const id of fallbackIds) {
      if (ids.length >= targetCount) {
        break;
      }

      ids.push(id);
    }
  }

  return ids.slice(0, targetCount);
};

const chunk = <T>(items: T[], size: number) => {
  const output: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    output.push(items.slice(index, index + size));
  }
  return output;
};

const getWindowedSlice = (ids: number[], offset: number, maxItems: number) => {
  if (ids.length === 0 || maxItems <= 0) {
    return { items: [] as number[], nextOffset: 0 };
  }

  const normalizedOffset = Math.max(0, offset % ids.length);
  const firstWindow = ids.slice(normalizedOffset, normalizedOffset + maxItems);
  if (firstWindow.length === maxItems || normalizedOffset === 0) {
    return {
      items: firstWindow,
      nextOffset: (normalizedOffset + firstWindow.length) % ids.length,
    };
  }

  const remaining = maxItems - firstWindow.length;
  const wrapped = ids.slice(0, remaining);
  const merged = firstWindow.concat(wrapped);

  return {
    items: merged,
    nextOffset: wrapped.length,
  };
};

const listMissingTmdbIds = async (store: IngestionStore, tmdbIds: number[], mediaType: 'movie' | 'tv') => {
  if (!store.listExistingTmdbIds) {
    throw new Error('listMissingTmdbIds requires listExistingTmdbIds store method');
  }

  if (tmdbIds.length === 0) {
    return [];
  }

  const existing = new Set<number>();
  const tmdbIdChunks = chunk(Array.from(new Set(tmdbIds)), 500);
  for (const tmdbIdChunk of tmdbIdChunks) {
    const rows = await store.listExistingTmdbIds(tmdbIdChunk, mediaType);
    for (const row of rows) {
      existing.add(row);
    }
  }

  return tmdbIds.filter((id) => !existing.has(id));
};

const collectMissingIdsFromExport = async (
  store: IngestionStore,
  ids: number[],
  mediaType: 'movie' | 'tv',
  startOffset: number,
  targetCount: number,
  scanWindow: number,
  maxScanItems: number,
) => {
  if (ids.length === 0 || targetCount <= 0) {
    return {
      missingIds: [] as number[],
      nextOffset: 0,
      scannedCount: 0,
    };
  }

  const normalizedOffset = Math.max(0, startOffset % ids.length);
  const windowSize = Math.max(1, Math.min(scanWindow, ids.length));
  const scanLimit = Math.max(windowSize, Math.min(ids.length, maxScanItems));

  let offset = normalizedOffset;
  let scannedCount = 0;
  const missingIds: number[] = [];
  const uniqueMissing = new Set<number>();

  while (scannedCount < scanLimit && missingIds.length < targetCount) {
    const currentWindowSize = Math.min(windowSize, scanLimit - scannedCount);
    const slice = getWindowedSlice(ids, offset, currentWindowSize);
    offset = slice.nextOffset;
    scannedCount += slice.items.length;

    const missingInSlice = await listMissingTmdbIds(store, slice.items, mediaType);
    for (const tmdbId of missingInSlice) {
      if (uniqueMissing.has(tmdbId)) {
        continue;
      }

      uniqueMissing.add(tmdbId);
      missingIds.push(tmdbId);
      if (missingIds.length >= targetCount) {
        break;
      }
    }

    if (offset === normalizedOffset) {
      break;
    }
  }

  return {
    missingIds,
    nextOffset: offset,
    scannedCount,
  };
};

const buildMovieRecordFromTmdb = (details: Record<string, any>, mediaType: 'movie' | 'tv') => {
  const imdbId = mediaType === 'movie' ? details.imdb_id ?? null : details.external_ids?.imdb_id ?? null;
  const credits = details.credits ?? {};
  const trailerUrl = buildYouTubeTrailerUrl(details.videos?.results ?? []);

  return {
    tmdbId: details.id,
    imdbId,
    mediaType,
    title: mediaType === 'movie' ? details.title : details.name,
    overview: details.overview || '',
    posterPath: details.poster_path ?? null,
    releaseDate:
      mediaType === 'movie'
        ? (details.release_date ? new Date(details.release_date) : null)
        : (details.first_air_date ? new Date(details.first_air_date) : null),
    genres: details.genres?.map((genre: { name: string }) => genre.name) ?? [],
    originalLanguage: details.original_language ?? null,
    watchProviders: buildPlatformAvailabilityRecords({
      movieId: 0,
      details,
      requestedRegions: Object.keys(details?.['watch/providers']?.results ?? {}),
    }).map((entry) => entry.providerName),
    runtimeMinutes:
      mediaType === 'movie' && Number.isFinite(Number(details.runtime))
        ? Number(details.runtime)
        : null,
    director:
      mediaType === 'movie'
        ? credits.crew?.filter((person: any) => person.job === 'Director').map((person: any) => person.name) ?? []
        : details.created_by?.map((person: any) => person.name) ?? [],
    cast: credits.cast?.slice(0, 10).map((person: any) => person.name) ?? [],
    totalSeasons: mediaType === 'tv' ? details.number_of_seasons ?? null : null,
    totalEpisodes: mediaType === 'tv' ? details.number_of_episodes ?? null : null,
    watchLink:
      details?.['watch/providers']?.results?.[normalizeRegion('IN')]?.link ??
      ((Object.values(details?.['watch/providers']?.results ?? {})[0] as any)?.link ?? null) ??
      null,
    trailerUrl,
    trailerSource: trailerUrl ? 'tmdb' : null,
  };
};

const buildYouTubeTrailerUrl = (videos: Array<Record<string, any>>) => {
  const candidates = videos.filter((video) => {
    return video?.site === 'YouTube' && typeof video?.key === 'string' && video.key.trim().length > 0;
  });

  const officialTrailer = candidates.find((video) => video.type === 'Trailer' && video.official === true);
  const trailer = officialTrailer ?? candidates.find((video) => video.type === 'Trailer');
  const teaser = trailer ?? candidates.find((video) => video.type === 'Teaser');
  const selected = teaser ?? candidates[0];

  return selected ? `https://www.youtube.com/watch?v=${selected.key}` : null;
};

const buildDeepEnrichUpdate = (omdbData: Record<string, any>) => {
  const rtRatingString = omdbData.Ratings?.find((entry: any) => entry.Source === 'Rotten Tomatoes')?.Value;
  const imdbVotesNumeric =
    omdbData.imdbVotes && omdbData.imdbVotes !== 'N/A'
      ? Number.parseInt(String(omdbData.imdbVotes).replace(/,/g, ''), 10)
      : null;

  return {
    imdbRating: omdbData.imdbRating && omdbData.imdbRating !== 'N/A' ? Number.parseFloat(omdbData.imdbRating) : null,
    rottenTomatoesRating: rtRatingString ? Number.parseInt(rtRatingString.replace('%', ''), 10) : null,
    metascore: omdbData.Metascore && omdbData.Metascore !== 'N/A' ? Number.parseInt(omdbData.Metascore, 10) : null,
    imdbVotes: omdbData.imdbVotes && omdbData.imdbVotes !== 'N/A' ? omdbData.imdbVotes : null,
    imdbVotesNumeric: Number.isNaN(imdbVotesNumeric) ? null : imdbVotesNumeric,
  };
};

export const buildUserInterestJobInput = (movie: {
  movieId: number;
  tmdbId: number;
  mediaType: 'movie' | 'tv';
  imdbId?: string | null;
}) => {
  return buildDeepEnrichJobData({
    source: 'user_signal',
    movieId: movie.movieId,
    tmdbId: movie.tmdbId,
    mediaType: movie.mediaType,
    imdbId: movie.imdbId ?? null,
    priority: HIGH_INTENT_PRIORITY,
  });
};

export const isRetryablePrismaConnectionError = (error: unknown) => {
  const code =
    typeof error === 'object' && error !== null && 'code' in error
      ? String((error as { code?: unknown }).code)
      : null;

  if (code !== null && RETRYABLE_PRISMA_CONNECTION_ERROR_CODES.has(code)) {
    return true;
  }

  const message = error instanceof Error ? error.message.toLowerCase() : String(error ?? '').toLowerCase();
  return RETRYABLE_PRISMA_CONNECTION_ERROR_MESSAGES.some((pattern) => message.includes(pattern));
};

const getPrismaErrorCode = (error: unknown) => {
  return typeof error === 'object' && error !== null && 'code' in error
    ? String((error as { code?: unknown }).code)
    : null;
};

export const isStorageLimitExceededError = (error: unknown) => {
  const text = error instanceof Error ? error.message : String(error ?? '');
  const normalized = text.toLowerCase();
  return (
    normalized.includes('code: "53100"') ||
    normalized.includes('project size limit') ||
    normalized.includes('neon.max_cluster_size')
  );
};

const waitFor = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const reconnectPrismaClient = async (prisma: Pick<PrismaClient, '$connect' | '$disconnect'>) => {
  try {
    await prisma.$disconnect();
  } catch {
    // Best effort only. If disconnect itself fails, a fresh connect may still recover the client.
  }

  await prisma.$connect();
};

const runWithPrismaConnectionRetry = async <T>(
  operation: () => Promise<T>,
  prisma: Pick<PrismaClient, '$connect' | '$disconnect'>,
  logger: Pick<Console, 'warn'>,
) => {
  try {
    return await operation();
  } catch (error) {
    const errorCode = getPrismaErrorCode(error);
    if (!isRetryablePrismaConnectionError(error)) {
      throw error;
    }

    if (errorCode === 'P2024') {
      logger.warn(
        `Ingestion worker hit Prisma pool timeout (${errorCode}). Cooling down for ${POOL_TIMEOUT_RETRY_COOLDOWN_MS}ms before retry...`,
      );
      await waitFor(POOL_TIMEOUT_RETRY_COOLDOWN_MS);
    } else {
      logger.warn('Ingestion worker lost its database connection. Resetting Prisma and retrying once...');
    }

    await reconnectPrismaClient(prisma);
    return operation();
  }
};

export const recoverStaleRunningJobsWithRetry = async (
  store: IngestionStore,
  prisma: Pick<PrismaClient, '$connect' | '$disconnect'>,
  now: Date,
  options: { staleAfterMinutes?: number; limit?: number },
  logger: Pick<Console, 'warn'> = console,
) => {
  if (!store.recoverStaleRunningJobs) {
    return 0;
  }

  return runWithPrismaConnectionRetry(
    () => store.recoverStaleRunningJobs!(now, options),
    prisma,
    logger,
  );
};

export const runIngestionWorkerTick = async (
  store: IngestionStore,
  prisma: Pick<PrismaClient, '$connect' | '$disconnect'>,
  depsInput: PipelineDeps = {},
  logger: Pick<Console, 'warn'> = console,
) => {
  return runWithPrismaConnectionRetry(
    () => runNextIngestionJob(store, depsInput),
    prisma,
    logger,
  );
};

const processCatalogRefreshJob = async (
  store: IngestionStore,
  deps: Required<PipelineDeps>,
  job: IngestionJob,
) => {
  const mode = String(job.payload.mode ?? 'discover').toLowerCase();
  if (mode === 'bootstrap_export') {
    await processBootstrapExportJob(store, deps, job);
    return;
  }

  const pagesToScan = Number.parseInt(String(job.payload.pagesToScan ?? 1), 10);
  const dailyTarget = Number.parseInt(String(job.payload.dailyTarget ?? 20), 10);
  const movieTarget = Math.floor(dailyTarget / 2);
  const tvTarget = dailyTarget - movieTarget;
  const minReleaseYear = getIngestionMinReleaseYear();
  const languageMode = getIngestionLanguageMode();
  const languageWeights = getIngestionLanguageWeights();
  const useLanguageWeights = languageMode === 'weighted' && languageWeights.length > 0;
  const baseDiscoveryOptions = minReleaseYear === null
    ? {}
    : {
        releaseDateGte: `${minReleaseYear}-01-01`,
        releaseDateLte: deps.now().toISOString().slice(0, 10),
        sortBy: 'primary_release_date.desc',
      };

  const movieIds = useLanguageWeights
    ? await discoverIdsWithLanguageWeights(store, deps.discoverMovies, 'movie', pagesToScan, movieTarget, languageWeights, baseDiscoveryOptions, {
        minReleaseYear,
        now: deps.now(),
      })
    : await discoverIds(deps.discoverMovies, pagesToScan, movieTarget, baseDiscoveryOptions);
  const tvIds = useLanguageWeights
    ? await discoverIdsWithLanguageWeights(store, deps.discoverTV, 'tv', pagesToScan, tvTarget, languageWeights, baseDiscoveryOptions, {
        minReleaseYear,
        now: deps.now(),
      })
    : await discoverIds(deps.discoverTV, pagesToScan, tvTarget, baseDiscoveryOptions);

  for (const tmdbId of movieIds) {
    await store.enqueueJob(
      buildFetchJobData({
        source: job.source,
        tmdbId,
        mediaType: 'movie',
        region: job.region,
        priority: job.priority,
        shadowMode: job.shadowMode ?? false,
        createdAt: deps.now(),
      }),
    );
  }

  for (const tmdbId of tvIds) {
    await store.enqueueJob(
      buildFetchJobData({
        source: job.source,
        tmdbId,
        mediaType: 'tv',
        region: job.region,
        priority: job.priority,
        shadowMode: job.shadowMode ?? false,
        createdAt: deps.now(),
      }),
    );
  }

  await store.completeJob(job.id, {
    enqueuedFetchJobs: movieIds.length + tvIds.length,
    languageMode,
    languageWeights: useLanguageWeights ? languageWeights : [],
    minReleaseYear,
  });
};

const processBootstrapExportJob = async (
  store: IngestionStore,
  deps: Required<PipelineDeps>,
  job: IngestionJob,
) => {
  if (!store.getMovieCount || !store.getInternalState || !store.setInternalState) {
    throw new Error('bootstrap_export mode requires getMovieCount/getInternalState/setInternalState store methods');
  }

  const targetCatalogSize = Number.parseInt(String(job.payload.targetCatalogSize ?? 50_000), 10);
  const batchSize = Number.parseInt(String(job.payload.batchSize ?? 1500), 10);
  const scanWindow = Number.parseInt(String(job.payload.scanWindow ?? 12_000), 10);
  const maxScanPerRun = Number.parseInt(String(job.payload.maxScanPerRun ?? Math.max(scanWindow * 8, 20_000)), 10);
  const now = deps.now();
  const currentCount = await store.getMovieCount();

  if (currentCount >= targetCatalogSize) {
    await store.completeJob(job.id, {
      skipped: 'target_reached',
      targetCatalogSize,
      currentCount,
    });
    return;
  }

  const checkpointRecord = await store.getInternalState(BOOTSTRAP_STATE_KEY);
  // Fetch export files sequentially to reduce peak memory in low-memory deployments.
  const movieExport = await deps.getDailyExportIds('movie', now);
  const tvExport = await deps.getDailyExportIds('tv', now);

  const checkpoint = (checkpointRecord?.value ?? {}) as Record<string, any>;
  const hasMatchingCheckpoint =
    checkpoint.movieDateKey === movieExport.dateKey && checkpoint.tvDateKey === tvExport.dateKey;

  const movieOffset = hasMatchingCheckpoint ? Number.parseInt(String(checkpoint.movieOffset ?? 0), 10) : 0;
  const tvOffset = hasMatchingCheckpoint ? Number.parseInt(String(checkpoint.tvOffset ?? 0), 10) : 0;

  const movieTarget = Math.max(0, Math.floor(batchSize / 2));
  const tvTarget = Math.max(0, batchSize - movieTarget);

  const [movieScan, tvScan] = await Promise.all([
    collectMissingIdsFromExport(
      store,
      movieExport.ids,
      'movie',
      movieOffset,
      movieTarget,
      Math.max(1, Math.floor(scanWindow / 2)),
      Math.max(1, Math.floor(maxScanPerRun / 2)),
    ),
    collectMissingIdsFromExport(
      store,
      tvExport.ids,
      'tv',
      tvOffset,
      tvTarget,
      Math.max(1, Math.floor(scanWindow / 2)),
      Math.max(1, Math.floor(maxScanPerRun / 2)),
    ),
  ]);

  const enqueuedMovieIds = movieScan.missingIds.slice(0, movieTarget);
  const enqueuedTvIds = tvScan.missingIds.slice(0, tvTarget);

  for (const tmdbId of enqueuedMovieIds) {
    await store.enqueueJob(
      buildFetchJobData({
        source: 'bootstrap',
        tmdbId,
        mediaType: 'movie',
        region: job.region,
        priority: 250,
        shadowMode: job.shadowMode ?? false,
        createdAt: now,
      }),
    );
  }

  for (const tmdbId of enqueuedTvIds) {
    await store.enqueueJob(
      buildFetchJobData({
        source: 'bootstrap',
        tmdbId,
        mediaType: 'tv',
        region: job.region,
        priority: 250,
        shadowMode: job.shadowMode ?? false,
        createdAt: now,
      }),
    );
  }

  await store.setInternalState(
    BOOTSTRAP_STATE_KEY,
    {
      movieDateKey: movieExport.dateKey,
      tvDateKey: tvExport.dateKey,
      movieOffset: movieScan.nextOffset,
      tvOffset: tvScan.nextOffset,
      updatedAt: now.toISOString(),
    },
    now,
  );

  await store.completeJob(job.id, {
    mode: 'bootstrap_export',
    currentCount,
    targetCatalogSize,
    scannedMovieIds: movieScan.scannedCount,
    scannedTvIds: tvScan.scannedCount,
    enqueuedFetchJobs: enqueuedMovieIds.length + enqueuedTvIds.length,
  });
};

const processDeltaRefreshJob = async (
  store: IngestionStore,
  deps: Required<PipelineDeps>,
  job: IngestionJob,
) => {
  const now = deps.now();
  const pagesToScan = Number.parseInt(String(job.payload.pagesToScan ?? 12), 10);
  const targetCount = Number.parseInt(String(job.payload.targetCount ?? 800), 10);
  const windowHours = Number.parseInt(String(job.payload.windowHours ?? 36), 10);
  const windowStart = new Date(now);
  windowStart.setUTCHours(windowStart.getUTCHours() - windowHours);
  const releaseDateGte = windowStart.toISOString().slice(0, 10);
  const releaseDateLte = now.toISOString().slice(0, 10);
  const minReleaseYear = getIngestionMinReleaseYear();

  const movieTarget = Math.floor(targetCount / 2);
  const tvTarget = targetCount - movieTarget;
  const baseDiscoveryOptions = {
    releaseDateGte,
    releaseDateLte,
  };
  const languageMode = getIngestionLanguageMode();
  const languageWeights = getIngestionLanguageWeights();
  const useLanguageWeights = languageMode === 'weighted' && languageWeights.length > 0;
  const movieIds = useLanguageWeights
    ? await discoverIdsWithLanguageWeights(
        store,
        deps.discoverMovies,
        'movie',
        pagesToScan,
        movieTarget,
        languageWeights,
        baseDiscoveryOptions,
        { minReleaseYear, now },
      )
    : await discoverIds(deps.discoverMovies, pagesToScan, movieTarget, baseDiscoveryOptions);
  const tvIds = useLanguageWeights
    ? await discoverIdsWithLanguageWeights(
        store,
        deps.discoverTV,
        'tv',
        pagesToScan,
        tvTarget,
        languageWeights,
        baseDiscoveryOptions,
        { minReleaseYear, now },
      )
    : await discoverIds(deps.discoverTV, pagesToScan, tvTarget, baseDiscoveryOptions);

  for (const tmdbId of movieIds) {
    await store.enqueueJob(
      buildFetchJobData({
        source: 'delta',
        tmdbId,
        mediaType: 'movie',
        region: job.region,
        priority: job.priority,
        shadowMode: job.shadowMode ?? false,
        createdAt: now,
      }),
    );
  }

  for (const tmdbId of tvIds) {
    await store.enqueueJob(
      buildFetchJobData({
        source: 'delta',
        tmdbId,
        mediaType: 'tv',
        region: job.region,
        priority: job.priority,
        shadowMode: job.shadowMode ?? false,
        createdAt: now,
      }),
    );
  }

  await store.completeJob(job.id, {
    mode: 'delta_refresh',
    releaseDateGte,
    releaseDateLte,
    enqueuedFetchJobs: movieIds.length + tvIds.length,
    languageMode,
    languageWeights: useLanguageWeights ? languageWeights : [],
    minReleaseYear,
  });
};

const processFetchJob = async (
  store: IngestionStore,
  deps: Required<PipelineDeps>,
  job: IngestionJob,
) => {
  const tmdbId = Number(job.payload.tmdbId);
  const mediaType = job.payload.mediaType as 'movie' | 'tv';
  const details =
    mediaType === 'movie'
      ? await deps.getMovieDetails(tmdbId)
      : await deps.getTVDetails(tmdbId);

  const snapshot = await store.createSnapshot(
    buildRawSnapshotData({
      source: 'TMDB',
      externalId: `${mediaType}:${tmdbId}`,
      region: job.region,
      payload: details,
      jobId: job.id,
      capturedAt: deps.now(),
    }),
  );

  await store.enqueueJob(
    buildNormalizeJobData({
      source: job.source,
      tmdbId,
      mediaType,
      snapshotId: snapshot.id,
      region: job.region,
      priority: job.priority + 20,
      shadowMode: job.shadowMode ?? false,
      createdAt: deps.now(),
    }),
  );

  await store.completeJob(job.id, {
    snapshotId: snapshot.id,
  });
};

const processNormalizeJob = async (
  store: IngestionStore,
  deps: Required<PipelineDeps>,
  job: IngestionJob,
) => {
  if (!store.getSnapshotById || !store.upsertMovie || !store.replacePlatformAvailability) {
    throw new Error('normalize jobs require snapshot, movie, and availability store methods');
  }

  const snapshot = await store.getSnapshotById(String(job.payload.snapshotId));
  if (!snapshot) {
    throw new Error(`Snapshot ${job.payload.snapshotId} not found`);
  }

  const mediaType = job.payload.mediaType as 'movie' | 'tv';
  const movieRecord = buildMovieRecordFromTmdb(snapshot.payload, mediaType);
  const minReleaseYear = getIngestionMinReleaseYear();

  if (isReleaseDateBeforeMinYear(movieRecord.releaseDate, minReleaseYear)) {
    await store.completeJob(job.id, {
      skipped: 'release_year_before_floor',
      tmdbId: movieRecord.tmdbId,
      minReleaseYear,
      releaseDate: movieRecord.releaseDate ? movieRecord.releaseDate.toISOString().slice(0, 10) : null,
      shadowMode: job.shadowMode ?? false,
    });

    if (shouldDeleteTmdbSnapshotAfterNormalize() && store.deleteSnapshotById) {
      await store.deleteSnapshotById(String(job.payload.snapshotId));
    }
    return;
  }

  if (!job.shadowMode) {
    const savedMovie = await store.upsertMovie(movieRecord);
    const availabilityRecords = buildPlatformAvailabilityRecords({
      movieId: savedMovie.id,
      details: snapshot.payload,
      requestedRegions: Object.keys(snapshot.payload?.['watch/providers']?.results ?? {}),
      capturedAt: deps.now(),
    });
    await store.replacePlatformAvailability(savedMovie.id, availabilityRecords);

    if (movieRecord.imdbId && job.priority >= DEEP_ENRICH_PRIORITY_THRESHOLD) {
      await store.enqueueJob(
        buildDeepEnrichJobData({
          source: job.source,
          movieId: savedMovie.id,
          tmdbId: movieRecord.tmdbId,
          mediaType,
          imdbId: movieRecord.imdbId,
          region: job.region,
          priority: job.priority,
          shadowMode: false,
          createdAt: deps.now(),
        }),
      );
    }
  } else if (movieRecord.imdbId && job.priority >= DEEP_ENRICH_PRIORITY_THRESHOLD) {
    await store.enqueueJob(
      buildDeepEnrichJobData({
        source: job.source,
        tmdbId: movieRecord.tmdbId,
        mediaType,
        imdbId: movieRecord.imdbId,
        region: job.region,
        priority: job.priority,
        shadowMode: true,
        createdAt: deps.now(),
      }),
    );
  }

  await store.completeJob(job.id, {
    tmdbId: movieRecord.tmdbId,
    shadowMode: job.shadowMode ?? false,
  });

  if (shouldDeleteTmdbSnapshotAfterNormalize() && store.deleteSnapshotById) {
    await store.deleteSnapshotById(String(job.payload.snapshotId));
  }
};

const processDeepEnrichJob = async (
  store: IngestionStore,
  deps: Required<PipelineDeps>,
  job: IngestionJob,
) => {
  if (!job.shadowMode && store.getOmdbUsage && store.deferJob) {
    const usage = await store.getOmdbUsage(deps.now());
    const nextAttemptIsRetry = (job.attempts ?? 0) > 0;
    const omdbDailyLimit = getOmdbDailyLimit();
    const omdbRetryReserve = getOmdbRetryReserve();
    const regularBudgetReached = usage.regular >= omdbDailyLimit;
    const retryBudgetReached = usage.retries >= omdbRetryReserve;
    const totalBudgetReached = usage.total >= omdbDailyLimit + omdbRetryReserve;

    const shouldDeferForBudget = totalBudgetReached || (nextAttemptIsRetry ? retryBudgetReached : regularBudgetReached);
    if (shouldDeferForBudget) {
      const nextDay = deps.now();
      nextDay.setUTCHours(24, 5, 0, 0);
      await store.deferJob(job.id, {
        availableAt: nextDay,
        reason: 'omdb_daily_budget_exhausted',
      });
      return;
    }
  }

  const movieId = job.payload.movieId ? Number(job.payload.movieId) : null;
  const tmdbId = job.payload.tmdbId ? Number(job.payload.tmdbId) : null;
  let imdbId = job.payload.imdbId ? String(job.payload.imdbId) : null;

  if (!imdbId && movieId && store.findMovieById) {
    const movie = await store.findMovieById(movieId);
    imdbId = movie?.imdbId ?? null;
  }

  if (!imdbId) {
    await store.completeJob(job.id, { skipped: 'missing_imdb_id' });
    return;
  }

  const omdbData = await deps.getRatings(imdbId);
  const returnedImdbId = typeof omdbData?.imdbID === 'string' ? omdbData.imdbID : null;
  if (returnedImdbId && returnedImdbId !== imdbId) {
    await store.completeJob(job.id, {
      skipped: 'omdb_imdb_id_mismatch',
      requestedImdbId: imdbId,
      returnedImdbId,
      shadowMode: job.shadowMode ?? false,
    });
    return;
  }

  if (shouldStoreOmdbSnapshots()) {
    await store.createSnapshot(
      buildRawSnapshotData({
        source: 'OMDB',
        externalId: imdbId,
        region: job.region,
        payload: omdbData,
        jobId: job.id,
        capturedAt: deps.now(),
      }),
    );
  }

  if (!job.shadowMode) {
    const update = buildDeepEnrichUpdate(omdbData);
    if (movieId && store.updateMovieById) {
      await store.updateMovieById(movieId, update);
    } else if (tmdbId && store.updateMovieByTmdbId) {
      const mediaType = job.payload.mediaType === 'tv' ? 'tv' : 'movie';
      await store.updateMovieByTmdbId(tmdbId, mediaType, update);
    }
  }

  await store.completeJob(job.id, {
    imdbId,
    shadowMode: job.shadowMode ?? false,
  });
};

export const runNextIngestionJob = async (
  store: IngestionStore,
  depsInput: PipelineDeps = {},
) => {
  const deps = { ...defaultDeps, ...depsInput };
  const job = await store.claimNextJob(deps.now());
  if (!job) {
    return { processed: false };
  }

  try {
    switch (job.type) {
      case 'CATALOG_REFRESH':
        await processCatalogRefreshJob(store, deps, job);
        break;
      case 'DISCOVER':
        await processDeltaRefreshJob(store, deps, job);
        break;
      case 'FETCH':
        await processFetchJob(store, deps, job);
        break;
      case 'NORMALIZE':
        await processNormalizeJob(store, deps, job);
        break;
      case 'DEEP_ENRICH':
        await processDeepEnrichJob(store, deps, job);
        break;
      default:
        throw new Error(`Unsupported ingestion job type: ${job.type}`);
    }

    return { processed: true, jobId: job.id, type: job.type };
  } catch (error: any) {
    const now = deps.now();
    const attempts = (job.attempts ?? 0) + 1;
    const errorMessage = error instanceof Error ? error.message : String(error);

    if (attempts >= job.maxAttempts) {
      await store.deadLetterJob(job.id, { errorMessage });
    } else {
      await store.rescheduleJob(job.id, {
        errorMessage,
        availableAt: getBackoffDate(now, attempts),
      });
    }

    return { processed: true, jobId: job.id, type: job.type, failed: true };
  }
};

export const startIngestionWorker = (
  prisma: PrismaClient,
  options: {
    pollIntervalMs?: number;
    maxJobsPerTick?: number;
    staleRecoveryMinutes?: number;
    staleRecoveryIntervalMs?: number;
  } = {},
) => {
  const pollIntervalMs = options.pollIntervalMs ?? 5_000;
  const maxJobsPerTick = Math.max(1, options.maxJobsPerTick ?? 10);
  const staleRecoveryMinutes = Math.max(5, options.staleRecoveryMinutes ?? 15);
  const staleRecoveryIntervalMs = Math.max(10_000, options.staleRecoveryIntervalMs ?? 60_000);
  const store = createPrismaIngestionStore(prisma);
  let timer: NodeJS.Timeout | null = null;
  let stopped = false;
  let nextRecoveryAt = 0;
  let pauseUntil = 0;

  const scheduleNextRun = () => {
    if (stopped) {
      return;
    }

    timer = setTimeout(runLoop, pollIntervalMs);
  };

  const runLoop = async () => {
    try {
      const now = Date.now();
      if (pauseUntil > now) {
        return;
      }

      if (now >= nextRecoveryAt) {
        const recovered = await recoverStaleRunningJobsWithRetry(store, prisma, new Date(), {
          staleAfterMinutes: staleRecoveryMinutes,
          limit: 25,
        });
        if (recovered > 0) {
          console.warn(`Recovered ${recovered} stale ingestion jobs back to queue.`);
        }
        nextRecoveryAt = now + staleRecoveryIntervalMs;
      }

      for (let i = 0; i < maxJobsPerTick; i += 1) {
        const result = await runIngestionWorkerTick(store, prisma);
        if (!result?.processed) {
          break;
        }
      }
    } catch (error) {
      if (isStorageLimitExceededError(error)) {
        pauseUntil = Date.now() + STORAGE_LIMIT_RETRY_COOLDOWN_MS;
        console.error(
          `Ingestion worker paused for ${STORAGE_LIMIT_RETRY_COOLDOWN_MS / 60_000} minutes because database storage is full. ` +
            'Free database space or increase quota, then worker will retry automatically.',
        );
      } else if (isRetryablePrismaConnectionError(error)) {
        pauseUntil = Date.now() + DATABASE_UNAVAILABLE_RETRY_COOLDOWN_MS;
        console.error(
          `Ingestion worker paused for ${DATABASE_UNAVAILABLE_RETRY_COOLDOWN_MS / 1000} seconds because the database is unreachable. ` +
            'It will retry automatically.',
        );
      } else {
        console.error('Ingestion worker loop failed:', error);
      }
    } finally {
      scheduleNextRun();
    }
  };

  scheduleNextRun();

  return {
    stop() {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
      }
    },
  };
};
