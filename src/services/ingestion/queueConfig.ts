const parsePositiveIntEnv = (
  value: string | undefined,
  defaultValue: number,
  options: { min?: number; max?: number } = {},
) => {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return defaultValue;
  }

  const min = options.min ?? 1;
  const max = options.max ?? Number.MAX_SAFE_INTEGER;
  return Math.min(max, Math.max(min, parsed));
};

const parseBooleanEnv = (
  value: string | undefined,
  defaultValue: boolean,
) => {
  if (value === undefined || String(value).trim() === '') {
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

export const getTmdbRequestsPerSecond = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.TMDB_REQUESTS_PER_SECOND, 8, { min: 1, max: 40 });

export const getTmdbMax429BackoffSeconds = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.TMDB_MAX_429_BACKOFF_SECONDS, 300, { min: 1, max: 3600 });

export const getOmdbDailyLimit = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.INGESTION_OMDB_DAILY_LIMIT, 950, { min: 1, max: 1000 });

export const getOmdbRetryReserve = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.INGESTION_OMDB_RETRY_RESERVE, 50, { min: 0, max: 200 });

export const isFallbackToGlobalEnabled = (
  env: Record<string, string | undefined> = process.env,
) => parseBooleanEnv(env.INGESTION_FALLBACK_TO_GLOBAL, true);

export const getIngestionQueueHighWatermark = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.INGESTION_QUEUE_HIGH_WATERMARK, 10_000, { min: 100 });

export const getIngestionQueueRefillTarget = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.INGESTION_QUEUE_REFILL_TARGET, 5_000, { min: 100 });

export const getIngestionDailyTarget = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.INGESTION_DAILY_TARGET, 500, { min: 1 });

const INGESTION_JOB_TYPES = [
  'CATALOG_REFRESH',
  'DISCOVER',
  'FETCH',
  'NORMALIZE',
  'DEEP_ENRICH',
] as const;

export const parseIngestionAllowedJobTypes = (
  env: Record<string, string | undefined> = process.env,
) => {
  const raw = String(env.INGESTION_ALLOWED_JOB_TYPES ?? '').trim();
  if (!raw) {
    return [] as string[];
  }

  const allowed = new Set(INGESTION_JOB_TYPES);
  return raw
    .split(',')
    .map((value) => value.trim().toUpperCase())
    .filter((value) => allowed.has(value as (typeof INGESTION_JOB_TYPES)[number]));
};

export const getOmdbMaxFetchBacklog = (
  env: Record<string, string | undefined> = process.env,
) => parsePositiveIntEnv(env.INGESTION_OMDB_MAX_FETCH_BACKLOG, 100, { min: 0, max: 10_000 });

export const getOmdbMinCatalogProgressRatio = (
  env: Record<string, string | undefined> = process.env,
) => {
  const parsed = Number.parseFloat(String(env.INGESTION_OMDB_MIN_CATALOG_PROGRESS_RATIO ?? ''));
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
    return 0.8;
  }

  return parsed;
};

export const isQueueAboveHighWatermark = (
  queuedAndRunningCount: number,
  env: Record<string, string | undefined> = process.env,
) => queuedAndRunningCount >= getIngestionQueueHighWatermark(env);
