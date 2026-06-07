import axios from 'axios';
import dotenv from 'dotenv';
import zlib from 'node:zlib';
import readline from 'node:readline';
import { getTmdbMax429BackoffSeconds, getTmdbRequestsPerSecond } from './ingestion/queueConfig';

dotenv.config();

const TMDB_API_URL = 'https://api.themoviedb.org/3';
const TMDB_EXPORTS_URL = 'http://files.tmdb.org/p/exports';
const TMDB_MAX_RATE_LIMIT_PENALTY_MS = 2000;

let nextAllowedTmdbRequestAt = 0;
let rateLimitPenaltyMs = 0;

const getTmdbBaseRequestSpacingMs = () => {
  const requestsPerSecond = getTmdbRequestsPerSecond();
  return Math.max(25, Math.ceil(1000 / requestsPerSecond));
};

const getTmdbClient = () => {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey) {
    throw new Error('Missing TMDB_API_KEY in .env file');
  }

  return axios.create({
    baseURL: TMDB_API_URL,
    params: {
      api_key: apiKey,
    },
    timeout: 30_000, // 30s so slow responses don't hang forever
  });
};

const waitFor = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

const throttleTmdbRequest = async () => {
  const now = Date.now();
  const waitMs = Math.max(0, nextAllowedTmdbRequestAt - now);
  if (waitMs > 0) {
    await waitFor(waitMs);
  }

  const effectiveSpacingMs = getTmdbBaseRequestSpacingMs() + rateLimitPenaltyMs;
  nextAllowedTmdbRequestAt = Date.now() + effectiveSpacingMs;
};

const registerTmdbRateLimit = (retryAfterHeader?: string | number | null) => {
  const retryAfterSeconds = Number.parseInt(String(retryAfterHeader ?? ''), 10);
  const retryAfterMs = Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
    ? Math.min(retryAfterSeconds * 1000, getTmdbMax429BackoffSeconds() * 1000)
    : 0;

  const penaltyBump = rateLimitPenaltyMs === 0 ? 200 : Math.ceil(rateLimitPenaltyMs * 1.5);
  rateLimitPenaltyMs = Math.min(
    TMDB_MAX_RATE_LIMIT_PENALTY_MS,
    Math.max(retryAfterMs, penaltyBump),
  );
};

const registerTmdbSuccess = () => {
  if (rateLimitPenaltyMs === 0) {
    return;
  }

  rateLimitPenaltyMs = Math.max(0, rateLimitPenaltyMs - 75);
};

const redactTmdbApiKey = (value: string) =>
  value.replace(/([?&]api_key=)[^&\s]+/gi, '$1[redacted]');

const buildTmdbErrorSummary = (error: any) => {
  const config = error?.config ?? {};
  const request = error?.request ?? {};
  const rawUrl = String(config.url ?? request.path ?? request._currentUrl ?? '');
  const rawBaseUrl = String(config.baseURL ?? '');
  const path = rawUrl.startsWith('http')
    ? redactTmdbApiKey(rawUrl)
    : redactTmdbApiKey(`${rawBaseUrl}${rawUrl}`);

  return {
    message: error instanceof Error ? error.message : String(error ?? 'Unknown TMDB error'),
    code: error?.code,
    status: error?.response?.status,
    method: config.method ? String(config.method).toUpperCase() : undefined,
    path: path || undefined,
  };
};

const logTmdbError = (label: string, error: unknown) => {
  console.error(label, buildTmdbErrorSummary(error));
};

const withTmdbGovernor = async <T>(fn: () => Promise<T>) => {
  await throttleTmdbRequest();

  try {
    const result = await fn();
    registerTmdbSuccess();
    return result;
  } catch (error: any) {
    if (error?.response?.status === 429) {
      registerTmdbRateLimit(error?.response?.headers?.['retry-after']);
    }
    throw error;
  }
};

/** Retryable network errors - TMDB may close connections under load (ECONNRESET) or rate-limit (429). */
function isRetryableError(err: any): boolean {
  const code = err?.code ?? err?.response?.status;
  return (
    code === 'ECONNRESET' ||
    code === 'ETIMEDOUT' ||
    code === 'ECONNABORTED' ||
    code === 'ENOTFOUND' ||
    code === 429
  );
}

async function withRetry<T>(fn: () => Promise<T>, label: string, maxAttempts = 3): Promise<T> {
  let lastError: any;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err: any) {
      lastError = err;
      if (attempt < maxAttempts && isRetryableError(err)) {
        const delayMs = Math.min(1000 * Math.pow(2, attempt - 1), 8000);
        console.warn(`${label}: ${err?.code || err?.message} (attempt ${attempt}/${maxAttempts}), retrying in ${delayMs}ms...`);
        await new Promise((r) => setTimeout(r, delayMs));
      } else {
        throw err;
      }
    }
  }
  throw lastError;
}

export const discoverMovies = async (page: number = 1, options: { releaseDateGte?: string, releaseDateLte?: string, withOriginalLanguage?: string, sortBy?: string } = {}) => {
  try {
    const params: any = {
      page,
      sort_by: options.sortBy ?? 'popularity.desc',
    };

    if (options.releaseDateGte) {
      params['primary_release_date.gte'] = options.releaseDateGte;
    }
    if (options.releaseDateLte) {
      params['primary_release_date.lte'] = options.releaseDateLte;
    }
    if (options.withOriginalLanguage) {
      params['with_original_language'] = options.withOriginalLanguage;
    }

    return withRetry(
      () => withTmdbGovernor(() => getTmdbClient().get('/discover/movie', { params })).then((response) => response.data.results),
      `discoverMovies(page ${page})`,
    );
  } catch (error) {
    logTmdbError('Error fetching movies from TMDB:', error);
    throw error;
  }
};

export const getMovieDetails = async (tmdbId: number) => {
  return withRetry(
    () =>
      withTmdbGovernor(() => getTmdbClient().get(`/movie/${tmdbId}`, {
        params: { append_to_response: 'watch/providers,credits,videos' },
      })).then((r) => r.data),
    `getMovieDetails(${tmdbId})`
  ).catch((error) => {
    logTmdbError(`Error fetching details for movie ${tmdbId} from TMDB:`, error);
    throw error;
  });
};

export const getVideos = async (tmdbId: number, mediaType: 'movie' | 'tv') => {
  return withRetry(
    () =>
      withTmdbGovernor(() => getTmdbClient().get(`/${mediaType}/${tmdbId}/videos`)).then((r) => r.data?.results ?? []),
    `getVideos(${mediaType}:${tmdbId})`,
  ).catch((error) => {
    logTmdbError(`Error fetching videos for ${mediaType} ${tmdbId} from TMDB:`, error);
    throw error;
  });
};

export const getGenres = async () => {
  try {
    const response = await withTmdbGovernor(() => getTmdbClient().get('/genre/movie/list'));
    return response.data.genres;
  } catch (error) {
    logTmdbError('Error fetching genres from TMDB:', error);
    throw error;
  }
};

export const getWatchProviders = async () => {
  try {
    const response = await withTmdbGovernor(() => getTmdbClient().get('/watch/providers/movie', {
      params: {
        watch_region: 'IN', // India region
      },
    }));
    return response.data.results;
  } catch (error) {
    logTmdbError('Error fetching watch providers from TMDB:', error);
    throw error;
  }
};

// --- TV Series Endpoints ---

export const discoverTV = async (page: number = 1, options: { releaseDateGte?: string, releaseDateLte?: string, withOriginalLanguage?: string, sortBy?: string } = {}) => {
  try {
    const params: any = {
      page,
      sort_by: options.sortBy === 'primary_release_date.desc'
        ? 'first_air_date.desc'
        : options.sortBy ?? 'popularity.desc',
    };

    if (options.releaseDateGte) {
      params['first_air_date.gte'] = options.releaseDateGte;
    }
    if (options.releaseDateLte) {
      params['first_air_date.lte'] = options.releaseDateLte;
    }
    if (options.withOriginalLanguage) {
      params['with_original_language'] = options.withOriginalLanguage;
    }

    return withRetry(
      () => withTmdbGovernor(() => getTmdbClient().get('/discover/tv', { params })).then((response) => response.data.results),
      `discoverTV(page ${page})`,
    );
  } catch (error) {
    logTmdbError('Error fetching TV series from TMDB:', error);
    throw error;
  }
};

export const getTVDetails = async (tmdbId: number) => {
  return withRetry(
    () =>
      withTmdbGovernor(() => getTmdbClient().get(`/tv/${tmdbId}`, {
        params: { append_to_response: 'watch/providers,credits,external_ids,videos' },
      })).then((r) => r.data),
    `getTVDetails(${tmdbId})`
  ).catch((error) => {
    logTmdbError(`Error fetching details for TV ${tmdbId} from TMDB:`, error);
    throw error;
  });
};

export const getTVGenres = async () => {
  try {
    const response = await withTmdbGovernor(() => getTmdbClient().get('/genre/tv/list'));
    return response.data.genres;
  } catch (error) {
    logTmdbError('Error fetching TV genres from TMDB:', error);
    throw error;
  }
};

export const summarizeTmdbErrorForLog = buildTmdbErrorSummary;

type ExportMediaType = 'movie' | 'tv';

const getExportFileCandidates = (mediaType: ExportMediaType, date: Date) => {
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  const yyyy = String(date.getUTCFullYear());
  const suffix = `${mm}_${dd}_${yyyy}.json.gz`;

  if (mediaType === 'movie') {
    return [`movie_ids_${suffix}`];
  }

  return [`tv_series_ids_${suffix}`, `tv_ids_${suffix}`];
};

const parseExportIdsFromStream = async (compressedStream: NodeJS.ReadableStream) => {
  const gunzip = zlib.createGunzip();
  const stream = compressedStream.pipe(gunzip);
  const lineReader = readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  });

  const ids: number[] = [];

  for await (const rawLine of lineReader) {
    const line = String(rawLine).trim();
    if (!line) {
      continue;
    }

    try {
      const payload = JSON.parse(line);
      const numericId = Number.parseInt(String(payload?.id ?? ''), 10);
      if (Number.isInteger(numericId) && numericId > 0) {
        ids.push(numericId);
      }
    } catch {
      // Ignore malformed lines so one bad record does not fail the entire export.
    }
  }

  return ids;
};

export const getDailyExportIds = async (
  mediaType: ExportMediaType,
  now: Date = new Date(),
): Promise<{ dateKey: string; ids: number[] }> => {
  const dayOffsets = [1, 2, 0];

  for (const offset of dayOffsets) {
    const date = new Date(now);
    date.setUTCDate(date.getUTCDate() - offset);
    const candidates = getExportFileCandidates(mediaType, date);

    for (const filename of candidates) {
      const url = `${TMDB_EXPORTS_URL}/${filename}`;
      try {
        const response = await axios.get(url, { responseType: 'stream', timeout: 90_000 });
        const ids = await parseExportIdsFromStream(response.data);
        return {
          dateKey: filename,
          ids,
        };
      } catch (error: any) {
        if (error?.response?.status !== 404) {
          console.warn(`Failed to fetch TMDB daily export ${filename}:`, error?.message ?? error);
        }
      }
    }
  }

  throw new Error(`Unable to fetch TMDB daily export for media type: ${mediaType}`);
};
