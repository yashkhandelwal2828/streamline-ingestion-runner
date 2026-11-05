import axios from 'axios';
import fs from 'node:fs';
import path from 'node:path';
import { searchTitle } from '../services/tmdb';
import { syncNetflixTop10, NETFLIX_COUNTRIES_URL, NETFLIX_GLOBAL_URL } from '../services/netflixTop10/sync';
import { createScriptContext, disconnectPrisma } from './scriptUtils';

const SCRIPT_NAME = 'ingestion:netflix-top10';

/**
 * Weekly job: downloads Netflix's official Top 10 TSV files, matches titles to TMDB, stores the
 * newest week(s), and queues missing titles for the normal enrichment pipeline.
 *
 * Env:
 *   NETFLIX_TOP10_WEEKS           newest weeks to ingest (default 1, max 52; use e.g. 8 for a first backfill)
 *   NETFLIX_TOP10_DRY_RUN=true    parse and report only; no TMDB calls, no database writes
 *   NETFLIX_TOP10_SOURCE_DIR      read all-weeks-countries.tsv / all-weeks-global.tsv from a local directory
 *   NETFLIX_TOP10_MAX_SEARCHES    cap on TMDB searches per run (default 3000)
 */
const readText = async (url: string, sourceDir: string | undefined) => {
  if (sourceDir) {
    return fs.readFileSync(path.join(sourceDir, path.basename(new URL(url).pathname)), 'utf8');
  }
  const response = await axios.get<string>(url, {
    responseType: 'text',
    timeout: 120_000,
    maxContentLength: 200 * 1024 * 1024,
    transformResponse: (data) => data,
    headers: { 'User-Agent': 'StreamlineIngestion/1.0 (+https://watchpickr.com)', Accept: 'text/tab-separated-values,*/*' },
  });
  return response.data;
};

const main = async () => {
  const weeks = Math.min(52, Math.max(1, Number.parseInt(process.env.NETFLIX_TOP10_WEEKS ?? '1', 10) || 1));
  const dryRun = ['true', '1', 'yes'].includes(String(process.env.NETFLIX_TOP10_DRY_RUN ?? '').toLowerCase());
  const sourceDir = process.env.NETFLIX_TOP10_SOURCE_DIR || undefined;
  const maxSearches = Number.parseInt(process.env.NETFLIX_TOP10_MAX_SEARCHES ?? '', 10) || undefined;
  console.log(`[${SCRIPT_NAME}] starting`, { weeks, dryRun, sourceDir: sourceDir ?? null });

  const { prisma, store } = createScriptContext();
  try {
    const summary = await syncNetflixTop10({
      prisma: prisma as any,
      store: store as any,
      fetchText: (url) => readText(url, sourceDir),
      search: (title, mediaType) => searchTitle(title, mediaType),
      log: (message, data) => console.log(JSON.stringify({ script: SCRIPT_NAME, message, ...data })),
    }, { weeks, dryRun, maxSearches, countriesUrl: NETFLIX_COUNTRIES_URL, globalUrl: NETFLIX_GLOBAL_URL });
    if (!dryRun && summary.entryMatchRate < 0.5) {
      console.warn(`[${SCRIPT_NAME}] low TMDB match rate ${summary.entryMatchRate}; check TMDB availability or Netflix title formatting`);
    }
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => {
  console.error(`[${SCRIPT_NAME}] failed:`, error);
  process.exit(1);
});
