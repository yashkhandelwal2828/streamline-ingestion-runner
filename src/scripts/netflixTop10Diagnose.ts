import { getTVDetails, getMovieDetails, getPosterCandidates, searchTitle, searchMulti } from '../services/tmdb';
import { titleVariants, normalizeTitleKey } from '../services/netflixTop10/matcher';
import { resolveNetflixTitle } from '../services/netflixTop10/resolve';
import { createScriptContext, disconnectPrisma } from './scriptUtils';

const SCRIPT_NAME = 'ingestion:netflix-top10-diagnose';

/**
 * READ-ONLY root-cause report for charted titles without artwork. Run it once after deploying the fix
 * (or locally against a copy of the data) with DATABASE_URL and TMDB_API_KEY set; it writes nothing.
 *
 * For every entry in the newest week that has no TMDB id, it prints what each search strategy returns
 * and whether the new resolver would match it. For every matched entry whose catalog row has no poster,
 * it prints the stored posterPath, what TMDB returns now (details and per-language images), and any
 * FETCH jobs for the title, so "TMDB has no poster yet" and "we saved null" can be told apart.
 */
const brief = (results: any[]) => results.slice(0, 5).map((r) => `${r.id}:${r.title ?? r.name} (${(r.release_date ?? r.first_air_date ?? '').slice(0, 4)})${r.media_type ? ` [${r.media_type}]` : ''}`);

const main = async () => {
  const { prisma } = createScriptContext();
  try {
    const latest = await prisma.netflixTop10Entry.findFirst({ orderBy: { week: 'desc' }, select: { week: true } });
    if (!latest) { console.log('no entries'); return; }
    const week = latest.week.toISOString().slice(0, 10);
    const entries = await prisma.netflixTop10Entry.findMany({ where: { week: latest.week } });

    const unmatched = new Map<string, { title: string; mediaType: 'movie' | 'tv'; scopes: string[] }>();
    for (const entry of entries.filter((e) => e.tmdbId === null)) {
      const key = `${entry.mediaType}:${normalizeTitleKey(entry.showTitle)}`;
      const row = unmatched.get(key) ?? { title: entry.showTitle, mediaType: entry.mediaType as 'movie' | 'tv', scopes: [] };
      row.scopes.push(`${entry.scope}#${entry.rank}`);
      unmatched.set(key, row);
    }
    for (const { title, mediaType, scopes } of unmatched.values()) {
      const cached = await prisma.netflixTitleMatch.findUnique({ where: { titleKey_mediaType: { titleKey: normalizeTitleKey(title), mediaType } } });
      const variants = titleVariants(title);
      const searches: Record<string, string[]> = {};
      for (const variant of variants) searches[`search/${mediaType}: ${variant}`] = brief(await searchTitle(variant, mediaType));
      searches[`search/multi: ${variants[0]}`] = brief(await searchMulti(variants[0]));
      const resolved = await resolveNetflixTitle({ search: searchTitle, searchMulti }, { title, mediaType, weekDate: week });
      console.log(JSON.stringify({ kind: 'unmatched', title, mediaType, scopes, cachedStatus: cached?.status ?? null, cachedCheckedAt: cached?.checkedAt ?? null, searches, resolverResult: resolved }, null, 2));
    }

    const ids = [...new Set(entries.filter((e) => e.tmdbId !== null).map((e) => `${e.mediaType}:${e.tmdbId}`))];
    for (const id of ids) {
      const [mediaType, raw] = id.split(':') as ['movie' | 'tv', string];
      const tmdbId = Number(raw);
      const movie = await prisma.movie.findUnique({ where: { tmdbId_mediaType: { tmdbId, mediaType } }, select: { id: true, title: true, posterPath: true, originalLanguage: true, createdAt: true, updatedAt: true } });
      if (movie?.posterPath) continue;
      const details: any = mediaType === 'movie' ? await getMovieDetails(tmdbId) : await getTVDetails(tmdbId);
      const posters = await getPosterCandidates(tmdbId, mediaType, movie?.originalLanguage ?? details.original_language);
      const jobs = await prisma.ingestionJob.findMany({ where: { dedupeKey: { startsWith: `fetch:${mediaType}:${tmdbId}:` } }, select: { type: true, status: true, attempts: true, errorMessage: true, createdAt: true }, orderBy: { createdAt: 'desc' }, take: 3 });
      console.log(JSON.stringify({
        kind: 'posterless', tmdbId, mediaType, catalogRow: movie ?? null,
        tmdbDetailsPosterPath: details.poster_path ?? null, tmdbOriginalLanguage: details.original_language,
        tmdbImagePosters: posters.map((p) => `${p.iso_639_1 ?? 'null'}${p.file_path}`).slice(0, 6),
        fetchJobs: jobs,
        verdict: !movie ? 'no catalog row (FETCH not drained?)' : posters.length === 0 && !details.poster_path ? 'TMDB has no poster' : details.poster_path ? 'TMDB has poster now; row saved before it existed' : 'poster exists only under /images; details poster_path null',
      }, null, 2));
    }
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => { console.error(`[${SCRIPT_NAME}] failed:`, error); process.exit(1); });
