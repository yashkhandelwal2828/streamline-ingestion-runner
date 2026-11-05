export type PosterCandidate = { file_path?: string; iso_639_1?: string | null; vote_average?: number; vote_count?: number };

/**
 * Chooses one poster from TMDB's /images response: the title's original language first (a Hindi film's
 * own artwork), then English, then language-neutral art; within a group the best-rated wins.
 */
export const pickPosterPath = (posters: PosterCandidate[], originalLanguage?: string | null): string | null => {
  const usable = posters.filter((poster) => typeof poster.file_path === 'string' && poster.file_path.startsWith('/'));
  if (usable.length === 0) return null;
  const rank = (poster: PosterCandidate) => {
    if (originalLanguage && poster.iso_639_1 === originalLanguage) return 0;
    if (poster.iso_639_1 === 'en') return 1;
    if (!poster.iso_639_1) return 2;
    return 3;
  };
  const best = [...usable].sort((a, b) =>
    rank(a) - rank(b)
    || (b.vote_average ?? 0) - (a.vote_average ?? 0)
    || (b.vote_count ?? 0) - (a.vote_count ?? 0)
    || String(a.file_path).localeCompare(String(b.file_path)))[0];
  return best.file_path as string;
};
