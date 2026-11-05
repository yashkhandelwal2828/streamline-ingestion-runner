/**
 * Maps a Netflix title to a TMDB id. Netflix's files carry only a title, a media type and the week,
 * so matching is deliberately conservative: a candidate must equal the Netflix title (or the TMDB
 * original title) after normalization, and cannot have been released after the chart week.
 * Ties go to the most-voted, then most popular candidate. When a release year is known
 * (`yearHint`, e.g. from a manual override) candidates within one year win first.
 */

export type TmdbSearchCandidate = {
  id: number;
  title?: string;
  name?: string;
  original_title?: string;
  original_name?: string;
  release_date?: string;
  first_air_date?: string;
  popularity?: number;
  vote_count?: number;
};

export const normalizeTitleKey = (value: string) =>
  value
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/['’`]/g, '')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim()
    .replace(/\s+/g, ' ');

const candidateYear = (candidate: TmdbSearchCandidate) => {
  const date = candidate.release_date || candidate.first_air_date || '';
  const year = Number.parseInt(date.slice(0, 4), 10);
  return Number.isInteger(year) ? year : null;
};

export type MatchOptions = {
  title: string;
  /** Last day of the earliest Netflix week the title charted; candidates released later are rejected. */
  weekDate: string;
  yearHint?: number | null;
};

export const pickBestCandidate = (
  candidates: TmdbSearchCandidate[],
  { title, weekDate, yearHint = null }: MatchOptions,
): { tmdbId: number; matchedTitle: string } | null => {
  const wanted = normalizeTitleKey(title);
  if (!wanted) return null;

  const matches = candidates.filter((candidate) => {
    const names = [candidate.title, candidate.name, candidate.original_title, candidate.original_name]
      .filter((value): value is string => typeof value === 'string' && value.length > 0)
      .map(normalizeTitleKey);
    if (!names.includes(wanted)) return false;
    const date = candidate.release_date || candidate.first_air_date;
    if (date && /^\d{4}-\d{2}-\d{2}$/.test(date) && date > weekDate) return false;
    return true;
  });

  if (matches.length === 0) return null;

  const score = (candidate: TmdbSearchCandidate) => {
    const year = candidateYear(candidate);
    const yearBonus = yearHint && year && Math.abs(year - yearHint) <= 1 ? 1 : 0;
    return [yearBonus, candidate.vote_count ?? 0, candidate.popularity ?? 0];
  };

  const best = [...matches].sort((a, b) => {
    const left = score(a);
    const right = score(b);
    for (let i = 0; i < left.length; i += 1) {
      if (right[i] !== left[i]) return right[i] - left[i];
    }
    return a.id - b.id;
  })[0];

  return { tmdbId: best.id, matchedTitle: best.title || best.name || title };
};
