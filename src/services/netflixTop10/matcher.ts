/**
 * Maps a Netflix title to a TMDB id. Netflix's files carry only a title, a media type and the week,
 * so matching stays conservative. Two tiers:
 *  - exact: the candidate's title/name/original title equals the Netflix title after normalization;
 *  - lenient: ignoring a leading article, or a ": subtitle" on either side (e.g. Netflix
 *    "Akka: The Protectors" vs TMDB "Akka"), and only for productions released within
 *    LENIENT_WINDOW_YEARS before the chart week.
 * Neither tier accepts a candidate released after the chart week (plus RELEASE_SLACK_DAYS, because
 * TMDB sometimes lists a theatrical date after the streaming premiere). Ties go to the most-voted,
 * then most popular candidate. With a `yearHint`, candidates within one year win first.
 */
export const RELEASE_SLACK_DAYS = 31;
export const LENIENT_WINDOW_YEARS = 3;

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
  /** `lenient` also accepts article/subtitle differences; see the file header. */
  tier?: 'exact' | 'lenient';
  /** Extra names for the wanted title (e.g. Netflix title variants, TMDB alternative titles). */
  extraNames?: string[];
};

/** Removes season/part/series markers Netflix appends to TV titles, e.g. "Love Is Blind: S11: Boston". */
export const stripSeasonSuffix = (value: string) =>
  value
    .replace(/\s*[:\-–—]\s*(?:season|series|part|volume|vol\.?|chapter|book|limited series|miniseries)\b.*$/i, '')
    .replace(/\s*:\s*S\d+\b.*$/i, '')
    .replace(/\s*\((?:\d{4}|[^)]*season[^)]*)\)\s*$/i, '')
    .trim();

const dropArticle = (key: string) => key.replace(/^(?:the|a|an)\s+/, '');
const beforeColon = (value: string) => {
  const index = value.indexOf(':');
  return index > 0 ? value.slice(0, index) : value;
};

/** Search strings to try, most specific first: as given, season-stripped, then the part before ": ". */
export const titleVariants = (title: string): string[] => {
  const stripped = stripSeasonSuffix(title);
  // The title as given is always searched; derived variants must be long enough not to match everything.
  const variants = [title.trim(), stripped, beforeColon(stripped)]
    .filter((value, index) => index === 0 || normalizeTitleKey(value).length >= 4);
  const seen = new Set<string>();
  return variants.filter((value) => {
    const key = normalizeTitleKey(value);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const addDays = (date: string, days: number) => {
  const time = Date.parse(`${date}T00:00:00Z`);
  return Number.isNaN(time) ? date : new Date(time + days * 86_400_000).toISOString().slice(0, 10);
};

const candidateNames = (candidate: TmdbSearchCandidate) =>
  [candidate.title, candidate.name, candidate.original_title, candidate.original_name]
    .filter((value): value is string => typeof value === 'string' && value.length > 0);

export const pickBestCandidate = (
  candidates: TmdbSearchCandidate[],
  { title, weekDate, yearHint = null, tier = 'exact', extraNames = [] }: MatchOptions,
): { tmdbId: number; matchedTitle: string } | null => {
  const wanted = normalizeTitleKey(title);
  if (!wanted) return null;
  const wantedKeys = new Set([wanted, ...extraNames.map(normalizeTitleKey).filter(Boolean)]);
  const latestRelease = addDays(weekDate, RELEASE_SLACK_DAYS);
  const lenient = tier === 'lenient';
  const wantedLoose = new Set([title, ...extraNames].flatMap((raw) => [dropArticle(normalizeTitleKey(raw)), normalizeTitleKey(beforeColon(raw))]).filter(Boolean));
  const earliestLenient = `${Number.parseInt(weekDate.slice(0, 4), 10) - LENIENT_WINDOW_YEARS}${weekDate.slice(4)}`;

  const matches = candidates.filter((candidate) => {
    const names = candidateNames(candidate).map(normalizeTitleKey);
    const date = candidate.release_date || candidate.first_air_date;
    const datedOk = !date || !/^\d{4}-\d{2}-\d{2}$/.test(date) || date <= latestRelease;
    if (!datedOk) return false;
    if (names.some((name) => wantedKeys.has(name))) return true;
    if (!lenient) return false;
    // Lenient tier is only safe for recent productions with a known date.
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date) || date < earliestLenient) return false;
    return candidateNames(candidate).some((raw) => {
      const key = normalizeTitleKey(raw);
      const loose = [dropArticle(key), normalizeTitleKey(beforeColon(raw))].filter((value) => value.length >= 4);
      return loose.some((value) => wantedLoose.has(value));
    });
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
