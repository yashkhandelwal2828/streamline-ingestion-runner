import { normalizeTitleKey, pickBestCandidate, stripSeasonSuffix, titleVariants, type TmdbSearchCandidate } from './matcher';

export type ResolveDeps = {
  search: (title: string, mediaType: 'movie' | 'tv') => Promise<TmdbSearchCandidate[]>;
  /** /search/multi results; entries carry `media_type`. Optional so older callers keep working. */
  searchMulti?: (title: string) => Promise<Array<TmdbSearchCandidate & { media_type?: string }>>;
  alternativeTitles?: (tmdbId: number, mediaType: 'movie' | 'tv') => Promise<string[]>;
};

export type ResolvedTitle = { tmdbId: number; matchedTitle: string; strategy: 'exact' | 'variant' | 'multi' | 'lenient' | 'alternative' };

const MAX_ALTERNATIVE_LOOKUPS = 3;

/**
 * Finds the TMDB entry for a Netflix title. Tries the cheap, strict strategies first and stops at
 * the first hit; each later strategy is a little more lenient:
 *   1. exact normalized title        2. season-stripped / pre-colon variants
 *   3. /search/multi                 4. lenient (article / subtitle) over everything seen
 *   5. TMDB alternative titles for the best-looking near misses.
 * The media type always has to agree, so a film never matches a series. Errors from TMDB propagate
 * (the caller must not cache a failed lookup as "unmatched").
 */
export const resolveNetflixTitle = async (
  deps: ResolveDeps,
  input: { title: string; mediaType: 'movie' | 'tv'; weekDate: string },
): Promise<ResolvedTitle | null> => {
  const { title, mediaType, weekDate } = input;
  const variants = titleVariants(title);
  // Only these names may count as an exact match. The pre-colon variant is just a search string:
  // its results are kept as candidates for the recency-guarded lenient tier.
  const exactNames = [title, stripSeasonSuffix(title)];
  const seen = new Map<number, TmdbSearchCandidate>();
  const remember = (candidates: TmdbSearchCandidate[]) => candidates.forEach((candidate) => { if (!seen.has(candidate.id)) seen.set(candidate.id, candidate); });
  const accept = (best: { tmdbId: number; matchedTitle: string } | null, strategy: ResolvedTitle['strategy']): ResolvedTitle | null =>
    best ? { ...best, strategy } : null;

  for (const [index, variant] of variants.entries()) {
    const candidates = await deps.search(variant, mediaType);
    remember(candidates);
    const hit = pickBestCandidate(candidates, { title, weekDate, extraNames: exactNames });
    if (hit) return accept(hit, index === 0 ? 'exact' : 'variant');
  }

  if (deps.searchMulti) {
    for (const variant of variants.slice(0, 2)) {
      const results = (await deps.searchMulti(variant)).filter((result) => result.media_type === mediaType);
      remember(results);
      const hit = pickBestCandidate(results, { title, weekDate, extraNames: exactNames });
      if (hit) return accept(hit, 'multi');
    }
  }

  const everything = [...seen.values()];
  const lenient = pickBestCandidate(everything, { title, weekDate, extraNames: exactNames, tier: 'lenient' });
  if (lenient) return accept(lenient, 'lenient');

  if (deps.alternativeTitles && everything.length > 0) {
    const wantedTokens = new Set(normalizeTitleKey(title).split(' ').filter((token) => token.length > 2));
    const nearMisses = everything
      .filter((candidate) => [candidate.title, candidate.name, candidate.original_title, candidate.original_name]
        .some((name) => name && normalizeTitleKey(name).split(' ').some((token) => wantedTokens.has(token))))
      .sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0))
      .slice(0, MAX_ALTERNATIVE_LOOKUPS);
    const variantKeys = new Set(exactNames.map(normalizeTitleKey));
    for (const candidate of nearMisses) {
      const alternatives = await deps.alternativeTitles(candidate.id, mediaType);
      const matchedAlternative = alternatives.find((alternative) => variantKeys.has(normalizeTitleKey(alternative)));
      if (!matchedAlternative) continue;
      // Reuse the exact tier (and its release-date guard) by presenting the alternative as the candidate's original title.
      const hit = pickBestCandidate([{ ...candidate, original_title: matchedAlternative }], { title, weekDate, extraNames: exactNames });
      if (hit) return accept(hit, 'alternative');
    }
  }
  return null;
};
