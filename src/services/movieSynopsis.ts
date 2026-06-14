export type SynopsisSource = 'omdb' | 'tmdb';

export const parseOmdbPlot = (plot: unknown): string | null => {
  if (typeof plot !== 'string') return null;
  const trimmed = plot.trim();
  if (!trimmed || trimmed.toUpperCase() === 'N/A') return null;
  return trimmed;
};

export const buildDisplaySynopsis = (movie: {
  overview?: string | null;
  plotFull?: string | null;
}): { synopsis: string; synopsisSource: SynopsisSource } => {
  const overview = String(movie.overview ?? '').trim();
  const plotFull = String(movie.plotFull ?? '').trim();

  if (plotFull.length > overview.length) {
    return { synopsis: plotFull, synopsisSource: 'omdb' };
  }
  return { synopsis: overview || plotFull, synopsisSource: overview ? 'tmdb' : 'omdb' };
};
