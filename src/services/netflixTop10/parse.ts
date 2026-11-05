/**
 * Parser for Netflix's official weekly Top 10 data files (https://www.netflix.com/tudum/top10).
 *
 * Verified 2026-11-05 against the live files:
 *  - all-weeks-countries.tsv columns: country_name, country_iso2, week, category (Films|TV),
 *    weekly_rank, show_title, season_title, cumulative_weeks_in_top_10
 *  - all-weeks-global.tsv columns: week, category (Films|TV (English|Non-English)), weekly_rank,
 *    show_title, season_title, weekly_hours_viewed, runtime, weekly_views, cumulative_weeks_in_top_10
 * Files are tab separated, unquoted, with a trailing newline. `week` is the last day (Sunday) of the
 * Netflix week. There is no release year or TMDB id in either file.
 */

export type NetflixFileKind = 'countries' | 'global';

export type NetflixTop10Row = {
  scope: string; // ISO2 country code or "GLOBAL"
  week: string; // YYYY-MM-DD
  category: string; // films | tv | films-english | films-non-english | tv-english | tv-non-english
  rank: number;
  weeksInTop10: number;
  showTitle: string;
  seasonTitle: string | null;
  mediaType: 'movie' | 'tv';
};

export type NetflixParseResult = {
  rows: NetflixTop10Row[];
  weeks: string[];
  skippedRows: number;
  totalRows: number;
};

const REQUIRED_COLUMNS: Record<NetflixFileKind, string[]> = {
  countries: ['country_iso2', 'week', 'category', 'weekly_rank', 'show_title', 'season_title', 'cumulative_weeks_in_top_10'],
  global: ['week', 'category', 'weekly_rank', 'show_title', 'season_title', 'cumulative_weeks_in_top_10'],
};

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const COUNTRY_PATTERN = /^[A-Z]{2}$/;

export const normalizeNetflixCategory = (raw: string): { category: string; mediaType: 'movie' | 'tv' } | null => {
  const value = raw.trim().toLowerCase();
  const match = /^(films|tv)(?:\s*\((english|non-english)\))?$/.exec(value);
  if (!match) {
    return null;
  }
  const base = match[1];
  const category = match[2] ? `${base}-${match[2]}` : base;
  return { category, mediaType: base === 'films' ? 'movie' : 'tv' };
};

export class NetflixFormatError extends Error {}

/**
 * Parses a Netflix TSV. Only the newest `weeks` distinct weeks are kept (the countries file holds
 * every week since 2021 and is >30 MB). Malformed rows are skipped and counted; a missing required
 * column throws so a silent upstream format change fails the job instead of ingesting garbage.
 */
export const parseNetflixTsv = (
  text: string,
  kind: NetflixFileKind,
  options: { weeks?: number } = {},
): NetflixParseResult => {
  const weeksToKeep = Math.max(1, options.weeks ?? 1);
  const lines = text.replace(/^﻿/, '').split(/\r?\n/);
  const header = (lines[0] ?? '').split('\t').map((column) => column.trim());
  const index = new Map(header.map((column, position) => [column, position] as const));

  for (const column of REQUIRED_COLUMNS[kind]) {
    if (!index.has(column)) {
      throw new NetflixFormatError(`Netflix ${kind} file is missing required column "${column}"; header was: ${header.join(',')}`);
    }
  }

  const get = (cells: string[], column: string) => (cells[index.get(column) as number] ?? '').trim();

  // First pass: find the newest weeks without materializing 500k rows.
  const weekColumn = index.get('week') as number;
  const seenWeeks = new Set<string>();
  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) continue;
    const week = (line.split('\t')[weekColumn] ?? '').trim();
    if (DATE_PATTERN.test(week)) seenWeeks.add(week);
  }
  const weeks = [...seenWeeks].sort().reverse().slice(0, weeksToKeep);
  const keep = new Set(weeks);

  const rows: NetflixTop10Row[] = [];
  let skippedRows = 0;
  let totalRows = 0;

  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line) continue;
    const cells = line.split('\t');
    const week = get(cells, 'week');
    if (!keep.has(week)) continue;
    totalRows += 1;

    const category = normalizeNetflixCategory(get(cells, 'category'));
    const rank = Number.parseInt(get(cells, 'weekly_rank'), 10);
    const weeksInTop10 = Number.parseInt(get(cells, 'cumulative_weeks_in_top_10'), 10);
    const showTitle = get(cells, 'show_title');
    const scope = kind === 'global' ? 'GLOBAL' : get(cells, 'country_iso2').toUpperCase();
    const seasonRaw = get(cells, 'season_title');

    if (
      !category || !showTitle || !Number.isInteger(rank) || rank < 1 || rank > 10
      || !Number.isInteger(weeksInTop10) || weeksInTop10 < 1
      || (kind === 'countries' && !COUNTRY_PATTERN.test(scope))
    ) {
      skippedRows += 1;
      continue;
    }

    rows.push({
      scope,
      week,
      category: category.category,
      rank,
      weeksInTop10,
      showTitle,
      seasonTitle: seasonRaw && seasonRaw.toUpperCase() !== 'N/A' ? seasonRaw : null,
      mediaType: category.mediaType,
    });
  }

  return { rows, weeks, skippedRows, totalRows };
};
