const test = require('node:test');
const assert = require('node:assert/strict');
const { loadTsModule } = require('./helpers/load-ts');

const { parseNetflixTsv, normalizeNetflixCategory, NetflixFormatError } = loadTsModule('src/services/netflixTop10/parse.ts');

const COUNTRIES_HEADER = 'country_name\tcountry_iso2\tweek\tcategory\tweekly_rank\tshow_title\tseason_title\tcumulative_weeks_in_top_10';
const GLOBAL_HEADER = 'week\tcategory\tweekly_rank\tshow_title\tseason_title\tweekly_hours_viewed\truntime\tweekly_views\tcumulative_weeks_in_top_10';

test('parses country rows, keeps only the newest week and maps categories to media types', () => {
  const text = [
    COUNTRIES_HEADER,
    'India\tIN\t2026-10-25\tFilms\t1\tOld Film\tN/A\t3',
    'India\tIN\t2026-11-01\tFilms\t1\tGajab Raat\tN/A\t2',
    'India\tIN\t2026-11-01\tTV\t2\tThe Great Indian Kapil Show\tThe Great Indian Kapil Show: Season 5\t6',
    '',
  ].join('\n');
  const result = parseNetflixTsv(text, 'countries');
  assert.deepEqual(result.weeks, ['2026-11-01']);
  assert.equal(result.rows.length, 2);
  assert.deepEqual(result.rows[0], {
    scope: 'IN', week: '2026-11-01', category: 'films', rank: 1, weeksInTop10: 2,
    showTitle: 'Gajab Raat', seasonTitle: null, mediaType: 'movie',
  });
  assert.equal(result.rows[1].mediaType, 'tv');
  assert.equal(result.rows[1].seasonTitle, 'The Great Indian Kapil Show: Season 5');
});

test('can keep several weeks for a backfill', () => {
  const text = [COUNTRIES_HEADER,
    'US\tUS\t2026-10-18\tTV\t1\tA\tN/A\t1', 'US\tUS\t2026-10-25\tTV\t1\tB\tN/A\t1', 'US\tUS\t2026-11-01\tTV\t1\tC\tN/A\t1'].join('\n');
  const result = parseNetflixTsv(text, 'countries', { weeks: 2 });
  assert.deepEqual(result.weeks, ['2026-11-01', '2026-10-25']);
  assert.deepEqual(result.rows.map((row) => row.showTitle), ['B', 'C']);
});

test('parses the global file into GLOBAL scope with language-split categories and handles CRLF + BOM', () => {
  const text = `﻿${[GLOBAL_HEADER,
    '2026-11-01\tFilms (English)\t1\tAnimals\tN/A\t8400000\t1.8\t4700000\t4',
    '2026-11-01\tTV (Non-English)\t3\tLupin\tLupin: Part 4\t1\t1\t1\t9'].join('\r\n')}\r\n`;
  const result = parseNetflixTsv(text, 'global');
  assert.equal(result.rows.length, 2);
  assert.equal(result.rows[0].scope, 'GLOBAL');
  assert.equal(result.rows[0].category, 'films-english');
  assert.equal(result.rows[1].category, 'tv-non-english');
  assert.equal(result.rows[1].mediaType, 'tv');
  assert.equal(result.rows[1].weeksInTop10, 9);
});

test('skips malformed rows but counts them', () => {
  const text = [COUNTRIES_HEADER,
    'India\tIN\t2026-11-01\tFilms\t11\tRank too high\tN/A\t1',
    'India\tIN\t2026-11-01\tFilms\t2\t\tN/A\t1',
    'India\tIN\t2026-11-01\tShorts\t3\tUnknown category\tN/A\t1',
    'India\tIN\t2026-11-01\tFilms\t4\tBad weeks\tN/A\t0',
    'India\tin1\t2026-11-01\tFilms\t5\tBad country\tN/A\t1',
    'India\tIN\t2026-11-01\tFilms\t6\tValid\tN/A\t1'].join('\n');
  const result = parseNetflixTsv(text, 'countries');
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].showTitle, 'Valid');
  assert.equal(result.skippedRows, 5);
});

test('throws when Netflix changes the file format instead of ingesting garbage', () => {
  assert.throws(() => parseNetflixTsv('country\tweek\nIN\t2026-11-01', 'countries'), NetflixFormatError);
  assert.throws(() => parseNetflixTsv('', 'global'), NetflixFormatError);
});

test('category normalization', () => {
  assert.deepEqual(normalizeNetflixCategory('Films'), { category: 'films', mediaType: 'movie' });
  assert.deepEqual(normalizeNetflixCategory('TV (English)'), { category: 'tv-english', mediaType: 'tv' });
  assert.equal(normalizeNetflixCategory('Documentaries'), null);
});
