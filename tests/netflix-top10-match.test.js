const test = require('node:test');
const assert = require('node:assert/strict');
const { loadTsModule } = require('./helpers/load-ts');

const { normalizeTitleKey, pickBestCandidate } = loadTsModule('src/services/netflixTop10/matcher.ts');

test('normalizes accents, case, punctuation, ampersands and apostrophes', () => {
  assert.equal(normalizeTitleKey('Avarachan & Sons'), 'avarachan and sons');
  assert.equal(normalizeTitleKey("Don't Trouble the Trouble"), 'dont trouble the trouble');
  assert.equal(normalizeTitleKey('  Amélie: The   Film! '), 'amelie the film');
  assert.equal(normalizeTitleKey('Ice Age 4: Continental Drift'), 'ice age 4 continental drift');
  assert.equal(normalizeTitleKey('!!!'), '');
});

test('requires an exact normalized title match; fuzzy hits are rejected', () => {
  const candidates = [{ id: 1, title: 'Ice Age: The Meltdown', popularity: 90, vote_count: 9000, release_date: '2006-03-31' }];
  assert.equal(pickBestCandidate(candidates, { title: 'Ice Age', weekDate: '2026-11-01' }), null);
});

test('matches on original title as well as the localized title', () => {
  const candidates = [{ id: 7, title: 'The Greatest', original_title: 'सबसे महान', vote_count: 10, release_date: '2026-01-01' }];
  assert.equal(pickBestCandidate(candidates, { title: 'सबसे महान', weekDate: '2026-11-01' }).tmdbId, 7);
});

test('rejects candidates released after the chart week', () => {
  const candidates = [
    { id: 1, title: 'Lupin', first_air_date: '2027-05-01', vote_count: 9000 },
    { id: 2, title: 'Lupin', first_air_date: '2021-01-08', vote_count: 5000 },
  ];
  assert.equal(pickBestCandidate(candidates, { title: 'Lupin', weekDate: '2026-11-01' }).tmdbId, 2);
});

test('prefers the most-voted then most popular exact match, and a year hint wins first', () => {
  const candidates = [
    { id: 10, title: 'Dorothy', release_date: '1995-01-01', vote_count: 5, popularity: 1 },
    { id: 11, title: 'Dorothy', release_date: '2026-09-01', vote_count: 200, popularity: 30 },
    { id: 12, title: 'Dorothy', release_date: '2020-01-01', vote_count: 200, popularity: 10 },
  ];
  assert.equal(pickBestCandidate(candidates, { title: 'Dorothy', weekDate: '2026-11-01' }).tmdbId, 11);
  assert.equal(pickBestCandidate(candidates, { title: 'Dorothy', weekDate: '2026-11-01', yearHint: 1995 }).tmdbId, 10);
});

test('returns null for empty results or empty titles', () => {
  assert.equal(pickBestCandidate([], { title: 'Anything', weekDate: '2026-11-01' }), null);
  assert.equal(pickBestCandidate([{ id: 1, title: 'x' }], { title: '???', weekDate: '2026-11-01' }), null);
});
