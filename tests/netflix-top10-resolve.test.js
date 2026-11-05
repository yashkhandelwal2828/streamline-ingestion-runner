const test = require('node:test');
const assert = require('node:assert/strict');
const { loadTsModule } = require('./helpers/load-ts');

const { pickBestCandidate, stripSeasonSuffix, titleVariants } = loadTsModule('src/services/netflixTop10/matcher.ts');
const { resolveNetflixTitle } = loadTsModule('src/services/netflixTop10/resolve.ts');
const { pickPosterPath } = loadTsModule('src/services/netflixTop10/poster.ts');

const WEEK = '2026-11-01';

test('strips season markers Netflix appends to TV titles', () => {
  assert.equal(stripSeasonSuffix('Love Is Blind: S11: Boston'), 'Love Is Blind');
  assert.equal(stripSeasonSuffix('Roadies Rebirth: Season 21'), 'Roadies Rebirth');
  assert.equal(stripSeasonSuffix('Dead-End Job: Limited Series'), 'Dead-End Job');
  assert.equal(stripSeasonSuffix('Lupin: Part 4'), 'Lupin');
  assert.equal(stripSeasonSuffix('Sigma'), 'Sigma');
});

test('title variants run from the given title to the pre-colon form, without duplicates or tiny strings', () => {
  assert.deepEqual(titleVariants('Akka: The Protectors'), ['Akka: The Protectors', 'Akka']);
  assert.deepEqual(titleVariants('Roadies Rebirth: Season 21'), ['Roadies Rebirth: Season 21', 'Roadies Rebirth']);
  assert.deepEqual(titleVariants('Sigma'), ['Sigma']);
  assert.deepEqual(titleVariants('Raw: October 26, 2026'), ['Raw: October 26, 2026']);
});

test('exact tier tolerates a TMDB release date a few weeks after the chart week but not months', () => {
  const near = [{ id: 1, title: 'Gajab Raat', release_date: '2026-11-20', vote_count: 2 }];
  assert.equal(pickBestCandidate(near, { title: 'Gajab Raat', weekDate: WEEK }).tmdbId, 1);
  const far = [{ id: 2, title: 'Gajab Raat', release_date: '2027-03-01', vote_count: 2 }];
  assert.equal(pickBestCandidate(far, { title: 'Gajab Raat', weekDate: WEEK }), null);
});

test('lenient tier matches a subtitle or leading article difference only for recent productions', () => {
  const recent = [{ id: 9, name: 'Akka', first_air_date: '2026-10-20', vote_count: 1 }];
  assert.equal(pickBestCandidate(recent, { title: 'Akka: The Protectors', weekDate: WEEK }), null, 'exact tier stays strict');
  assert.equal(pickBestCandidate(recent, { title: 'Akka: The Protectors', weekDate: WEEK, tier: 'lenient' }).tmdbId, 9);
  const old = [{ id: 10, name: 'Akka', first_air_date: '1999-01-01', vote_count: 900 }];
  assert.equal(pickBestCandidate(old, { title: 'Akka: The Protectors', weekDate: WEEK, tier: 'lenient' }), null);
  const article = [{ id: 11, title: 'Paradise', release_date: '2026-09-01' }];
  assert.equal(pickBestCandidate(article, { title: 'The Paradise', weekDate: WEEK, tier: 'lenient' }).tmdbId, 11);
});

test('lenient tier never accepts unrelated or future titles', () => {
  const candidates = [
    { id: 1, title: 'Beat Luke', release_date: '2026-09-01' },
    { id: 2, name: 'Akka Returns', first_air_date: '2027-06-01' },
  ];
  assert.equal(pickBestCandidate(candidates, { title: 'Beat Luke Littler', weekDate: WEEK, tier: 'lenient' }), null);
  assert.equal(pickBestCandidate(candidates, { title: 'Akka: The Protectors', weekDate: WEEK, tier: 'lenient' }), null);
});

const makeResolveDeps = ({ search = {}, multi = {}, alternatives = {} } = {}) => {
  const calls = [];
  return {
    calls,
    deps: {
      search: async (title, mediaType) => { calls.push(`search:${mediaType}:${title}`); return search[`${mediaType}:${title}`] ?? []; },
      searchMulti: async (title) => { calls.push(`multi:${title}`); return multi[title] ?? []; },
      alternativeTitles: async (id) => { calls.push(`alt:${id}`); return alternatives[id] ?? []; },
    },
  };
};

test('resolver stops at the first exact hit with a single search', async () => {
  const { deps, calls } = makeResolveDeps({ search: { 'movie:Sigma': [{ id: 5, title: 'Sigma', release_date: '2026-05-01', vote_count: 4 }] } });
  const hit = await resolveNetflixTitle(deps, { title: 'Sigma', mediaType: 'movie', weekDate: WEEK });
  assert.deepEqual([hit.tmdbId, hit.strategy], [5, 'exact']);
  assert.deepEqual(calls, ['search:movie:Sigma']);
});

test('resolver finds a TV title through its season-stripped variant', async () => {
  const { deps } = makeResolveDeps({ search: { 'tv:Roadies Rebirth': [{ id: 77, name: 'Roadies Rebirth', first_air_date: '2026-08-01', vote_count: 3 }] } });
  const hit = await resolveNetflixTitle(deps, { title: 'Roadies Rebirth: Season 21', mediaType: 'tv', weekDate: WEEK });
  assert.deepEqual([hit.tmdbId, hit.strategy], [77, 'variant']);
});

test('resolver falls back to /search/multi and ignores results of the wrong media type', async () => {
  const { deps } = makeResolveDeps({
    multi: { 'Instadocs: Jane Doe v. Cornell': [
      { id: 1, media_type: 'movie', title: 'Instadocs: Jane Doe v. Cornell', release_date: '2026-10-01' },
      { id: 2, media_type: 'tv', name: 'Instadocs: Jane Doe v. Cornell', first_air_date: '2026-10-01' },
    ] },
  });
  const hit = await resolveNetflixTitle(deps, { title: 'Instadocs: Jane Doe v. Cornell', mediaType: 'tv', weekDate: WEEK });
  assert.deepEqual([hit.tmdbId, hit.strategy], [2, 'multi']);
  const none = await resolveNetflixTitle(makeResolveDeps({ multi: { Foo: [{ id: 1, media_type: 'movie', title: 'Foo', release_date: '2026-10-01' }] } }).deps, { title: 'Foo', mediaType: 'tv', weekDate: WEEK });
  assert.equal(none, null);
});

test('resolver uses the lenient tier on candidates gathered by the pre-colon search', async () => {
  const { deps } = makeResolveDeps({ search: { 'tv:Akka': [{ id: 9, name: 'Akka', first_air_date: '2026-10-20', vote_count: 1 }] } });
  const hit = await resolveNetflixTitle(deps, { title: 'Akka: The Protectors', mediaType: 'tv', weekDate: WEEK });
  assert.deepEqual([hit.tmdbId, hit.strategy], [9, 'lenient']);
});

test('resolver matches through TMDB alternative titles, with the release-date guard still applied', async () => {
  const candidate = { id: 31, title: 'Avarachan and Sons', original_title: 'അവറാച്ചൻ & സൺസ്', release_date: '2026-10-25', popularity: 5 };
  const { deps } = makeResolveDeps({
    search: { 'movie:Avarachan & Sons': [{ ...candidate, title: 'Avarachante Makkal' }] },
    alternatives: { 31: ['Avarachan & Sons'] },
  });
  const hit = await resolveNetflixTitle(deps, { title: 'Avarachan & Sons', mediaType: 'movie', weekDate: WEEK });
  assert.deepEqual([hit.tmdbId, hit.strategy], [31, 'alternative']);
  const late = makeResolveDeps({
    search: { 'movie:Avarachan & Sons': [{ ...candidate, title: 'Avarachante Makkal', release_date: '2028-01-01' }] },
    alternatives: { 31: ['Avarachan & Sons'] },
  });
  assert.equal(await resolveNetflixTitle(late.deps, { title: 'Avarachan & Sons', mediaType: 'movie', weekDate: WEEK }), null);
});

test('resolver returns null when nothing is close, and propagates TMDB errors instead of caching them as unmatched', async () => {
  const { deps } = makeResolveDeps();
  assert.equal(await resolveNetflixTitle(deps, { title: 'Beat Luke Littler', mediaType: 'tv', weekDate: WEEK }), null);
  await assert.rejects(resolveNetflixTitle({ search: async () => { throw new Error('429'); } }, { title: 'X Y Z', mediaType: 'tv', weekDate: WEEK }), /429/);
});

test('poster choice prefers the original language, then English, then language-neutral art', () => {
  const posters = [
    { file_path: '/null.jpg', iso_639_1: null, vote_average: 9 },
    { file_path: '/en.jpg', iso_639_1: 'en', vote_average: 5 },
    { file_path: '/hi-low.jpg', iso_639_1: 'hi', vote_average: 1 },
    { file_path: '/hi-high.jpg', iso_639_1: 'hi', vote_average: 4 },
  ];
  assert.equal(pickPosterPath(posters, 'hi'), '/hi-high.jpg');
  assert.equal(pickPosterPath(posters, 'ko'), '/en.jpg');
  assert.equal(pickPosterPath(posters.filter((p) => !p.iso_639_1 || p.iso_639_1 === 'hi'), 'ko'), '/null.jpg');
  assert.equal(pickPosterPath([], 'hi'), null);
  assert.equal(pickPosterPath([{ file_path: '' }, { iso_639_1: 'en' }], 'en'), null);
});

test('re-fetching a title without artwork never erases a poster already stored', async () => {
  const { createPrismaIngestionStore } = loadTsModule('src/services/ingestion/jobQueue.ts');
  const upserts = [];
  const store = createPrismaIngestionStore({ movie: { upsert: async (args) => { upserts.push(args); return { id: 1 }; } } });
  await store.upsertMovie({ tmdbId: 1, mediaType: 'movie', title: 'X', posterPath: null });
  assert.equal('posterPath' in upserts[0].update, false);
  assert.equal(upserts[0].create.posterPath, null);
  await store.upsertMovie({ tmdbId: 1, mediaType: 'movie', title: 'X', posterPath: '/new.jpg' });
  assert.equal(upserts[1].update.posterPath, '/new.jpg');
});
