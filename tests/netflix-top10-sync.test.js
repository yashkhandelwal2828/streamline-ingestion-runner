const test = require('node:test');
const assert = require('node:assert/strict');
const { loadTsModule } = require('./helpers/load-ts');

const { syncNetflixTop10 } = loadTsModule('src/services/netflixTop10/sync.ts');

const COUNTRIES = [
  'country_name\tcountry_iso2\tweek\tcategory\tweekly_rank\tshow_title\tseason_title\tcumulative_weeks_in_top_10',
  'India\tIN\t2026-11-01\tFilms\t1\tGajab Raat\tN/A\t2',
  'India\tIN\t2026-11-01\tTV\t1\tAkka\tAkka: Season 1\t1',
  'United States\tUS\t2026-11-01\tFilms\t1\tGajab Raat\tN/A\t1',
  'United States\tUS\t2026-11-01\tFilms\t2\tNo Such Title\tN/A\t1',
].join('\n');
const GLOBAL = [
  'week\tcategory\tweekly_rank\tshow_title\tseason_title\tweekly_hours_viewed\truntime\tweekly_views\tcumulative_weeks_in_top_10',
  '2026-11-01\tFilms (English)\t1\tGajab Raat\tN/A\t1\t1\t1\t4',
].join('\n');

const makeDeps = ({ existingMovies = [], cachedMatches = [], searchResults = {} } = {}) => {
  const calls = { searches: [], entryOps: [], matchUpserts: [], jobs: [] };
  const prisma = {
    netflixTitleMatch: {
      findMany: async () => cachedMatches,
      upsert: async (args) => { calls.matchUpserts.push(args); },
    },
    netflixTop10Entry: {
      deleteMany: (args) => ({ op: 'deleteMany', args }),
      createMany: (args) => ({ op: 'createMany', args }),
    },
    movie: { findMany: async () => existingMovies },
    $transaction: async (ops) => { calls.entryOps.push(...ops); },
  };
  const store = { enqueueJob: async (data) => { calls.jobs.push(data); return data; } };
  const deps = {
    prisma, store,
    fetchText: async (url) => (url.includes('countries') ? COUNTRIES : GLOBAL),
    search: async (title, mediaType) => { calls.searches.push([title, mediaType]); return searchResults[`${mediaType}:${title}`] ?? []; },
    now: () => new Date('2026-11-04T06:00:00Z'),
  };
  return { deps, calls };
};

const results = {
  'movie:Gajab Raat': [{ id: 501, title: 'Gajab Raat', release_date: '2026-10-01', vote_count: 3 }],
  'tv:Akka': [{ id: 902, name: 'Akka', first_air_date: '2026-10-20', vote_count: 3 }],
};

test('stores entries per scope/week with TMDB ids, searching each unique title once', async () => {
  const { deps, calls } = makeDeps({ searchResults: results });
  const summary = await syncNetflixTop10(deps);
  assert.equal(calls.searches.length, 3); // Gajab Raat (movie), Akka (tv), No Such Title (movie)
  const creates = calls.entryOps.filter((op) => op.op === 'createMany');
  const scopes = creates.map((op) => op.args.data[0].scope).sort();
  assert.deepEqual(scopes, ['GLOBAL', 'IN', 'US']);
  const indian = creates.find((op) => op.args.data[0].scope === 'IN').args.data;
  assert.deepEqual(indian.map((row) => [row.category, row.rank, row.tmdbId, row.mediaType]), [['films', 1, 501, 'movie'], ['tv', 1, 902, 'tv']]);
  const unmatched = creates.find((op) => op.args.data[0].scope === 'US').args.data.find((row) => row.rank === 2);
  assert.equal(unmatched.tmdbId, null);
  assert.equal(summary.entries, 5);
  assert.equal(summary.matchedTitles, 2);
  assert.equal(summary.unmatchedTitles, 1);
  assert.equal(summary.entryMatchRate, 0.8);
  assert.equal(calls.matchUpserts.find((u) => u.where.titleKey_mediaType.titleKey === 'no such title').create.status, 'UNMATCHED');
});

test('replaces each scope/week atomically (delete before create) so reruns are idempotent', async () => {
  const { deps, calls } = makeDeps({ searchResults: results });
  await syncNetflixTop10(deps);
  const ops = calls.entryOps.map((op) => op.op);
  assert.deepEqual(ops, ['deleteMany', 'createMany', 'deleteMany', 'createMany', 'deleteMany', 'createMany']);
  assert.equal(calls.entryOps[0].args.where.week.toISOString(), '2026-11-01T00:00:00.000Z');
});

test('queues only titles missing from Movie through the FETCH pipeline with the netflix source', async () => {
  const { deps, calls } = makeDeps({ searchResults: results, existingMovies: [{ tmdbId: 501 }] });
  const summary = await syncNetflixTop10(deps);
  assert.equal(summary.fetchJobsEnqueued, 1);
  assert.equal(calls.jobs[0].type, 'FETCH');
  assert.equal(calls.jobs[0].source, 'netflix_top10');
  assert.deepEqual(calls.jobs[0].payload, { tmdbId: 902, mediaType: 'tv' });
  assert.equal(calls.jobs[0].dedupeKey, 'fetch:tv:902:GLOBAL');
  assert.ok(calls.jobs[0].priority >= 200, 'must reach OMDb deep enrichment');
});

test('uses the match cache and rechecks stale unmatched titles only', async () => {
  const recent = new Date('2026-11-01T00:00:00Z');
  const stale = new Date('2026-09-01T00:00:00Z');
  const { deps, calls } = makeDeps({
    searchResults: results,
    cachedMatches: [
      { titleKey: 'gajab raat', mediaType: 'movie', tmdbId: 501, status: 'MATCHED', checkedAt: stale },
      { titleKey: 'no such title', mediaType: 'movie', tmdbId: null, status: 'UNMATCHED', checkedAt: recent },
      { titleKey: 'akka', mediaType: 'tv', tmdbId: null, status: 'UNMATCHED', checkedAt: stale },
    ],
  });
  const summary = await syncNetflixTop10(deps);
  assert.deepEqual(calls.searches, [['Akka', 'tv']]);
  assert.equal(summary.cachedMatches, 2);
});

test('honors the TMDB search cap and leaves deferred titles unmatched without caching them', async () => {
  const { deps, calls } = makeDeps({ searchResults: results });
  const summary = await syncNetflixTop10(deps, { maxSearches: 1 });
  assert.equal(calls.searches.length, 1);
  assert.equal(summary.searchesDeferred, 2);
  assert.equal(calls.matchUpserts.length, 1);
});

test('dry run parses and reports without TMDB calls or writes', async () => {
  const { deps, calls } = makeDeps({ searchResults: results });
  const summary = await syncNetflixTop10(deps, { dryRun: true });
  assert.equal(calls.searches.length, 0);
  assert.equal(calls.entryOps.length, 0);
  assert.equal(calls.jobs.length, 0);
  assert.equal(summary.entries, 5);
  assert.equal(summary.dryRun, true);
});

test('refuses to continue when the downloaded files contain no usable rows', async () => {
  const { deps } = makeDeps();
  deps.fetchText = async () => 'week\tcategory\tweekly_rank\tshow_title\tseason_title\tcumulative_weeks_in_top_10\ncountry_iso2\n';
  await assert.rejects(syncNetflixTop10(deps));
});
