const test = require('node:test');
const assert = require('node:assert/strict');
const { loadTsModule } = require('./helpers/load-ts');

const { runNextIngestionJob } = loadTsModule('src/services/ingestion/pipeline.ts');

const normalizeStore = (source, upserts, completions) => ({
  async claimNextJob() {
    return {
      id: 'job_norm', type: 'NORMALIZE', status: 'RUNNING', source, region: 'GLOBAL', priority: 250,
      attempts: 0, maxAttempts: 3, shadowMode: false,
      payload: { snapshotId: 's1', tmdbId: 8355, mediaType: 'movie' },
    };
  },
  async getSnapshotById() {
    return { id: 's1', payload: { id: 8355, title: 'Ice Age', release_date: '2002-03-10', overview: 'o', genres: [], credits: { crew: [], cast: [] }, 'watch/providers': { results: {} } } };
  },
  async upsertMovie(movie) { upserts.push(movie); return { id: 1, ...movie }; },
  async replacePlatformAvailability() {},
  async enqueueJob() { return { id: 'x' }; },
  async completeJob(id, result) { completions.push(result); },
  async rescheduleJob() { throw new Error('unexpected'); },
  async deadLetterJob() { throw new Error('unexpected'); },
});

test('catalog growth still skips titles older than the release-year floor', async () => {
  process.env.INGESTION_MIN_RELEASE_YEAR = '2010';
  const upserts = []; const completions = [];
  await runNextIngestionJob(normalizeStore('schedule', upserts, completions), { now: () => new Date('2026-11-04T00:00:00Z') });
  assert.equal(upserts.length, 0);
  assert.equal(completions[0].skipped, 'release_year_before_floor');
});

test('Netflix Top 10 titles bypass the release-year floor so older charted titles get enriched', async () => {
  process.env.INGESTION_MIN_RELEASE_YEAR = '2010';
  const upserts = []; const completions = [];
  await runNextIngestionJob(normalizeStore('netflix_top10', upserts, completions), { now: () => new Date('2026-11-04T00:00:00Z') });
  assert.equal(upserts.length, 1);
  assert.equal(upserts[0].tmdbId, 8355);
  delete process.env.INGESTION_MIN_RELEASE_YEAR;
});
