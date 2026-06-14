const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const loadTsModule = (relativePath) => {
  const filename = path.join(__dirname, '..', relativePath);
  const source = fs.readFileSync(filename, 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true,
    },
    fileName: filename,
  });

  const module = { exports: {} };
  const localRequire = (request) => {
    if (request.startsWith('.')) {
      const resolved = path.join(path.dirname(relativePath), request);
      return loadTsModule(path.extname(resolved) ? resolved : `${resolved}.ts`);
    }

    return require(request);
  };

  const factory = new Function('module', 'exports', 'require', outputText);
  factory(module, module.exports, localRequire);
  return module.exports;
};

const { runNextIngestionJob } = loadTsModule('src/services/ingestion/pipeline.ts');

test('normalize job persists tagline from TMDB details', async () => {
  const upserts = [];

  const store = {
    async claimNextJob() {
      return {
        id: 'job_normalize_tagline',
        type: 'NORMALIZE',
        status: 'RUNNING',
        source: 'schedule',
        region: 'IN',
        priority: 250,
        attempts: 0,
        maxAttempts: 3,
        shadowMode: false,
        payload: {
          snapshotId: 'snapshot_tagline',
          tmdbId: 202,
          mediaType: 'movie',
        },
      };
    },
    async getSnapshotById(snapshotId) {
      return {
        id: snapshotId,
        payload: {
          id: 202,
          title: 'Tagline Test',
          imdb_id: 'tt0202',
          overview: 'Overview',
          tagline: 'One line hook.',
          genres: [],
          credits: { crew: [], cast: [] },
          'watch/providers': { results: {} },
        },
      };
    },
    async upsertMovie(movie) {
      upserts.push(movie);
      return { id: 77, ...movie };
    },
    async replacePlatformAvailability() {},
    async enqueueJob() {
      return { id: 'job_deep_1', status: 'QUEUED', dedupeKey: 'deep_enrich:movie:77' };
    },
    async completeJob() {},
    async rescheduleJob() {
      throw new Error('normalize success should not reschedule');
    },
    async deadLetterJob() {
      throw new Error('normalize success should not dead-letter');
    },
  };

  const result = await runNextIngestionJob(store, { now: () => new Date('2026-03-29T12:00:00.000Z') });

  assert.equal(result.processed, true);
  assert.equal(upserts.length, 1);
  assert.equal(upserts[0].tagline, 'One line hook.');
});

test('deep enrich persists plotFull from OMDb Plot', async () => {
  const updates = [];

  const store = {
    async claimNextJob() {
      return {
        id: 'job_enrich_plot',
        type: 'DEEP_ENRICH',
        status: 'RUNNING',
        source: 'omdb_enqueue',
        region: 'IN',
        priority: 220,
        attempts: 0,
        maxAttempts: 3,
        shadowMode: false,
        payload: {
          movieId: 1001,
          tmdbId: 1001,
          mediaType: 'movie',
          imdbId: 'tt1001',
        },
      };
    },
    async updateMovieById(movieId, data) {
      updates.push({ movieId, data });
    },
    async completeJob() {},
    async rescheduleJob() {
      throw new Error('deep enrich success should not reschedule');
    },
    async deadLetterJob() {
      throw new Error('deep enrich success should not dead-letter');
    },
  };

  const deps = {
    now: () => new Date('2026-03-29T12:00:00.000Z'),
    getRatings: async () => ({
      imdbRating: '8.1',
      Ratings: [{ Source: 'Rotten Tomatoes', Value: '91%' }],
      Metascore: '72',
      imdbVotes: '150,000',
      Plot: 'A detective unravels a conspiracy that spans decades and tests every alliance he has ever made.',
    }),
  };

  const result = await runNextIngestionJob(store, deps);

  assert.equal(result.processed, true);
  assert.equal(updates.length, 1);
  assert.equal(
    updates[0].data.plotFull,
    'A detective unravels a conspiracy that spans decades and tests every alliance he has ever made.',
  );
});

test('findMoviesNeedingOmdbEnrichment includes rated titles missing plotFull', async () => {
  const { findMoviesNeedingOmdbEnrichment } = loadTsModule('src/services/ingestion/omdbEnqueue.ts');
  const calls = [];

  const prisma = {
    movie: {
      async findMany(args) {
        calls.push(args);
        return [{ id: 1, tmdbId: 42, mediaType: 'movie', imdbId: 'tt0042' }];
      },
    },
  };

  const result = await findMoviesNeedingOmdbEnrichment(prisma, 5);

  assert.equal(result.length, 1);
  assert.deepEqual(calls[0].where, {
    imdbId: { not: null },
    OR: [{ imdbRating: null }, { plotFull: null }],
  });
  assert.deepEqual(calls[0].orderBy, [
    { imdbRating: 'asc' },
    { releaseDate: 'desc' },
    { updatedAt: 'asc' },
  ]);
  assert.equal(calls[0].take, 5);
});
