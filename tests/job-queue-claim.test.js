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

const { createPrismaIngestionStore } = loadTsModule('src/services/ingestion/jobQueue.ts');
const {
  parseIngestionAllowedJobTypes,
  getIngestionDailyTarget,
} = loadTsModule('src/services/ingestion/queueConfig.ts');
const { shouldDeferOmdbEnqueue } = loadTsModule('src/services/ingestion/omdbEnqueue.ts');

test('parseIngestionAllowedJobTypes filters unknown values', () => {
  assert.deepEqual(
    parseIngestionAllowedJobTypes({ INGESTION_ALLOWED_JOB_TYPES: 'FETCH,NORMALIZE,NOT_REAL' }),
    ['FETCH', 'NORMALIZE'],
  );
  assert.deepEqual(parseIngestionAllowedJobTypes({ INGESTION_ALLOWED_JOB_TYPES: '' }), []);
});

test('claimNextJob respects INGESTION_ALLOWED_JOB_TYPES filter', async () => {
  const calls = [];
  const prisma = {
    ingestionJob: {
      findFirst: async (args) => {
        calls.push(args);
        return { id: 'job-1', type: 'FETCH', status: 'QUEUED' };
      },
      findUnique: async () => ({ id: 'job-1', type: 'FETCH', status: 'RUNNING' }),
      updateMany: async () => ({ count: 1 }),
    },
  };

  const store = createPrismaIngestionStore(prisma, { allowedJobTypes: ['FETCH', 'NORMALIZE'] });
  const claimed = await store.claimNextJob(new Date('2026-06-09T00:00:00.000Z'));

  assert.equal(claimed.id, 'job-1');
  assert.deepEqual(calls[0].where.type, { in: ['FETCH', 'NORMALIZE'] });
});

test('claimNextJob without filter does not constrain type', async () => {
  const calls = [];
  const prisma = {
    ingestionJob: {
      findFirst: async (args) => {
        calls.push(args);
        return null;
      },
      findUnique: async () => null,
      updateMany: async () => ({ count: 0 }),
    },
  };

  const store = createPrismaIngestionStore(prisma);
  await store.claimNextJob();

  assert.equal(calls[0].where.type, undefined);
});

test('shouldDeferOmdbEnqueue waits when FETCH backlog is high', async () => {
  const result = await shouldDeferOmdbEnqueue(
    {
      countQueuedJobsByType: async (type) => (type === 'FETCH' ? 150 : 0),
      countMoviesCreatedSince: async () => 500,
    },
    { env: { INGESTION_OMDB_MAX_FETCH_BACKLOG: '100', INGESTION_DAILY_TARGET: '500' } },
  );

  assert.equal(result.deferred, true);
  assert.equal(result.skippedReason, 'fetch_backlog');
});

test('shouldDeferOmdbEnqueue waits when catalog is behind daily target', async () => {
  const result = await shouldDeferOmdbEnqueue(
    {
      countQueuedJobsByType: async () => 0,
      countMoviesCreatedSince: async () => 10,
    },
    {
      now: new Date('2026-06-09T12:00:00.000Z'),
      env: {
        INGESTION_DAILY_TARGET: '500',
        INGESTION_OMDB_MIN_CATALOG_PROGRESS_RATIO: '0.8',
      },
    },
  );

  assert.equal(result.deferred, true);
  assert.equal(result.skippedReason, 'catalog_behind_target');
  assert.equal(result.requiredToday, 400);
});

test('getIngestionDailyTarget defaults to 500', () => {
  assert.equal(getIngestionDailyTarget({}), 500);
});
