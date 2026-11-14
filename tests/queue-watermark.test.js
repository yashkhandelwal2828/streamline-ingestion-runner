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
  INGESTION_DISCOVERY_LANE_JOB_TYPES,
  isQueueAboveHighWatermark,
} = loadTsModule('src/services/ingestion/queueConfig.ts');
const { skipIfQueueAboveWatermark } = loadTsModule('src/scripts/scriptUtils.ts');

const ACTIVE = ['QUEUED', 'RUNNING'];

test('discovery lane excludes DEEP_ENRICH', () => {
  assert.ok(INGESTION_DISCOVERY_LANE_JOB_TYPES.includes('FETCH'));
  assert.ok(INGESTION_DISCOVERY_LANE_JOB_TYPES.includes('DISCOVER'));
  assert.equal(INGESTION_DISCOVERY_LANE_JOB_TYPES.includes('DEEP_ENRICH'), false);
});

test('countQueuedAndRunningJobs filters by per-call job types', async () => {
  const calls = [];
  const store = createPrismaIngestionStore({
    ingestionJob: {
      findFirst: async () => null,
      create: async (args) => args.data,
      count: async (args) => {
        calls.push(args);
        return 7;
      },
    },
  });

  await store.countQueuedAndRunningJobs({ allowedJobTypes: INGESTION_DISCOVERY_LANE_JOB_TYPES });

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].where.status.in, ACTIVE);
  assert.deepEqual(calls[0].where.type.in, [...INGESTION_DISCOVERY_LANE_JOB_TYPES]);
});

test('countQueuedAndRunningJobs falls back to store-level allowed types', async () => {
  const calls = [];
  const store = createPrismaIngestionStore(
    {
      ingestionJob: {
        findFirst: async () => null,
        create: async (args) => args.data,
        count: async (args) => {
          calls.push(args);
          return 3;
        },
      },
    },
    { allowedJobTypes: ['FETCH'] },
  );

  await store.countQueuedAndRunningJobs();

  assert.deepEqual(calls[0].where.type.in, ['FETCH']);
});

test('watermark ignores a deep DEEP_ENRICH backlog for discovery enqueues', async () => {
  process.env.INGESTION_QUEUE_HIGH_WATERMARK = '10000';
  const store = {
    countQueuedAndRunningJobs: async (options = {}) =>
      options.allowedJobTypes ? 0 : 10_462,
  };

  assert.equal(isQueueAboveHighWatermark(10_462), true);

  const discovery = await skipIfQueueAboveWatermark(store, 'ingestion:enqueue-delta', {
    allowedJobTypes: INGESTION_DISCOVERY_LANE_JOB_TYPES,
  });
  assert.equal(discovery.skipped, false);
  assert.equal(discovery.depth, 0);

  const enrichment = await skipIfQueueAboveWatermark(store, 'ingestion:enqueue-omdb');
  assert.equal(enrichment.skipped, true);
  assert.equal(enrichment.depth, 10_462);
});
