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

const {
  runSnapshotCleanup,
  buildScheduledIngestionJobInput,
  buildScheduledBootstrapInput,
  buildScheduledDeltaInput,
  isBootstrapIngestionEnabled,
  getIngestionScheduleConfig,
  getScheduledDailyTarget,
  queueDailyTargetFill,
} = loadTsModule('src/services/cronScheduler.ts');

test('snapshot cleanup deletes expired raw payloads on a schedule tick', async () => {
  const calls = [];
  const deletedCount = 4;

  await runSnapshotCleanup(
    {
      async deleteExpiredSnapshots(now) {
        calls.push(now);
        return deletedCount;
      },
    },
    {
      now: () => new Date('2026-03-29T12:00:00.000Z'),
      logger: {
        info() {},
        error() {
          throw new Error('cleanup success should not log an error');
        },
      },
    },
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].toISOString(), '2026-03-29T12:00:00.000Z');
});

test('scheduled ingestion stays in shadow mode until rollout is promoted', () => {
  assert.deepEqual(buildScheduledIngestionJobInput('shadow'), {
    source: 'schedule',
    pagesToScan: 100,
    dailyTarget: 150,
    mode: 'discover',
    shadowMode: true,
  });

  assert.deepEqual(buildScheduledIngestionJobInput('live'), {
    source: 'schedule',
    pagesToScan: 100,
    dailyTarget: 150,
    mode: 'discover',
    shadowMode: false,
  });
});

test('scheduled ingestion target can be configured from environment', () => {
  const original = process.env.INGESTION_DAILY_TARGET;
  process.env.INGESTION_DAILY_TARGET = '1500';

  try {
    assert.equal(getScheduledDailyTarget(), 1500);
    assert.deepEqual(buildScheduledIngestionJobInput('live'), {
      source: 'schedule',
      pagesToScan: 100,
      dailyTarget: 1500,
      mode: 'discover',
      shadowMode: false,
    });
  } finally {
    if (original === undefined) {
      delete process.env.INGESTION_DAILY_TARGET;
    } else {
      process.env.INGESTION_DAILY_TARGET = original;
    }
  }
});

test('daily target fill queues only the remaining catalog target', async () => {
  const originalTarget = process.env.INGESTION_DAILY_TARGET;
  const originalRollout = process.env.INGESTION_ROLLOUT_MODE;
  process.env.INGESTION_DAILY_TARGET = '1000';
  process.env.INGESTION_ROLLOUT_MODE = 'live';

  const calls = [];
  const createdJob = {
    id: 'fill_job_1',
    dedupeKey: 'catalog_refresh:IN:100:750',
    status: 'QUEUED',
  };
  const store = {
    async countMoviesCreatedSince(since) {
      calls.push(['count', since.toISOString()]);
      return 250;
    },
    async findActiveCatalogIngestionJob() {
      calls.push(['active']);
      return null;
    },
    async findActiveJobByDedupeKey(dedupeKey) {
      calls.push(['find', dedupeKey]);
      return null;
    },
    async createJob(data) {
      calls.push(['create', data.source, data.payload.dailyTarget, data.shadowMode]);
      return createdJob;
    },
  };

  try {
    const result = await queueDailyTargetFill(store, {
      now: () => new Date('2026-05-12T17:30:00.000Z'),
      logger: { info() {}, error() {} },
    });

    assert.equal(result.created, true);
    assert.equal(result.job, createdJob);
    assert.equal(result.createdToday, 250);
    assert.equal(result.remainingTarget, 750);
    assert.deepEqual(calls, [
      ['count', '2026-05-12T00:00:00.000Z'],
      ['active'],
      ['find', 'catalog_refresh:IN:100:750'],
      ['create', 'daily_target_fill', 750, false],
    ]);
  } finally {
    if (originalTarget === undefined) delete process.env.INGESTION_DAILY_TARGET;
    else process.env.INGESTION_DAILY_TARGET = originalTarget;

    if (originalRollout === undefined) delete process.env.INGESTION_ROLLOUT_MODE;
    else process.env.INGESTION_ROLLOUT_MODE = originalRollout;
  }
});

test('daily target fill skips when target is already met', async () => {
  const originalTarget = process.env.INGESTION_DAILY_TARGET;
  process.env.INGESTION_DAILY_TARGET = '1000';

  const store = {
    async countMoviesCreatedSince() {
      return 1000;
    },
    async findActiveCatalogIngestionJob() {
      throw new Error('active job lookup should not run once target is met');
    },
    async findActiveJobByDedupeKey() {
      throw new Error('enqueue should not run once target is met');
    },
    async createJob() {
      throw new Error('createJob should not run once target is met');
    },
  };

  try {
    const result = await queueDailyTargetFill(store, {
      now: () => new Date('2026-05-12T17:30:00.000Z'),
      logger: { info() {}, error() {} },
    });

    assert.deepEqual(result, {
      created: false,
      skippedReason: 'daily_target_met',
      dailyTarget: 1000,
      createdToday: 1000,
      remainingTarget: 0,
    });
  } finally {
    if (originalTarget === undefined) delete process.env.INGESTION_DAILY_TARGET;
    else process.env.INGESTION_DAILY_TARGET = originalTarget;
  }
});

test('daily target fill waits when a catalog job is already active', async () => {
  const originalTarget = process.env.INGESTION_DAILY_TARGET;
  process.env.INGESTION_DAILY_TARGET = '1000';
  const activeJob = {
    id: 'active_job',
    status: 'RUNNING',
  };

  const store = {
    async countMoviesCreatedSince() {
      return 300;
    },
    async findActiveCatalogIngestionJob() {
      return activeJob;
    },
    async findActiveJobByDedupeKey() {
      throw new Error('enqueue should not run while a catalog job is active');
    },
    async createJob() {
      throw new Error('createJob should not run while a catalog job is active');
    },
  };

  try {
    const result = await queueDailyTargetFill(store, {
      now: () => new Date('2026-05-12T17:30:00.000Z'),
      logger: { info() {}, error() {} },
    });

    assert.equal(result.created, false);
    assert.equal(result.skippedReason, 'active_catalog_job');
    assert.equal(result.dailyTarget, 1000);
    assert.equal(result.createdToday, 300);
    assert.equal(result.remainingTarget, 700);
    assert.equal(result.job, activeJob);
  } finally {
    if (originalTarget === undefined) delete process.env.INGESTION_DAILY_TARGET;
    else process.env.INGESTION_DAILY_TARGET = originalTarget;
  }
});

test('bootstrap and delta schedules default to free-plan-friendly freshness settings', () => {
  assert.deepEqual(buildScheduledBootstrapInput('live'), {
    source: 'bootstrap',
    pagesToScan: 1,
    dailyTarget: 500,
    mode: 'bootstrap_export',
    targetCatalogSize: 50000,
    batchSize: 50,
    scanWindow: 4000,
    maxScanPerRun: 40000,
    shadowMode: false,
  });

  assert.deepEqual(buildScheduledDeltaInput('live'), {
    source: 'delta',
    pagesToScan: 2,
    targetCount: 30,
    windowHours: 48,
    priority: 240,
    shadowMode: false,
  });
});

test('ingestion cron schedules default to spaced-out compute usage', () => {
  assert.deepEqual(getIngestionScheduleConfig({}), {
    bootstrap: '0 */6 * * *',
    delta: '20 */6 * * *',
    dailyFill: '10 */6 * * *',
    cleanup: '30 2 * * *',
  });
});

test('ingestion cron schedules can be overridden with valid cron expressions', () => {
  assert.deepEqual(getIngestionScheduleConfig({
    INGESTION_BOOTSTRAP_CRON: '0 1 * * *',
    INGESTION_DELTA_CRON: '15 */4 * * *',
    INGESTION_DAILY_FILL_CRON: '30 */8 * * *',
    INGESTION_CLEANUP_CRON: '45 3 * * *',
  }), {
    bootstrap: '0 1 * * *',
    delta: '15 */4 * * *',
    dailyFill: '30 */8 * * *',
    cleanup: '45 3 * * *',
  });
});

test('invalid ingestion cron overrides fall back to defaults', () => {
  assert.deepEqual(getIngestionScheduleConfig({
    INGESTION_BOOTSTRAP_CRON: 'not cron',
    INGESTION_DELTA_CRON: '',
    INGESTION_DAILY_FILL_CRON: '10 */6 * * *',
    INGESTION_CLEANUP_CRON: 'also bad',
  }), {
    bootstrap: '0 */6 * * *',
    delta: '20 */6 * * *',
    dailyFill: '10 */6 * * *',
    cleanup: '30 2 * * *',
  });
});

test('bootstrap schedule honors environment overrides for throughput and catalog target', () => {
  const original = {
    INGESTION_BOOTSTRAP_DAILY_TARGET: process.env.INGESTION_BOOTSTRAP_DAILY_TARGET,
    INGESTION_BOOTSTRAP_TARGET_CATALOG_SIZE: process.env.INGESTION_BOOTSTRAP_TARGET_CATALOG_SIZE,
    INGESTION_BOOTSTRAP_BATCH_SIZE: process.env.INGESTION_BOOTSTRAP_BATCH_SIZE,
    INGESTION_BOOTSTRAP_SCAN_WINDOW: process.env.INGESTION_BOOTSTRAP_SCAN_WINDOW,
    INGESTION_BOOTSTRAP_MAX_SCAN_PER_RUN: process.env.INGESTION_BOOTSTRAP_MAX_SCAN_PER_RUN,
  };

  process.env.INGESTION_BOOTSTRAP_DAILY_TARGET = '5000';
  process.env.INGESTION_BOOTSTRAP_TARGET_CATALOG_SIZE = '70000';
  process.env.INGESTION_BOOTSTRAP_BATCH_SIZE = '80';
  process.env.INGESTION_BOOTSTRAP_SCAN_WINDOW = '8000';
  process.env.INGESTION_BOOTSTRAP_MAX_SCAN_PER_RUN = '60000';

  try {
    assert.deepEqual(buildScheduledBootstrapInput('live'), {
      source: 'bootstrap',
      pagesToScan: 1,
      dailyTarget: 5000,
      mode: 'bootstrap_export',
      targetCatalogSize: 70000,
      batchSize: 80,
      scanWindow: 8000,
      maxScanPerRun: 60000,
      shadowMode: false,
    });
  } finally {
    for (const [key, value] of Object.entries(original)) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
});

test('bootstrap lane is disabled by default unless explicitly enabled', () => {
  assert.equal(isBootstrapIngestionEnabled({}), false);
  assert.equal(isBootstrapIngestionEnabled({ INGESTION_BOOTSTRAP_ENABLED: 'false' }), false);
  assert.equal(isBootstrapIngestionEnabled({ INGESTION_BOOTSTRAP_ENABLED: '0' }), false);
  assert.equal(isBootstrapIngestionEnabled({ INGESTION_BOOTSTRAP_ENABLED: 'true' }), true);
  assert.equal(isBootstrapIngestionEnabled({ INGESTION_BOOTSTRAP_ENABLED: '1' }), true);
});
