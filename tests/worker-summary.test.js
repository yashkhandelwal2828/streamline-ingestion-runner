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
  createWorkerStats,
  recordWorkerTickResult,
  buildWorkerSummary,
} = loadTsModule('src/services/ingestion/pipeline.ts');
const { decideChainHop } = loadTsModule('src/scripts/ingestionWorkerChainDecision.ts');

test('worker stats count empty ticks separately from processed jobs', () => {
  const stats = createWorkerStats(Date.parse('2026-10-20T10:00:00.000Z'));

  recordWorkerTickResult(stats, { processed: false });
  recordWorkerTickResult(stats, { processed: false });
  recordWorkerTickResult(stats, { processed: true, jobId: 'job_1', type: 'FETCH' });

  assert.equal(stats.attempts, 3);
  assert.equal(stats.emptyPolls, 2);
  assert.equal(stats.processed, 1);
  assert.equal(stats.failed, 0);
  assert.deepEqual(stats.byType, { FETCH: { processed: 1, failed: 0 } });
});

test('worker stats track failures, dead letters and the last error per job type', () => {
  const stats = createWorkerStats();

  recordWorkerTickResult(stats, { processed: true, jobId: 'job_a', type: 'FETCH' });
  recordWorkerTickResult(stats, {
    processed: true,
    jobId: 'job_b',
    type: 'NORMALIZE',
    failed: true,
    attempts: 2,
    errorMessage: 'TMDB 503',
  });
  recordWorkerTickResult(stats, {
    processed: true,
    jobId: 'job_c',
    type: 'FETCH',
    failed: true,
    deadLettered: true,
    attempts: 5,
    errorMessage: 'TMDB 404',
  });

  assert.equal(stats.processed, 3);
  assert.equal(stats.failed, 2);
  assert.equal(stats.deadLettered, 1);
  assert.equal(stats.lastError, 'TMDB 404');
  assert.deepEqual(stats.byType, {
    FETCH: { processed: 2, failed: 1 },
    NORMALIZE: { processed: 1, failed: 1 },
  });
});

test('worker summary reports throughput and backlog', () => {
  const startedAtMs = Date.parse('2026-10-20T10:00:00.000Z');
  const stats = createWorkerStats(startedAtMs);

  for (let i = 0; i < 120; i += 1) {
    recordWorkerTickResult(stats, { processed: true, jobId: `job_${i}`, type: 'FETCH' });
  }

  const summary = buildWorkerSummary(stats, 'burst_complete', 42, startedAtMs + 120_000);

  assert.equal(summary.event, 'ingestion_worker_summary');
  assert.equal(summary.reason, 'burst_complete');
  assert.equal(summary.uptimeSeconds, 120);
  assert.equal(summary.processed, 120);
  assert.equal(summary.jobsPerMinute, 60);
  assert.equal(summary.backlog, 42);
  assert.equal(summary.byType.FETCH.processed, 120);
});

test('chain decision continues while backlog remains and hops are left', () => {
  assert.deepEqual(decideChainHop({ processed: 80, failed: 1, backlog: 120 }, { depth: 2, maxHops: 24 }), {
    shouldChain: true,
    depth: 2,
    nextDepth: 3,
    maxHops: 24,
    backlog: 120,
    processed: 80,
    failed: 1,
  });
});

test('chain decision stops when the queue is drained, hops run out, or no summary exists', () => {
  assert.equal(decideChainHop({ processed: 80, backlog: 0 }, { depth: 0, maxHops: 24 }).shouldChain, false);
  assert.equal(decideChainHop({ processed: 80, backlog: 5 }, { depth: 24, maxHops: 24 }).shouldChain, false);
  assert.equal(decideChainHop(null, { depth: 0, maxHops: 24 }).shouldChain, false);
});

test('chain decision falls back to processed count when backlog is unavailable', () => {
  assert.equal(decideChainHop({ processed: 3, backlog: null }, { depth: 0, maxHops: 1 }).shouldChain, true);
  assert.equal(decideChainHop({ processed: 0, backlog: null }, { depth: 0, maxHops: 1 }).shouldChain, false);
});
