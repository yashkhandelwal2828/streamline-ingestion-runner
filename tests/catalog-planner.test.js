const test = require('node:test');
const assert = require('node:assert/strict');

const { runBudget, laneTargets, scanExport, runCatalogPlanner, readPlannerConfig } = require('../dist/services/ingestion/catalogPlanner.js');
const { filterAvailabilityRegions } = require('../dist/services/ingestion/pipeline.js');

const config = { ...readPlannerConfig({}), maxDiscoverPages: 3 };

test('runBudget spreads what is left over the remaining runs', () => {
  assert.equal(runBudget(config, 0, new Date('2026-11-09T00:30:00Z')), 1000);
  assert.equal(runBudget(config, 3000, new Date('2026-11-09T18:30:00Z')), 1000);
  assert.equal(runBudget(config, 4000, new Date('2026-11-09T12:00:00Z')), 0);
  assert.equal(runBudget(config, 5000, new Date('2026-11-09T12:00:00Z')), 0);
});

test('laneTargets uses the configured shares', () => {
  assert.deepEqual(laneTargets(1000, { new: 0.4, repair: 0.15 }), { new: 400, repair: 150 });
});

test('scanExport resumes from the checkpoint, skips known ids and wraps', async () => {
  const entries = [1, 2, 3, 4, 5, 6].map((id) => ({ id, popularity: 10 - id }));
  const known = new Set([1, 2, 5]);
  const findMissing = async (ids) => ids.filter((id) => !known.has(id));
  const first = await scanExport(entries, 0, 2, 100, findMissing, 2);
  assert.deepEqual(first.picked, [3, 4]);
  const second = await scanExport(entries, first.nextOffset, 5, 100, findMissing, 2);
  assert.deepEqual(second.picked, [6, 3, 4], 'wraps to the start once and stops after one full lap');
});

const fakePrisma = ({ dbMb = 300, existing = new Set(), stubs = [], fetchesToday = 0 } = {}) => {
  const snapshots = [];
  return {
    snapshots,
    ingestionJob: { count: async () => fetchesToday },
    movie: { findMany: async ({ where }) => where.tmdbId.in.filter((id) => existing.has(`${where.mediaType}:${id}`)).map((tmdbId) => ({ tmdbId })) },
    $queryRawUnsafe: async (sql, limit) => sql.includes('pg_database_size') ? [{ bytes: BigInt(dbMb * 1048576) }] : stubs.slice(0, limit),
    ingestionSnapshot: {
      findFirst: async () => snapshots.at(-1) ?? null,
      deleteMany: async () => { snapshots.length = 0; },
      create: async ({ data }) => { snapshots.push(data); },
    },
  };
};

const deps = (prisma, jobs) => ({
  prisma,
  now: () => new Date('2026-11-09T00:10:00Z'),
  log: () => {},
  enqueue: async (job) => { jobs.push(job); },
  discover: async (mediaType, page, { language }) => (language || page > 1 ? [] : [{ id: 100, poster_path: '/a.jpg' }, { id: 101, poster_path: null }]),
  exportEntries: async (mediaType) => ({ dateKey: 'x', entries: [{ id: 7, popularity: 9 }, { id: 8, popularity: 5 }] }),
});

test('planner fills new, repair and backfill lanes in priority order', async () => {
  const jobs = [];
  const prisma = fakePrisma({ existing: new Set(['movie:8']), stubs: [{ tmdbId: 55, mediaType: 'tv' }] });
  const summary = await runCatalogPlanner(deps(prisma, jobs), { ...config, dailyBudget: 40 });
  assert.equal(summary.budget, 10);
  const sources = jobs.map((job) => `${job.source}:${job.payload.mediaType}:${job.payload.tmdbId}`);
  assert.ok(sources.includes('catalog_new:movie:100'), 'new titles with posters are queued');
  assert.ok(!sources.some((value) => value.endsWith(':101')), 'posterless discover results are skipped');
  assert.ok(sources.includes('catalog_repair:tv:55'));
  assert.ok(sources.includes('catalog_backfill:movie:7'));
  assert.ok(!sources.includes('catalog_backfill:movie:8'), 'existing titles are never queued again');
  assert.ok(jobs.every((job) => job.type === 'FETCH'));
  assert.equal(prisma.snapshots.length, 1, 'checkpoint saved');
});

test('planner stops adding titles near the storage cap but keeps repairing', async () => {
  const jobs = [];
  const prisma = fakePrisma({ dbMb: 460, stubs: [{ tmdbId: 55, mediaType: 'movie' }] });
  const summary = await runCatalogPlanner(deps(prisma, jobs), { ...config, dailyBudget: 40 });
  assert.equal(summary.storageFull, true);
  assert.deepEqual(jobs.map((job) => job.source), ['catalog_repair']);
});

test('planner does nothing once the daily budget is used', async () => {
  const jobs = [];
  const summary = await runCatalogPlanner(deps(fakePrisma({ fetchesToday: 4000 }), jobs), config);
  assert.equal(summary.skippedReason, 'daily_budget_used');
  assert.equal(jobs.length, 0);
});

test('filterAvailabilityRegions keeps only configured countries', () => {
  assert.deepEqual(filterAvailabilityRegions(['IN', 'US', 'PA'], { INGESTION_AVAILABILITY_REGIONS: 'in,us' }), ['IN', 'US']);
  assert.deepEqual(filterAvailabilityRegions(['IN', 'PA'], {}), ['IN', 'PA']);
});

test('parseSizeSetting reads Neon size settings', () => {
  const { parseSizeSetting } = require('../dist/services/ingestion/catalogPlanner.js');
  assert.equal(parseSizeSetting('1GB'), 1024 ** 3);
  assert.equal(parseSizeSetting('512MB'), 512 * 1024 ** 2);
  assert.equal(parseSizeSetting('300'), 300 * 1024 ** 2);
  assert.equal(parseSizeSetting(null), null);
  assert.equal(parseSizeSetting('unlimited'), null);
});
