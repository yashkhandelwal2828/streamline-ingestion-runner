import { createScriptContext, disconnectPrisma } from './scriptUtils';

/**
 * One-off storage maintenance, run by hand from storage-prune.yml.
 * 1. Reports the Neon size limit and current usage.
 * 2. With PRUNE_APPLY=true, deletes availability rows for countries outside KEEP_REGIONS in small batches
 *    (short locks), then rewrites the bloated tables (VACUUM FULL) so the freed space counts against the limit.
 */
const KEEP_REGIONS = String(process.env.KEEP_REGIONS ?? '').split(',').map((value) => value.trim().toUpperCase()).filter((value) => /^[A-Z]{2}$/.test(value));
const APPLY = process.env.PRUNE_APPLY === 'true';
const BATCH = 20_000;

const main = async () => {
  // One connection, so the session setting below applies to the VACUUM that follows it.
  if (process.env.DATABASE_URL && !/connection_limit=/.test(process.env.DATABASE_URL)) {
    process.env.DATABASE_URL += `${process.env.DATABASE_URL.includes('?') ? '&' : '?'}connection_limit=1`;
  }
  const { prisma } = createScriptContext();
  const q = <T = any>(sql: string, ...params: unknown[]) => prisma.$queryRawUnsafe<T[]>(sql, ...params);
  const mb = (bytes: unknown) => Math.round(Number(bytes) / 1048576);
  const report = async (label: string) => {
    const [{ size }] = await q('SELECT pg_database_size(current_database()) AS size');
    const [limit] = await q("SELECT current_setting('neon.max_cluster_size', true) AS value");
    const tables = await q(`SELECT relname, pg_total_relation_size(relid) AS bytes FROM pg_stat_user_tables
      WHERE relname IN ('PlatformAvailability', 'Movie', 'IngestionJob', 'actor_search') ORDER BY 2 DESC`);
    console.log(JSON.stringify({
      event: 'storage_report', label, databaseMb: mb(size),
      neonMaxClusterSizeMb: limit?.value ?? null,
      tablesMb: Object.fromEntries(tables.map((table: any) => [table.relname, mb(table.bytes)])),
    }));
  };

  try {
    await report('before');
    if (KEEP_REGIONS.length < 5) throw new Error(`KEEP_REGIONS looks wrong: ${KEEP_REGIONS.join(',')}`);
    const placeholders = KEEP_REGIONS.map((_, index) => `$${index + 1}`).join(',');
    const [{ count }] = await q(`SELECT COUNT(*) AS count FROM "PlatformAvailability" WHERE region NOT IN (${placeholders})`, ...KEEP_REGIONS);
    const [{ kept }] = await q(`SELECT COUNT(*) AS kept FROM "PlatformAvailability" WHERE region IN (${placeholders})`, ...KEEP_REGIONS);
    console.log(JSON.stringify({ event: 'prune_plan', keepRegions: KEEP_REGIONS, rowsToDelete: Number(count), rowsKept: Number(kept), apply: APPLY }));
    if (!APPLY) return;

    let deleted = 0;
    for (;;) {
      const removed = await prisma.$executeRawUnsafe(`DELETE FROM "PlatformAvailability" WHERE id IN (
        SELECT id FROM "PlatformAvailability" WHERE region NOT IN (${placeholders}) LIMIT ${BATCH})`, ...KEEP_REGIONS);
      deleted += removed;
      if (removed === 0) break;
      console.log(JSON.stringify({ event: 'prune_progress', deleted }));
    }

    // VACUUM FULL rewrites the table; give up quickly rather than queue behind a busy lock, and retry.
    for (const table of ['PlatformAvailability', 'IngestionJob']) {
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        try {
          await prisma.$executeRawUnsafe("SET lock_timeout = '10s'");
          const started = Date.now();
          await prisma.$executeRawUnsafe(`VACUUM (FULL, ANALYZE) "${table}"`);
          console.log(JSON.stringify({ event: 'vacuum_full_done', table, seconds: Math.round((Date.now() - started) / 1000) }));
          break;
        } catch (error) {
          console.warn(JSON.stringify({ event: 'vacuum_full_retry', table, attempt, error: error instanceof Error ? error.message : String(error) }));
          await new Promise((resolve) => setTimeout(resolve, 15_000));
        }
      }
    }
    await report('after');
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => {
  console.error('[storage-prune] failed:', error);
  process.exit(1);
});
