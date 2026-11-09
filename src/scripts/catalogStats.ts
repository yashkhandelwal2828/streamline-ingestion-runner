import { createScriptContext, disconnectPrisma } from './scriptUtils';

/** Read-only snapshot of catalog shape and database size. Safe to run any time. */
const main = async () => {
  const { prisma } = createScriptContext();
  try {
    const q = async (sql: string) => prisma.$queryRawUnsafe<any[]>(sql);
    const toJson = (rows: any[]) => JSON.parse(JSON.stringify(rows, (_k, v) => (typeof v === 'bigint' ? Number(v) : v)));
    const report = {
      databaseBytes: toJson(await q(`SELECT pg_database_size(current_database()) AS bytes`)),
      tables: toJson(await q(`SELECT relname AS table, pg_total_relation_size(relid) AS bytes, n_live_tup AS rows
        FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 12`)),
      byMediaType: toJson(await q(`SELECT "mediaType", COUNT(*) AS n FROM "Movie" GROUP BY 1`)),
      byDecade: toJson(await q(`SELECT COALESCE((EXTRACT(YEAR FROM "releaseDate")::int / 10) * 10, -1) AS decade, COUNT(*) AS n
        FROM "Movie" GROUP BY 1 ORDER BY 1`)),
      createdLast7Days: toJson(await q(`SELECT DATE("createdAt") AS day, COUNT(*) AS n FROM "Movie"
        WHERE "createdAt" > NOW() - INTERVAL '7 days' GROUP BY 1 ORDER BY 1`)),
      missingFields: toJson(await q(`SELECT
        COUNT(*) FILTER (WHERE "posterPath" IS NULL OR "posterPath" = '') AS no_poster,
        COUNT(*) FILTER (WHERE "imdbRating" IS NULL) AS no_imdb_rating,
        COUNT(*) FILTER (WHERE "releaseDate" IS NULL) AS no_release_date,
        COUNT(*) FILTER (WHERE cardinality(genres) = 0) AS no_genres,
        COUNT(*) AS total FROM "Movie"`)),
      availabilityRegions: toJson(await q(`SELECT region, COUNT(DISTINCT "movieId") AS titles FROM "PlatformAvailability"
        GROUP BY 1 ORDER BY 2 DESC LIMIT 12`)),
      queue: toJson(await q(`SELECT type, status, COUNT(*) AS n FROM "IngestionJob" GROUP BY 1, 2 ORDER BY 1, 2`)),
    };
    console.log(JSON.stringify({ event: 'catalog_stats', ...report }));
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => {
  console.error('[catalog-stats] failed:', error);
  process.exit(1);
});
