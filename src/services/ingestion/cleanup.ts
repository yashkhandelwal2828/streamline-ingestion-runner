type CleanupOptions = {
  historyDaysToKeep: number;
  deleteMoviesBeforeYear?: number;
};

type CleanupPrisma = {
  ingestionSnapshot?: {
    count: (args: any) => Promise<number>;
    deleteMany: (args: any) => Promise<{ count: number }>;
  };
  ingestionJob: {
    count: (args: any) => Promise<number>;
    deleteMany: (args: any) => Promise<{ count: number }>;
  };
  movie?: {
    count: (args: any) => Promise<number>;
    deleteMany: (args: any) => Promise<{ count: number }>;
  };
  $queryRawUnsafe?: <T = unknown>(query: string) => Promise<T>;
};

const INGESTION_JOB_TERMINAL_STATUSES = ['COMPLETED', 'FAILED', 'DEAD_LETTERED'] as const;

const buildHistoryCutoff = (now: Date, historyDaysToKeep: number) => {
  const cutoff = new Date(now);
  cutoff.setUTCDate(cutoff.getUTCDate() - historyDaysToKeep);
  return cutoff;
};

const buildMovieReleaseCutoff = (deleteMoviesBeforeYear?: number) => {
  if (!deleteMoviesBeforeYear) {
    return null;
  }

  return new Date(Date.UTC(deleteMoviesBeforeYear, 0, 1));
};

const getStorageByTable = async (prisma: CleanupPrisma) => {
  if (!prisma.$queryRawUnsafe) {
    return null;
  }

  try {
    const rows = await prisma.$queryRawUnsafe<Array<{ table_name: string; bytes: bigint | number | string }>>(
      `
        SELECT
          c.relname AS table_name,
          pg_total_relation_size(c.oid) AS bytes
        FROM pg_class c
        WHERE c.relkind = 'r'
          AND c.relnamespace = 'public'::regnamespace
          AND c.relname IN ('Movie', 'IngestionSnapshot', 'IngestionJob')
      `,
    );

    return rows.map((row) => {
      const numericValue = Number(row.bytes);
      return {
        table: row.table_name,
        bytes: Number.isFinite(numericValue) ? numericValue : null,
      };
    });
  } catch {
    return null;
  }
};

export const getIngestionCleanupPreview = async (
  prisma: CleanupPrisma,
  options: CleanupOptions,
  now = new Date(),
) => {
  if (!prisma.ingestionSnapshot) {
    throw new Error('Ingestion cleanup preview requires Prisma ingestionSnapshot support');
  }

  if (!prisma.movie) {
    throw new Error('Ingestion cleanup preview requires Prisma movie support');
  }

  const historyCutoff = buildHistoryCutoff(now, options.historyDaysToKeep);
  const movieReleaseCutoff = buildMovieReleaseCutoff(options.deleteMoviesBeforeYear);

  const [snapshotRowsToDelete, jobRowsToDelete, movieRowsToDelete, totalMoviesBefore, storageByTable] =
    await Promise.all([
      prisma.ingestionSnapshot.count({
        where: {
          createdAt: {
            lt: historyCutoff,
          },
        },
      }),
      prisma.ingestionJob.count({
        where: {
          createdAt: {
            lt: historyCutoff,
          },
          status: {
            in: INGESTION_JOB_TERMINAL_STATUSES,
          },
        },
      }),
      movieReleaseCutoff
        ? prisma.movie.count({
            where: {
              releaseDate: {
                lt: movieReleaseCutoff,
              },
            },
          })
        : Promise.resolve(0),
      prisma.movie.count({}),
      getStorageByTable(prisma),
    ]);

  return {
    generatedAt: now,
    options: {
      historyDaysToKeep: options.historyDaysToKeep,
      deleteMoviesBeforeYear: options.deleteMoviesBeforeYear ?? null,
    },
    cutoffs: {
      historyBefore: historyCutoff,
      movieReleaseBefore: movieReleaseCutoff,
    },
    counts: {
      totalMoviesBefore,
      snapshotRowsToDelete,
      jobRowsToDelete,
      movieRowsToDelete,
    },
    storageByTable,
  };
};

export const runIngestionCleanup = async (
  prisma: CleanupPrisma,
  options: CleanupOptions & { dryRun: boolean },
  now = new Date(),
) => {
  if (!prisma.ingestionSnapshot) {
    throw new Error('Ingestion cleanup execution requires Prisma ingestionSnapshot support');
  }

  if (!prisma.movie) {
    throw new Error('Ingestion cleanup execution requires Prisma movie support');
  }

  const preview = await getIngestionCleanupPreview(prisma, options, now);
  if (options.dryRun) {
    return {
      dryRun: true,
      preview,
      deleted: {
        snapshots: 0,
        jobs: 0,
        movies: 0,
      },
    };
  }

  const historyCutoff = buildHistoryCutoff(now, options.historyDaysToKeep);
  const movieReleaseCutoff = buildMovieReleaseCutoff(options.deleteMoviesBeforeYear);

  const deletedSnapshots = await prisma.ingestionSnapshot.deleteMany({
    where: {
      createdAt: {
        lt: historyCutoff,
      },
    },
  });

  const deletedJobs = await prisma.ingestionJob.deleteMany({
    where: {
      createdAt: {
        lt: historyCutoff,
      },
      status: {
        in: INGESTION_JOB_TERMINAL_STATUSES,
      },
    },
  });

  const deletedMovies = movieReleaseCutoff
    ? await prisma.movie.deleteMany({
        where: {
          releaseDate: {
            lt: movieReleaseCutoff,
          },
        },
      })
    : { count: 0 };

  const after = await getIngestionCleanupPreview(prisma, options, new Date());

  return {
    dryRun: false,
    preview,
    deleted: {
      snapshots: deletedSnapshots.count,
      jobs: deletedJobs.count,
      movies: deletedMovies.count,
    },
    after,
  };
};
