import { PrismaClient } from '@prisma/client';
import { buildDeepEnrichJobData } from './jobQueue';
import { getOmdbDailyLimit } from './queueConfig';

type OmdbEnqueueStore = {
  getOmdbUsage: (now?: Date) => Promise<{ regular: number; retries: number; total: number }>;
  countQueuedAndRunningJobs: () => Promise<number>;
  findActiveJobByDedupeKey: (dedupeKey: string) => Promise<{ id: string } | null>;
  createJob: (data: Record<string, unknown>) => Promise<{ id: string }>;
};

export const findMoviesNeedingOmdbEnrichment = async (
  prisma: PrismaClient,
  limit: number,
) => {
  return prisma.movie.findMany({
    where: {
      imdbId: { not: null },
      imdbRating: null,
    },
    orderBy: [
      { releaseDate: 'desc' },
      { updatedAt: 'asc' },
    ],
    take: limit,
    select: {
      id: true,
      tmdbId: true,
      mediaType: true,
      imdbId: true,
    },
  });
};

export const enqueueOmdbBackfill = async (
  prisma: PrismaClient,
  store: OmdbEnqueueStore,
  options: {
    now?: Date;
    logger?: Pick<Console, 'info' | 'error'>;
    maxEnqueue?: number;
  } = {},
) => {
  const now = options.now ?? new Date();
  const logger = options.logger ?? console;
  const usage = await store.getOmdbUsage(now);
  const dailyLimit = getOmdbDailyLimit();
  const remainingBudget = Math.max(0, dailyLimit - usage.regular);

  if (remainingBudget === 0) {
    logger.info(`OMDb daily budget exhausted (${usage.regular}/${dailyLimit}).`);
    return {
      created: 0,
      skipped: 0,
      skippedReason: 'omdb_daily_budget_exhausted' as const,
      usage,
      remainingBudget,
    };
  }

  const maxEnqueue = Math.min(
    options.maxEnqueue ?? remainingBudget,
    remainingBudget,
  );

  const candidates = await findMoviesNeedingOmdbEnrichment(prisma, maxEnqueue);
  let created = 0;
  let skipped = 0;

  for (const movie of candidates) {
    if (!movie.imdbId) {
      skipped += 1;
      continue;
    }

    const mediaType = movie.mediaType === 'tv' ? 'tv' : 'movie';
    const jobData = buildDeepEnrichJobData({
      source: 'omdb_enqueue',
      movieId: movie.id,
      tmdbId: movie.tmdbId,
      mediaType,
      imdbId: movie.imdbId,
      priority: 220,
      createdAt: now,
    });

    const existing = await store.findActiveJobByDedupeKey(jobData.dedupeKey);
    if (existing) {
      skipped += 1;
      continue;
    }

    await store.createJob(jobData);
    created += 1;
  }

  logger.info(
    `OMDb enqueue complete: created=${created}, skipped=${skipped}, remainingBudget=${remainingBudget - created}, usage=${usage.regular}/${dailyLimit}.`,
  );

  return {
    created,
    skipped,
    usage,
    remainingBudget: remainingBudget - created,
    candidates: candidates.length,
  };
};
