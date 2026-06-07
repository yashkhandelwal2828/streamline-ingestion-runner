import dotenv from 'dotenv';
import { PrismaClient } from '@prisma/client';
import { createPrismaIngestionStore } from '../services/ingestion/jobQueue';
import {
  getIngestionDailyTarget,
  getIngestionQueueHighWatermark,
  getIngestionQueueRefillTarget,
  getOmdbDailyLimit,
  getTmdbRequestsPerSecond,
  isQueueAboveHighWatermark,
} from '../services/ingestion/queueConfig';
import { getIngestionLanguageWeights } from '../services/ingestion/languageConfig';

dotenv.config();

export const createScriptContext = () => {
  const prisma = new PrismaClient();
  const store = createPrismaIngestionStore(prisma);
  return { prisma, store };
};

export const logRunnerConfig = (scriptName: string) => {
  const weights = getIngestionLanguageWeights();
  console.log(`[${scriptName}] starting at ${new Date().toISOString()}`);
  console.log(`[${scriptName}] config:`, {
    dailyTarget: getIngestionDailyTarget(),
    queueHighWatermark: getIngestionQueueHighWatermark(),
    queueRefillTarget: getIngestionQueueRefillTarget(),
    tmdbRequestsPerSecond: getTmdbRequestsPerSecond(),
    omdbDailyLimit: getOmdbDailyLimit(),
    minReleaseYear: process.env.INGESTION_MIN_RELEASE_YEAR ?? null,
    languageWeights: weights.map((entry) => `${entry.code}:${entry.weight}`).join(','),
    fallbackToGlobal: process.env.INGESTION_FALLBACK_TO_GLOBAL ?? 'true',
    runCronScheduler: process.env.RUN_CRON_SCHEDULER ?? 'false',
  });
};

export const getQueueDepth = async (store: ReturnType<typeof createPrismaIngestionStore>) => {
  return store.countQueuedAndRunningJobs();
};

export const logQueueDepth = async (
  label: string,
  store: ReturnType<typeof createPrismaIngestionStore>,
) => {
  const depth = await getQueueDepth(store);
  console.log(`[queue] ${label}: ${depth} queued/running`);
  return depth;
};

export const skipIfQueueAboveWatermark = async (
  store: ReturnType<typeof createPrismaIngestionStore>,
  scriptName: string,
) => {
  const depth = await getQueueDepth(store);
  if (isQueueAboveHighWatermark(depth)) {
    console.log(
      `[${scriptName}] queue backlog ${depth} is at/above high watermark ${getIngestionQueueHighWatermark()}; skipping enqueue.`,
    );
    return { skipped: true as const, depth };
  }

  return { skipped: false as const, depth };
};

export const disconnectPrisma = async (prisma: PrismaClient) => {
  await prisma.$disconnect();
};
