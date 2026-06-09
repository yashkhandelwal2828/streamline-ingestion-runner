import dotenv from 'dotenv';
import { PrismaClient } from '@prisma/client';
import { startCronScheduler } from './services/cronScheduler';
import { startIngestionWorker } from './services/ingestion/pipeline';
import { parseIngestionAllowedJobTypes } from './services/ingestion/queueConfig';
import { describeIngestionRolloutMode, getIngestionRolloutMode } from './services/ingestion/rollout';

dotenv.config();

const prisma = new PrismaClient();
const pollIntervalMs = Number.parseInt(String(process.env.INGESTION_POLL_INTERVAL_MS ?? 300000), 10);
const maxJobsPerTick = Number.parseInt(String(process.env.INGESTION_MAX_JOBS_PER_TICK ?? 1), 10);
const staleRecoveryMinutes = Number.parseInt(String(process.env.INGESTION_STALE_RECOVERY_MINUTES ?? 15), 10);
const staleRecoveryIntervalMs = Number.parseInt(String(process.env.INGESTION_STALE_RECOVERY_INTERVAL_MS ?? 1800000), 10);
const allowedJobTypes = parseIngestionAllowedJobTypes();

const shouldRunCronScheduler = () => {
  const value = String(process.env.RUN_CRON_SCHEDULER ?? 'true').trim().toLowerCase();
  return value !== 'false' && value !== '0' && value !== 'no' && value !== 'off';
};

console.log(`Ingestion worker process starting (${describeIngestionRolloutMode(getIngestionRolloutMode())}).`);
if (shouldRunCronScheduler()) {
  startCronScheduler(prisma);
} else {
  console.log('Cron scheduler disabled (RUN_CRON_SCHEDULER=false). Queue processing only.');
}
if (allowedJobTypes.length > 0) {
  console.log(`Ingestion worker job-type filter: ${allowedJobTypes.join(', ')}`);
}
const loop = startIngestionWorker(prisma, {
  pollIntervalMs: Number.isFinite(pollIntervalMs) && pollIntervalMs > 0 ? pollIntervalMs : 300000,
  maxJobsPerTick: Number.isFinite(maxJobsPerTick) && maxJobsPerTick > 0 ? maxJobsPerTick : 1,
  staleRecoveryMinutes:
    Number.isFinite(staleRecoveryMinutes) && staleRecoveryMinutes > 0 ? staleRecoveryMinutes : 15,
  staleRecoveryIntervalMs:
    Number.isFinite(staleRecoveryIntervalMs) && staleRecoveryIntervalMs > 0 ? staleRecoveryIntervalMs : 1800000,
  allowedJobTypes,
});

const shutdown = async (signal: string) => {
  console.log(`Received ${signal}. Stopping ingestion worker...`);
  loop.stop();
  await prisma.$disconnect();
  process.exit(0);
};

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});
