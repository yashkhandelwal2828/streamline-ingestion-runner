import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';
import { PrismaClient } from '@prisma/client';
import { startCronScheduler } from './services/cronScheduler';
import { startIngestionWorker } from './services/ingestion/pipeline';
import type { IngestionWorkerSummary } from './services/ingestion/pipeline';
import { parseIngestionAllowedJobTypes } from './services/ingestion/queueConfig';
import { describeIngestionRolloutMode, getIngestionRolloutMode } from './services/ingestion/rollout';

dotenv.config();

const prisma = new PrismaClient();
const pollIntervalMs = Number.parseInt(String(process.env.INGESTION_POLL_INTERVAL_MS ?? 300000), 10);
const maxJobsPerTick = Number.parseInt(String(process.env.INGESTION_MAX_JOBS_PER_TICK ?? 1), 10);
const staleRecoveryMinutes = Number.parseInt(String(process.env.INGESTION_STALE_RECOVERY_MINUTES ?? 15), 10);
const staleRecoveryIntervalMs = Number.parseInt(String(process.env.INGESTION_STALE_RECOVERY_INTERVAL_MS ?? 1800000), 10);
const summaryIntervalMs = Number.parseInt(String(process.env.INGESTION_WORKER_SUMMARY_INTERVAL_MS ?? 60000), 10);
const burstSeconds = Number.parseInt(String(process.env.INGESTION_WORKER_BURST_SECONDS ?? ''), 10);
const summaryFile = process.env.INGESTION_WORKER_SUMMARY_FILE?.trim();
const allowedJobTypes = parseIngestionAllowedJobTypes();

const shouldRunCronScheduler = () => {
  const value = String(process.env.RUN_CRON_SCHEDULER ?? 'true').trim().toLowerCase();
  return value !== 'false' && value !== '0' && value !== 'no' && value !== 'off';
};

const writeSummaryFile = (summary: IngestionWorkerSummary) => {
  if (!summaryFile) {
    return;
  }

  try {
    const target = path.resolve(summaryFile);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, `${JSON.stringify(summary)}\n`, 'utf8');
    console.log(`Worker summary written to ${target}.`);
  } catch (error) {
    console.error('Failed to write worker summary file:', error);
  }
};

const effectivePollIntervalMs = Number.isFinite(pollIntervalMs) && pollIntervalMs > 0 ? pollIntervalMs : 300000;
const effectiveMaxJobsPerTick = Number.isFinite(maxJobsPerTick) && maxJobsPerTick > 0 ? maxJobsPerTick : 1;

console.log(`Ingestion worker process starting (${describeIngestionRolloutMode(getIngestionRolloutMode())}).`);
console.log(
  `Worker config: maxJobsPerTick=${effectiveMaxJobsPerTick} pollIntervalMs=${effectivePollIntervalMs} ` +
    `summaryIntervalMs=${Number.isFinite(summaryIntervalMs) && summaryIntervalMs > 0 ? summaryIntervalMs : 60000}` +
    `${Number.isFinite(burstSeconds) && burstSeconds > 0 ? ` burstSeconds=${burstSeconds}` : ''}.`,
);
if (shouldRunCronScheduler()) {
  startCronScheduler(prisma);
} else {
  console.log('Cron scheduler disabled (RUN_CRON_SCHEDULER=false). Queue processing only.');
}
if (allowedJobTypes.length > 0) {
  console.log(`Ingestion worker job-type filter: ${allowedJobTypes.join(', ')}`);
}
const loop = startIngestionWorker(prisma, {
  pollIntervalMs: effectivePollIntervalMs,
  maxJobsPerTick: effectiveMaxJobsPerTick,
  staleRecoveryMinutes:
    Number.isFinite(staleRecoveryMinutes) && staleRecoveryMinutes > 0 ? staleRecoveryMinutes : 15,
  staleRecoveryIntervalMs:
    Number.isFinite(staleRecoveryIntervalMs) && staleRecoveryIntervalMs > 0 ? staleRecoveryIntervalMs : 1800000,
  summaryIntervalMs: Number.isFinite(summaryIntervalMs) && summaryIntervalMs > 0 ? summaryIntervalMs : 60000,
  allowedJobTypes,
});

let stopping = false;

const shutdown = async (reason: string) => {
  if (stopping) {
    return;
  }

  stopping = true;
  console.log(`Stopping ingestion worker (${reason}).`);
  loop.stop();
  await loop.drain();
  const summary = await loop.logSummary(reason);
  writeSummaryFile(summary);
  await Promise.race([
    prisma.$disconnect().catch(() => undefined),
    new Promise<void>((resolve) => setTimeout(resolve, 5000)),
  ]);
  process.exit(0);
};

process.on('SIGINT', () => {
  void shutdown('signal_sigint');
});

process.on('SIGTERM', () => {
  void shutdown('signal_sigterm');
});

if (Number.isFinite(burstSeconds) && burstSeconds > 0) {
  console.log(
    `Worker burst budget set to ${burstSeconds}s. The worker will stop itself and emit a summary before the external timeout.`,
  );
  setTimeout(() => {
    void shutdown('burst_complete');
  }, burstSeconds * 1000);
}
