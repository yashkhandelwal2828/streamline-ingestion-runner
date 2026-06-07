import { runSnapshotCleanup } from '../services/cronScheduler';
import { runIngestionCleanup } from '../services/ingestion/cleanup';
import {
  createScriptContext,
  disconnectPrisma,
  logQueueDepth,
  logRunnerConfig,
} from './scriptUtils';

const SCRIPT_NAME = 'ingestion:cleanup';

const parsePositiveIntEnv = (value: string | undefined, defaultValue: number) => {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
};

const main = async () => {
  logRunnerConfig(SCRIPT_NAME);
  const { prisma, store } = createScriptContext();

  try {
    const depthBefore = await logQueueDepth('before', store);
    const historyDaysToKeep = parsePositiveIntEnv(
      process.env.INGESTION_JOB_HISTORY_RETENTION_DAYS,
      7,
    );

    await runSnapshotCleanup(store, { logger: console });

    const cleanupResult = await runIngestionCleanup(
      prisma,
      {
        historyDaysToKeep,
        dryRun: false,
      },
      new Date(),
    );

    const depthAfter = await logQueueDepth('after', store);

    console.log(JSON.stringify({
      script: SCRIPT_NAME,
      deletedSnapshots: cleanupResult.deleted.snapshots,
      deletedJobs: cleanupResult.deleted.jobs,
      deletedMovies: cleanupResult.deleted.movies,
      queueDepthBefore: depthBefore,
      queueDepthAfter: depthAfter,
    }));
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => {
  console.error(`[${SCRIPT_NAME}] failed:`, error);
  process.exit(1);
});
