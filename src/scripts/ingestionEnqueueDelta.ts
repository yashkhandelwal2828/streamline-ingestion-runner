import { buildScheduledDeltaInput } from '../services/cronScheduler';
import { enqueueDeltaRefreshJob } from '../services/ingestion/jobQueue';
import { INGESTION_DISCOVERY_LANE_JOB_TYPES } from '../services/ingestion/queueConfig';
import {
  createScriptContext,
  disconnectPrisma,
  logQueueDepth,
  logRunnerConfig,
  skipIfQueueAboveWatermark,
} from './scriptUtils';

const SCRIPT_NAME = 'ingestion:enqueue-delta';

const main = async () => {
  logRunnerConfig(SCRIPT_NAME);
  const { prisma, store } = createScriptContext();

  try {
    const catalogSizeBefore = await store.getMovieCount();
    const depthBefore = await logQueueDepth('before', store);

    // Only the discovery lane counts: a DEEP_ENRICH backlog is drained by its own worker and gate.
    const watermark = await skipIfQueueAboveWatermark(store, SCRIPT_NAME, {
      allowedJobTypes: INGESTION_DISCOVERY_LANE_JOB_TYPES,
    });
    if (watermark.skipped) {
      console.log(JSON.stringify({
        script: SCRIPT_NAME,
        skippedReason: 'queue_high_watermark',
        queueDepth: watermark.depth,
        catalogSize: catalogSizeBefore,
      }));
      return;
    }

    const deltaInput = buildScheduledDeltaInput();
    const result = await enqueueDeltaRefreshJob(store, deltaInput);
    const depthAfter = await logQueueDepth('after', store);
    const catalogSizeAfter = await store.getMovieCount();

    console.log(JSON.stringify({
      script: SCRIPT_NAME,
      ...result,
      deltaInput,
      queueDepthBefore: depthBefore,
      queueDepthAfter: depthAfter,
      catalogSizeBefore,
      catalogSizeAfter,
    }));
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => {
  console.error(`[${SCRIPT_NAME}] failed:`, error);
  process.exit(1);
});
