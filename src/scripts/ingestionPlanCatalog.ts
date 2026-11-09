import { readPlannerConfig, runCatalogPlanner } from '../services/ingestion/catalogPlanner';
import { discoverReleasedBetween, getDailyExportEntries } from '../services/tmdb';
import { createScriptContext, disconnectPrisma } from './scriptUtils';

const SCRIPT_NAME = 'ingestion:plan-catalog';

const main = async () => {
  const { prisma, store } = createScriptContext();
  try {
    const config = readPlannerConfig();
    console.log(`[${SCRIPT_NAME}] config`, JSON.stringify(config));
    await runCatalogPlanner({
      prisma,
      enqueue: (data) => store.enqueueJob(data),
      discover: (mediaType, page, options) => discoverReleasedBetween(mediaType, page, options),
      exportEntries: (mediaType, now) => getDailyExportEntries(mediaType, now),
    }, config);
  } finally {
    await disconnectPrisma(prisma);
  }
};

main().catch((error) => {
  console.error(`[${SCRIPT_NAME}] failed:`, error);
  process.exit(1);
});
