import fs from 'node:fs';
import path from 'node:path';

export type WorkerSummarySnapshot = {
  processed?: number;
  failed?: number;
  backlog?: number | null;
};

/**
 * Decides whether a finished worker burst should dispatch the next one.
 *
 * GitHub's scheduled workflow cron is frequently delayed by hours, so a burst that
 * finishes with work still queued re-dispatches itself until the queue drains or the
 * hop limit is reached.
 */
export const decideChainHop = (
  summary: WorkerSummarySnapshot | null,
  options: { depth: number; maxHops: number },
) => {
  const depth = Math.max(0, options.depth);
  const maxHops = Math.max(0, options.maxHops);
  const processed = summary?.processed ?? 0;
  const backlog = typeof summary?.backlog === 'number' ? summary.backlog : processed > 0 ? 1 : 0;

  return {
    shouldChain: summary !== null && backlog > 0 && depth < maxHops,
    depth,
    nextDepth: depth + 1,
    maxHops,
    backlog,
    processed,
    failed: summary?.failed ?? 0,
  };
};

const readSummary = (summaryFile: string | undefined): WorkerSummarySnapshot | null => {
  if (!summaryFile) {
    return null;
  }

  const target = path.resolve(summaryFile);
  if (!fs.existsSync(target)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(target, 'utf8')) as WorkerSummarySnapshot;
  } catch (error) {
    console.error(`Failed to parse worker summary at ${target}:`, error);
    return null;
  }
};

const main = () => {
  const depth = Number.parseInt(String(process.env.CHAIN_DEPTH ?? '0'), 10);
  const maxHops = Number.parseInt(String(process.env.INGESTION_WORKER_MAX_CHAIN_HOPS ?? '24'), 10);

  const decision = decideChainHop(readSummary(process.env.INGESTION_WORKER_SUMMARY_FILE), {
    depth: Number.isFinite(depth) && depth > 0 ? depth : 0,
    maxHops: Number.isFinite(maxHops) && maxHops > 0 ? maxHops : 24,
  });

  console.log(JSON.stringify(decision));
};

if (require.main === module) {
  main();
}
