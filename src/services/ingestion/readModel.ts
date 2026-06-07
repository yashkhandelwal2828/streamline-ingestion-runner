import { normalizeRegion } from './jobQueue';

export const buildRegionAwareProviderFilter = (providers: string[], region?: string) => {
  const normalizedRegion = normalizeRegion(region);

  return {
    OR: [
      {
        platformAvailability: {
          some: {
            region: normalizedRegion,
            providerName: { in: providers },
          },
        },
      },
      {
        watchProviders: { hasSome: providers },
      },
    ],
  };
};

export const applyRegionAvailabilityToMovies = (movies: Array<Record<string, any>>) => {
  return movies.map((movie) => {
    const availability = Array.isArray(movie.platformAvailability) ? movie.platformAvailability : [];
    if (availability.length === 0) {
      const { platformAvailability, ...rest } = movie;
      return rest;
    }

    const { platformAvailability, ...rest } = movie;
    return {
      ...rest,
      watchProviders: availability.map((entry) => entry.providerName),
      watchLink: availability[0]?.watchLink ?? rest.watchLink ?? null,
    };
  });
};

export const buildShadowStatusSummary = (jobs: Array<{ status: string }>) => {
  const summary = {
    queued: 0,
    running: 0,
    completed: 0,
    failed: 0,
    deadLettered: 0,
    total: jobs.length,
    healthy: true,
  };

  for (const job of jobs) {
    switch (job.status) {
      case 'QUEUED':
        summary.queued += 1;
        break;
      case 'RUNNING':
        summary.running += 1;
        break;
      case 'COMPLETED':
        summary.completed += 1;
        break;
      case 'FAILED':
        summary.failed += 1;
        summary.healthy = false;
        break;
      case 'DEAD_LETTERED':
        summary.deadLettered += 1;
        summary.healthy = false;
        break;
      default:
        summary.healthy = false;
        break;
    }
  }

  return summary;
};

type OperatorStatusInput = {
  queued: number;
  running: number;
  deadLettered: number;
  staleRunning: Array<{
    id: string;
    type: string;
    source: string;
    lockedAt?: Date | null;
    lastHeartbeatAt?: Date | null;
  }>;
  oldestQueuedAt?: Date | null;
  latestCatalogRefreshCompletedAt?: Date | null;
  jobsByType: Record<string, number>;
  throttledSources: Array<{
    source: string;
    count: number;
  }>;
};

const toRoundedMinutes = (valueMs: number) => {
  return Math.max(0, Math.round(valueMs / 60000));
};

export const buildOperatorStatusSummary = (
  input: OperatorStatusInput,
  now = new Date(),
) => {
  const stuckJobs = input.staleRunning.map((job) => {
    const referenceTime = job.lastHeartbeatAt ?? job.lockedAt ?? now;
    return {
      id: job.id,
      type: job.type,
      source: job.source,
      minutesStuck: toRoundedMinutes(now.getTime() - referenceTime.getTime()),
    };
  });

  const freshnessReference = input.latestCatalogRefreshCompletedAt ?? input.oldestQueuedAt ?? now;
  const freshestLagMinutes = toRoundedMinutes(now.getTime() - freshnessReference.getTime());
  const oldestQueuedAgeMinutes = input.oldestQueuedAt
    ? toRoundedMinutes(now.getTime() - input.oldestQueuedAt.getTime())
    : 0;

  return {
    queued: input.queued,
    running: input.running,
    deadLettered: input.deadLettered,
    backlogByType: input.jobsByType,
    stuckJobs,
    freshnessLagMinutes: freshestLagMinutes,
    oldestQueuedAgeMinutes,
    throttledSources: input.throttledSources,
    healthy:
      input.deadLettered === 0 &&
      stuckJobs.length === 0 &&
      input.throttledSources.length === 0,
  };
};
