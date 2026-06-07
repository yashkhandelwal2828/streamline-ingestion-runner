type CatalogRefreshInput = {
  source: string;
  pagesToScan: number;
  dailyTarget: number;
  mode?: 'discover' | 'bootstrap_export';
  targetCatalogSize?: number;
  batchSize?: number;
  region?: string;
  requestedBy?: string | null;
  createdAt?: Date;
  shadowMode?: boolean;
};

type DeltaRefreshInput = {
  source: string;
  region?: string;
  windowHours?: number;
  pagesToScan?: number;
  targetCount?: number;
  priority?: number;
  shadowMode?: boolean;
  createdAt?: Date;
};

type FetchJobInput = {
  source: string;
  tmdbId: number;
  mediaType: 'movie' | 'tv';
  region?: string;
  requestedBy?: string | null;
  priority?: number;
  shadowMode?: boolean;
  createdAt?: Date;
};

type NormalizeJobInput = {
  source: string;
  tmdbId: number;
  mediaType: 'movie' | 'tv';
  snapshotId: string;
  region?: string;
  priority?: number;
  shadowMode?: boolean;
  createdAt?: Date;
};

type DeepEnrichJobInput = {
  source: string;
  movieId?: number | null;
  tmdbId?: number | null;
  mediaType: 'movie' | 'tv';
  imdbId?: string | null;
  region?: string;
  priority?: number;
  shadowMode?: boolean;
  createdAt?: Date;
};

type RawSnapshotInput = {
  source: string;
  externalId: string;
  region?: string;
  payload: unknown;
  jobId?: string | null;
  capturedAt?: Date;
  retentionDays?: number;
};

type PlatformAvailabilityInput = {
  movieId: number;
  details: Record<string, any>;
  requestedRegions?: string[];
  capturedAt?: Date;
};

type IngestionJobRecord = {
  id: string;
  dedupeKey: string;
  status: string;
};

type IngestionStore = {
  findActiveJobByDedupeKey: (dedupeKey: string) => Promise<IngestionJobRecord | null>;
  createJob: (data: Record<string, any>) => Promise<IngestionJobRecord>;
};

const DEFAULT_REGION = 'IN';
const DEFAULT_RETENTION_DAYS = 1;
const ACTIVE_JOB_STATUSES = ['QUEUED', 'RUNNING'];

const getSnapshotRetentionDays = (env: Record<string, string | undefined> = process.env) => {
  const parsed = Number.parseInt(String(env.INGESTION_SNAPSHOT_RETENTION_DAYS ?? DEFAULT_RETENTION_DAYS), 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return DEFAULT_RETENTION_DAYS;
  }

  return Math.min(parsed, 30);
};

export const normalizeRegion = (region?: string) => {
  return (region || DEFAULT_REGION).trim().toUpperCase();
};

export const buildCatalogRefreshDedupeKey = ({
  pagesToScan,
  dailyTarget,
  mode,
  region,
}: Pick<CatalogRefreshInput, 'pagesToScan' | 'dailyTarget' | 'mode' | 'region'>) => {
  const normalizedMode = mode ?? 'discover';
  const modeSuffix = normalizedMode === 'discover' ? '' : `:${normalizedMode}`;
  return `catalog_refresh:${normalizeRegion(region)}:${pagesToScan}:${dailyTarget}${modeSuffix}`;
};

export const buildCatalogRefreshJobData = (input: CatalogRefreshInput) => {
  const createdAt = input.createdAt ?? new Date();
  const region = normalizeRegion(input.region);
  const mode = input.mode ?? 'discover';

  return {
    type: 'CATALOG_REFRESH',
    status: 'QUEUED',
    source: input.source,
    region,
    requestedBy: input.requestedBy ?? null,
    shadowMode: input.shadowMode ?? false,
    dedupeKey: buildCatalogRefreshDedupeKey(input),
    priority: input.shadowMode ? 50 : 100,
    payload: {
      mode,
      pagesToScan: input.pagesToScan,
      dailyTarget: input.dailyTarget,
      targetCatalogSize: input.targetCatalogSize ?? 50_000,
      batchSize: input.batchSize ?? Math.max(1200, input.dailyTarget),
      region,
    },
    availableAt: createdAt,
    maxAttempts: 5,
  };
};

export const buildDeltaRefreshJobData = (input: DeltaRefreshInput) => {
  const createdAt = input.createdAt ?? new Date();
  const region = normalizeRegion(input.region);
  const windowHours = input.windowHours ?? 36;
  const pagesToScan = input.pagesToScan ?? 12;
  const targetCount = input.targetCount ?? 800;

  return {
    type: 'DISCOVER',
    status: 'QUEUED',
    source: input.source,
    region,
    requestedBy: null,
    shadowMode: input.shadowMode ?? false,
    dedupeKey: `delta_refresh:${region}:${windowHours}:${pagesToScan}:${targetCount}`,
    priority: input.priority ?? 240,
    payload: {
      mode: 'delta_refresh',
      region,
      windowHours,
      pagesToScan,
      targetCount,
    },
    availableAt: createdAt,
    maxAttempts: 5,
  };
};

export const buildFetchJobData = (input: FetchJobInput) => {
  const createdAt = input.createdAt ?? new Date();
  const region = normalizeRegion(input.region);

  return {
    type: 'FETCH',
    status: 'QUEUED',
    source: input.source,
    region,
    requestedBy: input.requestedBy ?? null,
    shadowMode: input.shadowMode ?? false,
    dedupeKey: `fetch:${input.mediaType}:${input.tmdbId}:${region}`,
    priority: input.priority ?? 100,
    payload: {
      tmdbId: input.tmdbId,
      mediaType: input.mediaType,
    },
    availableAt: createdAt,
    maxAttempts: 5,
  };
};

export const buildNormalizeJobData = (input: NormalizeJobInput) => {
  const createdAt = input.createdAt ?? new Date();
  const region = normalizeRegion(input.region);

  return {
    type: 'NORMALIZE',
    status: 'QUEUED',
    source: input.source,
    region,
    requestedBy: null,
    shadowMode: input.shadowMode ?? false,
    dedupeKey: `normalize:${input.mediaType}:${input.tmdbId}:${input.snapshotId}`,
    priority: input.priority ?? 100,
    payload: {
      tmdbId: input.tmdbId,
      mediaType: input.mediaType,
      snapshotId: input.snapshotId,
    },
    availableAt: createdAt,
    maxAttempts: 5,
  };
};

export const buildDeepEnrichJobData = (input: DeepEnrichJobInput) => {
  const createdAt = input.createdAt ?? new Date();
  const region = normalizeRegion(input.region);
  const dedupeIdentity = input.movieId ?? `tmdb:${input.tmdbId ?? 'unknown'}`;

  return {
    type: 'DEEP_ENRICH',
    status: 'QUEUED',
    source: input.source,
    region,
    requestedBy: null,
    shadowMode: input.shadowMode ?? false,
    dedupeKey: `deep_enrich:${input.mediaType}:${dedupeIdentity}`,
    priority: input.priority ?? 200,
    payload: {
      movieId: input.movieId ?? null,
      tmdbId: input.tmdbId ?? null,
      mediaType: input.mediaType,
      imdbId: input.imdbId ?? null,
    },
    availableAt: createdAt,
    maxAttempts: 5,
  };
};

export const enqueueCatalogRefreshJob = async (
  store: IngestionStore,
  input: CatalogRefreshInput,
) => {
  const jobData = buildCatalogRefreshJobData(input);
  const existingJob = await store.findActiveJobByDedupeKey(jobData.dedupeKey);

  if (existingJob) {
    return {
      created: false,
      job: existingJob,
    };
  }

  const createdJob = await store.createJob(jobData);

  return {
    created: true,
    job: createdJob,
  };
};

export const enqueueDeltaRefreshJob = async (
  store: IngestionStore,
  input: DeltaRefreshInput,
) => {
  const jobData = buildDeltaRefreshJobData(input);
  const existingJob = await store.findActiveJobByDedupeKey(jobData.dedupeKey);

  if (existingJob) {
    return {
      created: false,
      job: existingJob,
    };
  }

  const createdJob = await store.createJob(jobData);
  return {
    created: true,
    job: createdJob,
  };
};

export const buildPlatformAvailabilityRecords = ({
  movieId,
  details,
  requestedRegions = [DEFAULT_REGION],
  capturedAt = new Date(),
}: PlatformAvailabilityInput) => {
  const providerResults = details?.['watch/providers']?.results ?? {};

  return requestedRegions.flatMap((requestedRegion) => {
    const region = normalizeRegion(requestedRegion);
    const regionResult = providerResults[region];
    const flatrateProviders = Array.isArray(regionResult?.flatrate) ? regionResult.flatrate : [];

    return flatrateProviders
      .filter((provider: { provider_name?: string }) => typeof provider.provider_name === 'string' && provider.provider_name.trim().length > 0)
      .map((provider: { provider_name: string }) => ({
        movieId,
        region,
        providerName: provider.provider_name.trim(),
        watchLink: regionResult?.link ?? null,
        lastSeenAt: capturedAt,
      }));
  });
};

export const buildRawSnapshotData = ({
  source,
  externalId,
  region,
  payload,
  jobId,
  capturedAt = new Date(),
  retentionDays = getSnapshotRetentionDays(),
}: RawSnapshotInput) => {
  const expiresAt = new Date(capturedAt);
  expiresAt.setUTCDate(expiresAt.getUTCDate() + retentionDays);

  return {
    source,
    externalId,
    region: normalizeRegion(region),
    payload,
    jobId: jobId ?? null,
    createdAt: capturedAt,
    expiresAt,
  };
};

export const createPrismaIngestionStore = (prisma: {
  ingestionJob: {
    findFirst: (args?: any) => Promise<IngestionJobRecord | null>;
    create: (args: any) => Promise<IngestionJobRecord>;
    findUnique?: (args: any) => Promise<any>;
    update?: (args: any) => Promise<any>;
    updateMany?: (args: any) => Promise<{ count: number }>;
    findMany?: (args: any) => Promise<any[]>;
    count?: (args: any) => Promise<number>;
  };
  ingestionSnapshot?: {
    create: (args: any) => Promise<any>;
    findUnique: (args: any) => Promise<any>;
    findFirst?: (args: any) => Promise<any>;
    findMany?: (args: any) => Promise<any[]>;
    deleteMany?: (args: any) => Promise<{ count: number }>;
  };
  movie?: {
    upsert: (args: any) => Promise<any>;
    findUnique: (args: any) => Promise<any>;
    findMany?: (args: any) => Promise<any[]>;
    count?: (args: any) => Promise<number>;
    update: (args: any) => Promise<any>;
  };
  platformAvailability?: {
    deleteMany: (args: any) => Promise<any>;
    createMany: (args: any) => Promise<any>;
  };
  $transaction?: (arg: any) => Promise<any>;
}) => {
  const findActiveJobByDedupeKey = async (dedupeKey: string) => {
    return prisma.ingestionJob.findFirst({
      where: {
        dedupeKey,
        status: {
          in: ACTIVE_JOB_STATUSES,
        },
      },
      orderBy: {
        createdAt: 'desc',
      },
    });
  };

  const createJob = async (data: Record<string, any>) => {
    return prisma.ingestionJob.create({
      data,
    });
  };

  return {
    findActiveJobByDedupeKey,
    createJob,
    async enqueueJob(data: Record<string, any>) {
      const existingJob = await findActiveJobByDedupeKey(data.dedupeKey);
      if (existingJob) {
        return existingJob;
      }

      return createJob(data);
    },
    async findActiveCatalogIngestionJob() {
      if (!prisma.ingestionJob.findFirst) {
        throw new Error('findActiveCatalogIngestionJob requires Prisma ingestionJob.findFirst support');
      }

      return prisma.ingestionJob.findFirst({
        where: {
          type: {
            in: ['CATALOG_REFRESH', 'FETCH', 'NORMALIZE'],
          },
          source: {
            in: ['schedule', 'daily_target_fill'],
          },
          status: {
            in: ACTIVE_JOB_STATUSES,
          },
        },
        orderBy: {
          createdAt: 'asc',
        },
      });
    },
    async claimNextJob(now = new Date()) {
      if (!prisma.ingestionJob.findUnique || !prisma.ingestionJob.updateMany) {
        throw new Error('claimNextJob requires Prisma ingestionJob.findUnique and updateMany support');
      }

      const candidate = await prisma.ingestionJob.findFirst({
        where: {
          status: 'QUEUED',
          availableAt: {
            lte: now,
          },
        },
        orderBy: [
          { priority: 'desc' },
          { availableAt: 'asc' },
          { createdAt: 'asc' },
        ],
      });

      if (!candidate) {
        return null;
      }

      const claimed = await prisma.ingestionJob.updateMany({
        where: {
          id: candidate.id,
          status: 'QUEUED',
        },
        data: {
          status: 'RUNNING',
          lockedAt: now,
          startedAt: now,
          lastHeartbeatAt: now,
          errorMessage: null,
        },
      });

      if (claimed.count === 0) {
        return null;
      }

      return prisma.ingestionJob.findUnique({
        where: { id: candidate.id },
      });
    },
    async completeJob(jobId: string, details: Record<string, any> = {}) {
      if (!prisma.ingestionJob.update) {
        throw new Error('completeJob requires Prisma ingestionJob.update support');
      }

      return prisma.ingestionJob.update({
        where: { id: jobId },
        data: {
          status: 'COMPLETED',
          completedAt: new Date(),
          lockedAt: null,
          lastHeartbeatAt: null,
          errorMessage: null,
          payload: details.payload ?? undefined,
        },
      });
    },
    async rescheduleJob(jobId: string, details: { errorMessage: string; availableAt: Date }) {
      if (!prisma.ingestionJob.update || !prisma.ingestionJob.findUnique) {
        throw new Error('rescheduleJob requires Prisma ingestionJob.findUnique and update support');
      }

      const current = await prisma.ingestionJob.findUnique({ where: { id: jobId } });
      return prisma.ingestionJob.update({
        where: { id: jobId },
        data: {
          status: 'QUEUED',
          attempts: (current?.attempts ?? 0) + 1,
          availableAt: details.availableAt,
          failedAt: new Date(),
          lockedAt: null,
          lastHeartbeatAt: null,
          errorMessage: details.errorMessage,
        },
      });
    },
    async deferJob(jobId: string, details: { availableAt: Date; reason?: string | null }) {
      if (!prisma.ingestionJob.update) {
        throw new Error('deferJob requires Prisma ingestionJob.update support');
      }

      return prisma.ingestionJob.update({
        where: { id: jobId },
        data: {
          status: 'QUEUED',
          availableAt: details.availableAt,
          lockedAt: null,
          lastHeartbeatAt: null,
          errorMessage: details.reason ?? null,
        },
      });
    },
    async recoverStaleRunningJobs(
      now = new Date(),
      options: { staleAfterMinutes?: number; limit?: number } = {},
    ) {
      if (!prisma.ingestionJob.findMany || !prisma.ingestionJob.update || !prisma.ingestionJob.findUnique) {
        throw new Error('recoverStaleRunningJobs requires Prisma ingestionJob.findMany/findUnique/update support');
      }

      const staleAfterMinutes = options.staleAfterMinutes ?? 15;
      const staleBefore = new Date(now);
      staleBefore.setUTCMinutes(staleBefore.getUTCMinutes() - staleAfterMinutes);
      const limit = options.limit ?? 25;

      const staleJobs = await prisma.ingestionJob.findMany({
        where: {
          status: 'RUNNING',
          OR: [
            { lastHeartbeatAt: { lte: staleBefore } },
            { lastHeartbeatAt: null, lockedAt: { lte: staleBefore } },
          ],
        },
        orderBy: { lockedAt: 'asc' },
        take: limit,
        select: { id: true },
      });

      for (const staleJob of staleJobs) {
        const current = await prisma.ingestionJob.findUnique({ where: { id: staleJob.id } });
        if (!current || current.status !== 'RUNNING') {
          continue;
        }

        await prisma.ingestionJob.update({
          where: { id: staleJob.id },
          data: {
            status: 'QUEUED',
            attempts: (current.attempts ?? 0) + 1,
            availableAt: now,
            failedAt: now,
            lockedAt: null,
            lastHeartbeatAt: null,
            errorMessage: 'stale_running_job_recovered',
          },
        });
      }

      return staleJobs.length;
    },
    async deadLetterJob(jobId: string, details: { errorMessage: string }) {
      if (!prisma.ingestionJob.update || !prisma.ingestionJob.findUnique) {
        throw new Error('deadLetterJob requires Prisma ingestionJob.findUnique and update support');
      }

      const current = await prisma.ingestionJob.findUnique({ where: { id: jobId } });
      return prisma.ingestionJob.update({
        where: { id: jobId },
        data: {
          status: 'DEAD_LETTERED',
          attempts: (current?.attempts ?? 0) + 1,
          failedAt: new Date(),
          lockedAt: null,
          lastHeartbeatAt: null,
          errorMessage: details.errorMessage,
        },
      });
    },
    async createSnapshot(data: any) {
      if (!prisma.ingestionSnapshot) {
        throw new Error('createSnapshot requires Prisma ingestionSnapshot support');
      }

      return prisma.ingestionSnapshot.create({ data });
    },
    async getSnapshotById(snapshotId: string) {
      if (!prisma.ingestionSnapshot) {
        throw new Error('getSnapshotById requires Prisma ingestionSnapshot support');
      }

      return prisma.ingestionSnapshot.findUnique({ where: { id: snapshotId } });
    },
    async deleteSnapshotById(snapshotId: string) {
      if (!prisma.ingestionSnapshot?.deleteMany) {
        throw new Error('deleteSnapshotById requires Prisma ingestionSnapshot.deleteMany support');
      }

      const result = await prisma.ingestionSnapshot.deleteMany({
        where: { id: snapshotId },
      });

      return result.count;
    },
    async upsertMovie(movie: Record<string, any>) {
      if (!prisma.movie) {
        throw new Error('upsertMovie requires Prisma movie support');
      }

      return prisma.movie.upsert({
        where: {
          tmdbId_mediaType: {
            tmdbId: movie.tmdbId,
            mediaType: movie.mediaType,
          },
        },
        update: movie,
        create: movie,
      });
    },
    async getMovieCount() {
      if (!prisma.movie?.count) {
        throw new Error('getMovieCount requires Prisma movie.count support');
      }

      return prisma.movie.count({});
    },
    async countMoviesCreatedSince(since: Date) {
      if (!prisma.movie?.count) {
        throw new Error('countMoviesCreatedSince requires Prisma movie.count support');
      }

      return prisma.movie.count({
        where: {
          createdAt: {
            gte: since,
          },
        },
      });
    },
    async listExistingTmdbIds(tmdbIds: number[], mediaType: 'movie' | 'tv') {
      if (!prisma.movie?.findMany) {
        throw new Error('listExistingTmdbIds requires Prisma movie.findMany support');
      }

      if (tmdbIds.length === 0) {
        return [];
      }

      const rows = await prisma.movie.findMany({
        where: {
          tmdbId: { in: tmdbIds },
          mediaType,
        },
        select: { tmdbId: true },
      });
      return rows.map((row: { tmdbId: number }) => row.tmdbId);
    },
    async replacePlatformAvailability(movieId: number, records: Array<Record<string, any>>) {
      if (!prisma.platformAvailability || !prisma.$transaction) {
        throw new Error('replacePlatformAvailability requires Prisma platformAvailability and transaction support');
      }

      const regions = Array.from(new Set(records.map((record) => record.region)));
      return prisma.$transaction(async (tx: any) => {
        await tx.platformAvailability.deleteMany({
          where: {
            movieId,
            ...(regions.length > 0 ? { region: { in: regions } } : {}),
          },
        });

        if (records.length > 0) {
          await tx.platformAvailability.createMany({
            data: records,
          });
        }
      });
    },
    async findMovieById(movieId: number) {
      if (!prisma.movie) {
        throw new Error('findMovieById requires Prisma movie support');
      }

      return prisma.movie.findUnique({ where: { id: movieId } });
    },
    async updateMovieById(movieId: number, data: Record<string, any>) {
      if (!prisma.movie) {
        throw new Error('updateMovieById requires Prisma movie support');
      }

      return prisma.movie.update({
        where: { id: movieId },
        data,
      });
    },
    async updateMovieByTmdbId(tmdbId: number, mediaType: 'movie' | 'tv', data: Record<string, any>) {
      if (!prisma.movie) {
        throw new Error('updateMovieByTmdbId requires Prisma movie support');
      }

      return prisma.movie.update({
        where: {
          tmdbId_mediaType: {
            tmdbId,
            mediaType,
          },
        },
        data,
      });
    },
    async replayJob(jobId: string) {
      if (!prisma.ingestionJob.findUnique || !prisma.ingestionJob.update) {
        throw new Error('replayJob requires Prisma ingestionJob.findUnique and update support');
      }

      const job = await prisma.ingestionJob.findUnique({ where: { id: jobId } });
      if (!job) {
        return null;
      }

      return prisma.ingestionJob.update({
        where: { id: jobId },
        data: {
          status: 'QUEUED',
          attempts: 0,
          availableAt: new Date(),
          startedAt: null,
          completedAt: null,
          failedAt: null,
          lockedAt: null,
          lastHeartbeatAt: null,
          errorMessage: null,
        },
      });
    },
    async listShadowJobs(limit = 50) {
      if (!prisma.ingestionJob.findMany) {
        throw new Error('listShadowJobs requires Prisma ingestionJob.findMany support');
      }

      return prisma.ingestionJob.findMany({
        where: { shadowMode: true },
        orderBy: { createdAt: 'desc' },
        take: limit,
      });
    },
    async deleteExpiredSnapshots(now = new Date()) {
      if (!prisma.ingestionSnapshot?.deleteMany) {
        throw new Error('deleteExpiredSnapshots requires Prisma ingestionSnapshot.deleteMany support');
      }

      const result = await prisma.ingestionSnapshot.deleteMany({
        where: {
          expiresAt: {
            lte: now,
          },
        },
      });

      return result.count;
    },
    async getInternalState(key: string) {
      if (!prisma.ingestionSnapshot?.findFirst) {
        throw new Error('getInternalState requires Prisma ingestionSnapshot.findFirst support');
      }

      const snapshot = await prisma.ingestionSnapshot.findFirst({
        where: {
          source: 'INTERNAL',
          externalId: key,
        },
        orderBy: { createdAt: 'desc' },
        select: {
          payload: true,
          createdAt: true,
        },
      });

      return snapshot ? { value: snapshot.payload, updatedAt: snapshot.createdAt } : null;
    },
    async setInternalState(key: string, value: unknown, now = new Date()) {
      if (!prisma.ingestionSnapshot?.create || !prisma.ingestionSnapshot?.deleteMany) {
        throw new Error('setInternalState requires Prisma ingestionSnapshot.create and deleteMany support');
      }

      await prisma.ingestionSnapshot.deleteMany({
        where: {
          source: 'INTERNAL',
          externalId: key,
        },
      });

      const expiresAt = new Date(now);
      expiresAt.setUTCFullYear(expiresAt.getUTCFullYear() + 20);

      return prisma.ingestionSnapshot.create({
        data: {
          source: 'INTERNAL',
          externalId: key,
          region: DEFAULT_REGION,
          payload: value as any,
          expiresAt,
          createdAt: now,
        },
      });
    },
    async getOmdbUsage(now = new Date()) {
      if (!prisma.ingestionJob.count) {
        throw new Error('getOmdbUsage requires Prisma ingestionJob.count support');
      }

      const dayStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
      const dayEnd = new Date(dayStart);
      dayEnd.setUTCDate(dayEnd.getUTCDate() + 1);

      const [regular, retries] = await Promise.all([
        prisma.ingestionJob.count({
          where: {
            type: 'DEEP_ENRICH',
            status: 'COMPLETED',
            shadowMode: false,
            attempts: 0,
            completedAt: {
              gte: dayStart,
              lt: dayEnd,
            },
          },
        }),
        prisma.ingestionJob.count({
          where: {
            type: 'DEEP_ENRICH',
            status: 'COMPLETED',
            shadowMode: false,
            attempts: {
              gt: 0,
            },
            completedAt: {
              gte: dayStart,
              lt: dayEnd,
            },
          },
        }),
      ]);

      return {
        regular,
        retries,
        total: regular + retries,
        dayStart,
        dayEnd,
      };
    },
    async countQueuedAndRunningJobs() {
      if (!prisma.ingestionJob.count) {
        throw new Error('countQueuedAndRunningJobs requires Prisma ingestionJob.count support');
      }

      return prisma.ingestionJob.count({
        where: {
          status: {
            in: ACTIVE_JOB_STATUSES,
          },
        },
      });
    },
    async getOperatorSnapshot(now = new Date(), options: { staleAfterMinutes?: number; limit?: number } = {}) {
      if (!prisma.ingestionJob.findMany || !prisma.ingestionJob.count) {
        throw new Error('getOperatorSnapshot requires Prisma ingestionJob.findMany and count support');
      }

      const staleAfterMinutes = options.staleAfterMinutes ?? 15;
      const staleBefore = new Date(now);
      staleBefore.setUTCMinutes(staleBefore.getUTCMinutes() - staleAfterMinutes);

      const [queued, running, deadLettered, queuedJobs, staleRunning, completedRefreshes, throttledJobs] =
        await Promise.all([
          prisma.ingestionJob.count({ where: { status: 'QUEUED' } }),
          prisma.ingestionJob.count({ where: { status: 'RUNNING' } }),
          prisma.ingestionJob.count({ where: { status: 'DEAD_LETTERED' } }),
          prisma.ingestionJob.findMany({
            where: { status: 'QUEUED' },
            orderBy: [{ priority: 'desc' }, { createdAt: 'asc' }],
            take: options.limit ?? 200,
            select: {
              type: true,
              createdAt: true,
            },
          }),
          prisma.ingestionJob.findMany({
            where: {
              status: 'RUNNING',
              OR: [
                { lastHeartbeatAt: { lte: staleBefore } },
                { lastHeartbeatAt: null, lockedAt: { lte: staleBefore } },
              ],
            },
            orderBy: { lockedAt: 'asc' },
            take: 25,
            select: {
              id: true,
              type: true,
              source: true,
              lockedAt: true,
              lastHeartbeatAt: true,
            },
          }),
          prisma.ingestionJob.findMany({
            where: {
              type: 'CATALOG_REFRESH',
              status: 'COMPLETED',
            },
            orderBy: { completedAt: 'desc' },
            take: 1,
            select: {
              completedAt: true,
            },
          }),
          prisma.ingestionJob.findMany({
            where: {
              errorMessage: {
                contains: '429',
              },
            },
            orderBy: { updatedAt: 'desc' },
            take: 50,
            select: {
              source: true,
            },
          }),
        ]);

      const jobsByType = queuedJobs.reduce((acc: Record<string, number>, job: { type: string }) => {
        acc[job.type] = (acc[job.type] ?? 0) + 1;
        return acc;
      }, {});

      const throttledSourcesMap = throttledJobs.reduce((acc: Record<string, number>, job: { source: string }) => {
        const key = String(job.source).toUpperCase();
        acc[key] = (acc[key] ?? 0) + 1;
        return acc;
      }, {});

      return {
        queued,
        running,
        deadLettered,
        staleRunning,
        oldestQueuedAt: queuedJobs[0]?.createdAt ?? null,
        latestCatalogRefreshCompletedAt: completedRefreshes[0]?.completedAt ?? null,
        jobsByType,
        throttledSources: Object.entries(throttledSourcesMap).map(([source, count]) => ({
          source,
          count: Number(count),
        })),
      };
    },
  };
};
