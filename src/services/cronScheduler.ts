import cron from 'node-cron';
import { PrismaClient } from '@prisma/client';
import {
    createPrismaIngestionStore,
    enqueueCatalogRefreshJob,
    enqueueDeltaRefreshJob,
} from './ingestion/jobQueue';
import { describeIngestionRolloutMode, getIngestionRolloutMode, resolveShadowMode } from './ingestion/rollout';

type CleanupStore = {
    deleteExpiredSnapshots: (now?: Date) => Promise<number>;
};

type DailyTargetFillStore = {
    countMoviesCreatedSince: (since: Date) => Promise<number>;
    findActiveCatalogIngestionJob?: () => Promise<{ id: string; status: string } | null>;
};

type CleanupDeps = {
    now?: () => Date;
    logger?: {
        info: (...args: any[]) => void;
        error: (...args: any[]) => void;
    };
};

const parsePositiveIntEnv = (
    value: string | undefined,
    defaultValue: number,
) => {
    const parsed = Number.parseInt(String(value ?? ''), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return defaultValue;
    }

    return parsed;
};

const parseCronEnv = (
    value: string | undefined,
    defaultValue: string,
) => {
    const candidate = String(value ?? '').trim();
    if (!candidate) {
        return defaultValue;
    }

    return cron.validate(candidate) ? candidate : defaultValue;
};

const startOfUtcDay = (date: Date) => {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
};

export const getScheduledDailyTarget = (
    env: Record<string, string | undefined> = process.env,
) => {
    return parsePositiveIntEnv(env.INGESTION_DAILY_TARGET, 150);
};

export const getIngestionScheduleConfig = (
    env: Record<string, string | undefined> = process.env,
) => {
    return {
        bootstrap: parseCronEnv(env.INGESTION_BOOTSTRAP_CRON, '0 */6 * * *'),
        delta: parseCronEnv(env.INGESTION_DELTA_CRON, '20 */6 * * *'),
        dailyFill: parseCronEnv(env.INGESTION_DAILY_FILL_CRON, '10 */6 * * *'),
        cleanup: parseCronEnv(env.INGESTION_CLEANUP_CRON, '30 2 * * *'),
    };
};

export const runSnapshotCleanup = async (
    ingestionStore: CleanupStore,
    deps: CleanupDeps = {},
) => {
    const now = deps.now ? deps.now() : new Date();
    const logger = deps.logger ?? console;

    try {
        const deletedCount = await ingestionStore.deleteExpiredSnapshots(now);
        logger.info(`Snapshot cleanup removed ${deletedCount} expired payloads.`);
    } catch (error) {
        logger.error('Snapshot cleanup failed:', error);
    }
};

export const buildScheduledIngestionJobInput = (
    rolloutMode = getIngestionRolloutMode(),
    options: {
        dailyTarget?: number;
        pagesToScan?: number;
        source?: string;
    } = {},
) => {
    return {
        source: options.source ?? 'schedule',
        pagesToScan: options.pagesToScan ?? 100,
        dailyTarget: options.dailyTarget ?? getScheduledDailyTarget(),
        mode: 'discover' as const,
        shadowMode: resolveShadowMode(rolloutMode),
    };
};

export const queueDailyTargetFill = async (
    ingestionStore: DailyTargetFillStore & Parameters<typeof enqueueCatalogRefreshJob>[0],
    deps: {
        now?: () => Date;
        logger?: Pick<Console, 'info' | 'error'>;
    } = {},
) => {
    const now = deps.now ? deps.now() : new Date();
    const logger = deps.logger ?? console;
    const dailyTarget = getScheduledDailyTarget();
    const createdToday = await ingestionStore.countMoviesCreatedSince(startOfUtcDay(now));
    const remainingTarget = Math.max(0, dailyTarget - createdToday);

    if (remainingTarget === 0) {
        logger.info(`Daily ingestion target already met (${createdToday}/${dailyTarget}).`);
        return {
            created: false,
            skippedReason: 'daily_target_met',
            dailyTarget,
            createdToday,
            remainingTarget,
        };
    }

    const activeCatalogJob = ingestionStore.findActiveCatalogIngestionJob
        ? await ingestionStore.findActiveCatalogIngestionJob()
        : null;
    if (activeCatalogJob) {
        logger.info(
            `Daily ingestion fill skipped because catalog job ${activeCatalogJob.id} is already ${activeCatalogJob.status}.`,
        );
        return {
            created: false,
            skippedReason: 'active_catalog_job',
            dailyTarget,
            createdToday,
            remainingTarget,
            job: activeCatalogJob,
        };
    }

    const rolloutMode = getIngestionRolloutMode();
    const scheduledInput = buildScheduledIngestionJobInput(rolloutMode, {
        source: 'daily_target_fill',
        dailyTarget: remainingTarget,
    });
    const result = await enqueueCatalogRefreshJob(ingestionStore, scheduledInput);

    logger.info(
        result.created
            ? `Daily ingestion fill queued job ${result.job.id} for remaining target ${remainingTarget}/${dailyTarget}.`
            : `Daily ingestion fill reused active job ${result.job.id} for remaining target ${remainingTarget}/${dailyTarget}.`,
    );

    return {
        ...result,
        dailyTarget,
        createdToday,
        remainingTarget,
    };
};

export const buildScheduledBootstrapInput = (rolloutMode = getIngestionRolloutMode()) => {
    const dailyTarget = parsePositiveIntEnv(process.env.INGESTION_BOOTSTRAP_DAILY_TARGET, 500);
    const targetCatalogSize = parsePositiveIntEnv(process.env.INGESTION_BOOTSTRAP_TARGET_CATALOG_SIZE, 50_000);
    const batchSize = parsePositiveIntEnv(process.env.INGESTION_BOOTSTRAP_BATCH_SIZE, 50);
    const scanWindow = parsePositiveIntEnv(process.env.INGESTION_BOOTSTRAP_SCAN_WINDOW, 4_000);
    const maxScanPerRun = parsePositiveIntEnv(process.env.INGESTION_BOOTSTRAP_MAX_SCAN_PER_RUN, 40_000);

    return {
        source: 'bootstrap',
        pagesToScan: 1,
        dailyTarget,
        mode: 'bootstrap_export' as const,
        targetCatalogSize,
        batchSize,
        scanWindow,
        maxScanPerRun,
        shadowMode: resolveShadowMode(rolloutMode),
    };
};

export const buildScheduledDeltaInput = (rolloutMode = getIngestionRolloutMode()) => {
    return {
        source: 'delta',
        pagesToScan: parsePositiveIntEnv(process.env.INGESTION_DELTA_PAGES_TO_SCAN, 2),
        targetCount: parsePositiveIntEnv(process.env.INGESTION_DELTA_TARGET_COUNT, 30),
        windowHours: parsePositiveIntEnv(process.env.INGESTION_DELTA_WINDOW_HOURS, 48),
        priority: 240,
        shadowMode: resolveShadowMode(rolloutMode),
    };
};

export const isBootstrapIngestionEnabled = (
    env: Record<string, string | undefined> = process.env,
) => {
    const raw = String(env.INGESTION_BOOTSTRAP_ENABLED ?? 'false').trim().toLowerCase();
    if (!raw) {
        return false;
    }

    return raw === 'true' || raw === '1' || raw === 'yes' || raw === 'on';
};

export const startCronScheduler = (prisma: PrismaClient) => {
    const startupRolloutMode = getIngestionRolloutMode();
    const bootstrapEnabled = isBootstrapIngestionEnabled();
    console.log(
        `Starting cron scheduler (${describeIngestionRolloutMode(startupRolloutMode)}).`,
    );
    const ingestionStore = createPrismaIngestionStore(prisma);
    const schedules = getIngestionScheduleConfig();

    if (bootstrapEnabled) {
        cron.schedule(schedules.bootstrap, async () => {
            const rolloutMode = getIngestionRolloutMode();
            const scheduledInput = buildScheduledBootstrapInput(rolloutMode);
            const runMode = scheduledInput.shadowMode ? 'shadow dry-run' : 'live writes';
            console.log(`Queueing bootstrap ingestion (${runMode})...`);
            try {
                const result = await enqueueCatalogRefreshJob(
                    ingestionStore,
                    scheduledInput,
                );
                console.log(
                    result.created
                        ? `Bootstrap ingestion queued as job ${result.job.id}.`
                        : `Bootstrap ingestion skipped because job ${result.job.id} is already active.`,
                );
            } catch (error) {
                console.error('Error queueing bootstrap ingestion:', error);
            }
        });
    } else {
        console.log('Bootstrap ingestion lane disabled by INGESTION_BOOTSTRAP_ENABLED.');
    }

    cron.schedule(schedules.delta, async () => {
        const rolloutMode = getIngestionRolloutMode();
        const deltaInput = buildScheduledDeltaInput(rolloutMode);
        const runMode = deltaInput.shadowMode ? 'shadow dry-run' : 'live writes';
        console.log(`Queueing delta refresh (${runMode})...`);

        try {
            const result = await enqueueDeltaRefreshJob(
                ingestionStore,
                deltaInput,
            );
            console.log(
                result.created
                    ? `Delta refresh queued as job ${result.job.id}.`
                    : `Delta refresh skipped because job ${result.job.id} is already active.`,
            );
        } catch (error) {
            console.error('Error queueing delta refresh:', error);
        }
    });

    cron.schedule(schedules.dailyFill, async () => {
        try {
            await queueDailyTargetFill(ingestionStore);
        } catch (error) {
            console.error('Error queueing daily ingestion fill:', error);
        }
    });

    cron.schedule(schedules.cleanup, async () => {
        await runSnapshotCleanup(ingestionStore);
    });

    console.log(
        `Cron scheduler started (${bootstrapEnabled ? `bootstrap ${schedules.bootstrap}, ` : ''}daily target fill ${schedules.dailyFill}, delta ${schedules.delta}, cleanup ${schedules.cleanup}).`,
    );
};
