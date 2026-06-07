export type IngestionRolloutMode = 'shadow' | 'live';

export const getIngestionRolloutMode = (
  env: Record<string, string | undefined> = process.env,
): IngestionRolloutMode => {
  const mode = String(env.INGESTION_ROLLOUT_MODE ?? '').trim().toLowerCase();
  return mode === 'shadow' ? 'shadow' : 'live';
};

export const resolveShadowMode = (
  rolloutMode: IngestionRolloutMode,
  requestedShadowMode?: boolean,
) => {
  if (rolloutMode === 'shadow') {
    return true;
  }

  return requestedShadowMode ?? false;
};

export const buildIngestionRolloutStatus = (rolloutMode: IngestionRolloutMode) => {
  return {
    mode: rolloutMode,
    writesEnabled: rolloutMode === 'live',
    promotionRequired: rolloutMode !== 'live',
  };
};

export const describeIngestionRolloutMode = (rolloutMode: IngestionRolloutMode) => {
  if (rolloutMode === 'live') {
    return 'live writes enabled';
  }

  return 'shadow dry-run (writes disabled)';
};
