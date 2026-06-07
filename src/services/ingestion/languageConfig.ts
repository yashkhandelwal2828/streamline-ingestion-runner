export type IngestionLanguageMode = 'weighted' | 'off';

export type LanguageWeight = {
  code: string;
  weight: number;
};

export type LanguageTarget = {
  code: string;
  target: number;
};

const DEFAULT_LANGUAGE_PROFILE = 'india_first';

const LANGUAGE_PROFILE_WEIGHTS: Record<string, LanguageWeight[]> = {
  india_first: [
    { code: 'hi', weight: 38 },
    { code: 'en', weight: 24 },
    { code: 'ta', weight: 9 },
    { code: 'te', weight: 8 },
    { code: 'ml', weight: 6 },
    { code: 'bn', weight: 6 },
    { code: 'mr', weight: 4 },
    { code: 'kn', weight: 3 },
    { code: 'pa', weight: 1 },
    { code: 'gu', weight: 1 },
  ],
  english_first: [
    { code: 'en', weight: 70 },
    { code: 'hi', weight: 15 },
    { code: 'ta', weight: 5 },
    { code: 'te', weight: 5 },
    { code: 'ml', weight: 3 },
    { code: 'bn', weight: 2 },
  ],
  current_mix: [
    { code: 'en', weight: 38 },
    { code: 'hi', weight: 23 },
    { code: 'zh', weight: 8 },
    { code: 'ja', weight: 6 },
    { code: 'fr', weight: 5 },
    { code: 'de', weight: 4 },
    { code: 'es', weight: 4 },
    { code: 'ko', weight: 3 },
    { code: 'pt', weight: 3 },
    { code: 'it', weight: 3 },
    { code: 'ru', weight: 3 },
  ],
};

const isValidLanguageCode = (value: string) => /^[a-z]{2,3}(?:-[a-z]{2})?$/.test(value);

const parseLanguageWeights = (raw: string | undefined): LanguageWeight[] => {
  if (!raw) {
    return [];
  }

  const aggregated = new Map<string, number>();
  const entries = raw.split(',');
  for (const entryRaw of entries) {
    const entry = entryRaw.trim();
    if (!entry) {
      continue;
    }

    const [codeRaw, weightRaw] = entry.split(':');
    const code = String(codeRaw ?? '').trim().toLowerCase();
    const weight = Number.parseFloat(String(weightRaw ?? '').trim());
    if (!isValidLanguageCode(code) || !Number.isFinite(weight) || weight <= 0) {
      continue;
    }

    aggregated.set(code, (aggregated.get(code) ?? 0) + weight);
  }

  return Array.from(aggregated.entries()).map(([code, weight]) => ({ code, weight }));
};

const normalizeMode = (value: string | undefined): IngestionLanguageMode => {
  const mode = String(value ?? '').trim().toLowerCase();
  return mode === 'off' ? 'off' : 'weighted';
};

export const getIngestionLanguageMode = (
  env: Record<string, string | undefined> = process.env,
): IngestionLanguageMode => {
  return normalizeMode(env.INGESTION_LANGUAGE_MODE);
};

export const getIngestionLanguageProfile = (
  env: Record<string, string | undefined> = process.env,
): string => {
  const profile = String(env.INGESTION_LANGUAGE_PROFILE ?? DEFAULT_LANGUAGE_PROFILE).trim().toLowerCase();
  if (LANGUAGE_PROFILE_WEIGHTS[profile]) {
    return profile;
  }

  return DEFAULT_LANGUAGE_PROFILE;
};

export const getIngestionLanguageWeights = (
  env: Record<string, string | undefined> = process.env,
): LanguageWeight[] => {
  if (getIngestionLanguageMode(env) === 'off') {
    return [];
  }

  const explicitWeights = parseLanguageWeights(env.INGESTION_LANGUAGE_WEIGHTS);
  if (explicitWeights.length > 0) {
    return explicitWeights;
  }

  const profile = getIngestionLanguageProfile(env);
  return LANGUAGE_PROFILE_WEIGHTS[profile].map((entry) => ({ ...entry }));
};

export const buildLanguageTargets = (targetCount: number, weights: LanguageWeight[]): LanguageTarget[] => {
  const normalizedTarget = Number.isFinite(targetCount) ? Math.max(0, Math.floor(targetCount)) : 0;
  if (normalizedTarget === 0 || weights.length === 0) {
    return [];
  }

  const validWeights = weights.filter((entry) => Number.isFinite(entry.weight) && entry.weight > 0);
  const totalWeight = validWeights.reduce((sum, entry) => sum + entry.weight, 0);
  if (totalWeight <= 0) {
    return [];
  }

  const provisional = validWeights.map((entry, index) => {
    const exact = (normalizedTarget * entry.weight) / totalWeight;
    const floor = Math.floor(exact);
    return {
      code: entry.code,
      floor,
      fraction: exact - floor,
      index,
    };
  });

  const floorSum = provisional.reduce((sum, entry) => sum + entry.floor, 0);
  let remaining = normalizedTarget - floorSum;

  provisional.sort((a, b) => {
    if (b.fraction !== a.fraction) {
      return b.fraction - a.fraction;
    }

    return a.index - b.index;
  });

  for (const row of provisional) {
    if (remaining <= 0) {
      break;
    }
    row.floor += 1;
    remaining -= 1;
  }

  provisional.sort((a, b) => a.index - b.index);
  return provisional
    .filter((entry) => entry.floor > 0)
    .map((entry) => ({ code: entry.code, target: entry.floor }));
};
