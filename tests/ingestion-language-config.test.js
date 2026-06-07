const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

const loadTsModule = (relativePath) => {
  const filename = path.join(__dirname, '..', relativePath);
  const source = fs.readFileSync(filename, 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      esModuleInterop: true,
    },
    fileName: filename,
  });

  const module = { exports: {} };
  const localRequire = (request) => {
    if (request.startsWith('.')) {
      const resolved = path.join(path.dirname(relativePath), request);
      return loadTsModule(path.extname(resolved) ? resolved : `${resolved}.ts`);
    }

    return require(request);
  };

  const factory = new Function('module', 'exports', 'require', outputText);
  factory(module, module.exports, localRequire);
  return module.exports;
};

const {
  getIngestionLanguageMode,
  getIngestionLanguageProfile,
  getIngestionLanguageWeights,
  buildLanguageTargets,
} = loadTsModule('src/services/ingestion/languageConfig.ts');

test('language mode defaults to weighted and can be switched off', () => {
  assert.equal(getIngestionLanguageMode({}), 'weighted');
  assert.equal(getIngestionLanguageMode({ INGESTION_LANGUAGE_MODE: 'off' }), 'off');
});

test('explicit language weights override profile defaults', () => {
  const weights = getIngestionLanguageWeights({
    INGESTION_LANGUAGE_MODE: 'weighted',
    INGESTION_LANGUAGE_PROFILE: 'english_first',
    INGESTION_LANGUAGE_WEIGHTS: 'hi:3,en:2,ta:1',
  });

  assert.deepEqual(weights, [
    { code: 'hi', weight: 3 },
    { code: 'en', weight: 2 },
    { code: 'ta', weight: 1 },
  ]);
});

test('unknown profile falls back to india_first', () => {
  assert.equal(getIngestionLanguageProfile({ INGESTION_LANGUAGE_PROFILE: 'unknown' }), 'india_first');
  const weights = getIngestionLanguageWeights({ INGESTION_LANGUAGE_PROFILE: 'unknown' });
  assert.equal(weights[0].code, 'hi');
});

test('language target allocation respects requested total', () => {
  const targets = buildLanguageTargets(7, [
    { code: 'hi', weight: 5 },
    { code: 'en', weight: 2 },
  ]);

  assert.deepEqual(targets, [
    { code: 'hi', target: 5 },
    { code: 'en', target: 2 },
  ]);
  assert.equal(targets.reduce((sum, row) => sum + row.target, 0), 7);
});
