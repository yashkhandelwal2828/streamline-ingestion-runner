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

const { buildDisplaySynopsis, parseOmdbPlot } = loadTsModule('src/services/movieSynopsis.ts');

test('buildDisplaySynopsis prefers longer OMDb plot', () => {
  const result = buildDisplaySynopsis({
    overview: 'Short blurb.',
    plotFull: 'A much longer plot that explains the full story arc and character motivations in detail.',
  });
  assert.equal(result.synopsisSource, 'omdb');
  assert.ok(result.synopsis.length > 20);
});

test('buildDisplaySynopsis falls back to overview when plot missing', () => {
  const result = buildDisplaySynopsis({ overview: 'TMDB only.', plotFull: null });
  assert.equal(result.synopsisSource, 'tmdb');
  assert.equal(result.synopsis, 'TMDB only.');
});

test('parseOmdbPlot rejects N/A', () => {
  assert.equal(parseOmdbPlot('N/A'), null);
});

test('parseOmdbPlot rejects empty and nullish values', () => {
  assert.equal(parseOmdbPlot(''), null);
  assert.equal(parseOmdbPlot('   '), null);
  assert.equal(parseOmdbPlot(null), null);
});

test('parseOmdbPlot accepts valid plot text', () => {
  assert.equal(parseOmdbPlot('  A full story.  '), 'A full story.');
});
