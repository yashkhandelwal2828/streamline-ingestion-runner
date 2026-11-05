const fs = require('node:fs');
const path = require('node:path');
const ts = require('typescript');

/** Loads a TypeScript source file (and its relative imports) for node:test without a build step. */
const loadTsModule = (relativePath) => {
  const filename = path.join(__dirname, '..', '..', relativePath);
  const source = fs.readFileSync(filename, 'utf8');
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020, esModuleInterop: true },
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
  new Function('module', 'exports', 'require', outputText)(module, module.exports, localRequire);
  return module.exports;
};

module.exports = { loadTsModule };
