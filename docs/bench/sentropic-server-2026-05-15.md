# Sentropic full-server boot POC (Sprint 19.C) — 2026-05-15

Pre-replayed 22/27 migrations against opendb-node, then spawned the actual sentropic API server (`tsx src/index.ts`) pointing its DATABASE_URL at opendb.

## Verdict

**boot CRASHED** — sentropic process exited before logging `API server listening`. First 4000 chars of logs:

```
npm warn Unknown builtin config "globalignorefile". This will stop working in the next major version of npm. See `npm help npmrc` for supported config options.
npm warn Unknown env config "globalignorefile". This will stop working in the next major version of npm. See `npm help npmrc` for supported config options.

node:internal/modules/run_main:123
    triggerUncaughtException(
    ^
Error [ERR_MODULE_NOT_FOUND]: Cannot find package 'drizzle-orm' imported from /home/antoinefa/src/sentropic/api/src/index.ts
    at Object.getPackageJSONURL (node:internal/modules/package_json_reader:314:9)
    at packageResolve (node:internal/modules/esm/resolve:768:81)
    at moduleResolve (node:internal/modules/esm/resolve:855:18)
    at defaultResolve (node:internal/modules/esm/resolve:985:11)
    at nextResolve (node:internal/modules/esm/hooks:748:28)
    at resolveBase (file:///home/antoinefa/.npm/_npx/fd45a72a545557e9/node_modules/tsx/dist/register-lJYvHe5s.mjs:2:6726)
    at resolveDirectory (file:///home/antoinefa/.npm/_npx/fd45a72a545557e9/node_modules/tsx/dist/register-lJYvHe5s.mjs:2:7813)
    at resolveTsPaths (file:///home/antoinefa/.npm/_npx/fd45a72a545557e9/node_modules/tsx/dist/register-lJYvHe5s.mjs:2:9255)
    at resolve2 (file:///home/antoinefa/.npm/_npx/fd45a72a545557e9/node_modules/tsx/dist/register-lJYvHe5s.mjs:2:10171)
    at nextResolve (node:internal/modules/esm/hooks:748:28) {
  code: 'ERR_MODULE_NOT_FOUND'
}

Node.js v22.22.2

```
