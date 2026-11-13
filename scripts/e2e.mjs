// npm run test:e2e            n8n on SQLite (its default)
// npm run test:e2e:postgres   n8n on PostgreSQL
// Starts the test stack, runs test/e2e.test.mjs against it and removes the
// stack again. Set KEEP_STACK=1 to leave it running, for example to open the
// n8n editor on http://localhost:55681 and look at the executions.

import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const postgres = process.argv[2] === 'postgres';
const compose = [
  'compose',
  '--env-file',
  'test/test.env',
  '-f',
  'docker-compose.yml',
  '-f',
  'test/docker-compose.yml',
  ...(postgres ? ['-f', 'test/docker-compose.postgres.yml'] : []),
  '-p',
  'dedupe-test',
];
const run = (command, args, env = {}) => spawnSync(command, args, { cwd: root, stdio: 'inherit', env: { ...process.env, ...env } }).status ?? 1;

run('docker', [...compose, 'down', '-v']);
let status = run('docker', [...compose, 'up', '-d']);
if (status === 0) {
  status = run(process.execPath, ['--test', '--test-reporter=spec', 'test/e2e.test.mjs'], { BACKEND: postgres ? 'PostgreSQL' : 'SQLite' });
}
if (status !== 0) run('docker', [...compose, 'logs', '--tail', '60', 'n8n']);
if (!process.env.KEEP_STACK) run('docker', [...compose, 'down', '-v']);
process.exit(status);
