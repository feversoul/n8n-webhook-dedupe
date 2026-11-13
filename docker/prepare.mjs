// Used by bootstrap.sh inside the n8n container: writes the import files for
// the n8n CLI from the environment, so no secret is stored in the repository.
//   node prepare.mjs <workflows dir> <output dir>

import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const [source, out] = process.argv.slice(2);
const env = process.env;

const credentials = [
  {
    id: 'webhookDedupeKey',
    name: 'Webhook Dedupe Key',
    type: 'httpHeaderAuth',
    data: { name: 'X-Webhook-Key', value: env.WEBHOOK_KEY },
  },
  {
    id: 'webhookDedupePg',
    name: 'Webhook Dedupe Postgres',
    type: 'postgres',
    data: {
      host: env.CLAIMS_DB_HOST || 'postgres',
      port: Number(env.CLAIMS_DB_PORT || 5432),
      database: env.CLAIMS_DB_NAME || 'dedupe',
      user: env.CLAIMS_DB_USER || 'dedupe',
      password: env.CLAIMS_DB_PASSWORD,
      ssl: 'disable',
    },
  },
];

mkdirSync(join(out, 'workflows'), { recursive: true });
writeFileSync(join(out, 'credentials.json'), JSON.stringify(credentials));

const lease = Number(env.LEASE_SECONDS || 0);

for (const file of readdirSync(source).filter((name) => name.endsWith('.json'))) {
  const workflow = JSON.parse(readFileSync(join(source, file), 'utf8'));
  for (const node of workflow.nodes) {
    // Where "Do the work" posts.
    if (node.name === 'Do the work' && env.WORK_URL) node.parameters.url = env.WORK_URL;
    // Optional: a shorter lease, for the tests.
    if (node.name === 'Event' && lease > 0) {
      node.parameters.jsCode = node.parameters.jsCode.replace(/const LEASE_SECONDS = \d+;/, `const LEASE_SECONDS = ${lease};`);
    }
  }
  writeFileSync(join(out, 'workflows', file), JSON.stringify(workflow));
}
