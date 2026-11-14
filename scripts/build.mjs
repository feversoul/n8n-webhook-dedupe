// Builds workflows/*.json.
//
// The workflows are plain n8n exports: import them and they run, no build
// needed. This script exists so the repeated parts are written once (the lab
// has six endpoints of the same shape, and the two receivers share most of
// their nodes). After changing it, run:  node scripts/build.mjs

import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

// Credentials are matched by name on import.
const WEBHOOK_KEY = { httpHeaderAuth: { id: 'webhookDedupeKey', name: 'Webhook Dedupe Key' } };
const POSTGRES = { postgres: { id: 'webhookDedupePg', name: 'Webhook Dedupe Postgres' } };

// The call the receivers make once per event. Replace it with your own.
const WORK_URL = 'https://example.com/replace-with-your-downstream-call';

const CLAIMS = 'webhook_claims';
const CLAIM_COLUMNS = [
  ['event_key', 'string'],
  ['owner', 'string'], // the run token of the execution that holds the claim
  ['status', 'string'], // processing | done | failed
  ['claimed_at', 'date'],
];

const LAB = 'dedupe_lab';
const LAB_COLUMNS = [
  ['method', 'string'],
  ['event_key', 'string'],
  ['owner', 'string'],
];

const uuid = (seed) => {
  const hex = createHash('sha1').update(seed).digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
};

const tableRef = (name) => ({ __rl: true, mode: 'name', value: name });
const conditions = (list) => ({
  matchType: 'allConditions',
  filters: { conditions: list.map(([keyName, keyValue]) => ({ keyName, condition: 'eq', keyValue })) },
});
const mapped = (columns, values) => ({
  mappingMode: 'defineBelow',
  value: values,
  matchingColumns: [],
  schema: columns
    .filter(([column]) => column in values)
    .map(([column, type]) => ({
      id: column,
      displayName: column,
      required: false,
      defaultMatch: false,
      display: true,
      type: type === 'date' ? 'dateTime' : type,
      canBeUsedToMatch: true,
    })),
  attemptToConvertTypes: false,
  convertFieldsToString: false,
});

function workflow(id, name, build) {
  const nodes = [];
  const connections = {};
  const api = {
    node(nodeName, type, typeVersion, position, parameters, extra = {}) {
      nodes.push({ parameters, id: uuid(`${id}/${nodeName}`), name: nodeName, type: `n8n-nodes-base.${type}`, typeVersion, position, ...extra });
      return nodeName;
    },
    note(nodeName, lines, position, width, height, color = 4) {
      return api.node(nodeName, 'stickyNote', 1, position, { content: lines.join('\n'), width, height, color });
    },
    link(from, to, output = 0) {
      connections[from] ??= { main: [] };
      while (connections[from].main.length <= output) connections[from].main.push([]);
      connections[from].main[output].push({ node: to, type: 'main', index: 0 });
    },
    // a -> b -> c ...
    chain(...names) {
      for (let i = 1; i < names.length; i++) api.link(names[i - 1], names[i]);
    },
    hook(nodeName, path, position) {
      return api.node(
        nodeName,
        'webhook',
        2,
        position,
        { httpMethod: 'POST', path: `dedupe/${path}`, authentication: 'headerAuth', responseMode: 'responseNode', options: {} },
        { webhookId: uuid(`${id}/webhook/${path}`), credentials: WEBHOOK_KEY },
      );
    },
    // An IF node on one JavaScript condition. Output 0 is true, 1 is false.
    when(nodeName, position, expression) {
      return api.node(nodeName, 'if', 2.2, position, {
        conditions: {
          options: { caseSensitive: true, leftValue: '', typeValidation: 'loose', version: 2 },
          conditions: [
            {
              id: uuid(`${id}/${nodeName}/condition`),
              leftValue: `={{ ${expression} }}`,
              rightValue: '',
              operator: { type: 'boolean', operation: 'true', singleValue: true },
            },
          ],
          combinator: 'and',
        },
        options: { looseTypeValidation: true },
      });
    },
    answer(nodeName, position, code, body) {
      return api.node(nodeName, 'respondToWebhook', 1.1, position, {
        respondWith: 'json',
        responseBody: `={{ ${body} }}`,
        options: { responseCode: code },
      });
    },
    // Data Table row operation. It always passes one item on (an empty one
    // when nothing came back), so the next node can tell the cases apart.
    rows(nodeName, position, table, operation, parameters) {
      return api.node(
        nodeName,
        'dataTable',
        1.1,
        position,
        { resource: 'row', operation, dataTableId: tableRef(table), ...parameters },
        { alwaysOutputData: true },
      );
    },
    sql(nodeName, position, query, replacement, extra = {}) {
      return api.node(
        nodeName,
        'postgres',
        2.6,
        position,
        { operation: 'executeQuery', query: query.join('\n'), options: replacement ? { queryReplacement: `={{ ${replacement} }}` } : {} },
        { alwaysOutputData: true, credentials: POSTGRES, ...extra },
      );
    },
    createTable(nodeName, position, table, columns) {
      return api.node(nodeName, 'dataTable', 1.1, position, {
        resource: 'table',
        operation: 'create',
        tableName: table,
        columns: { column: columns.map(([name, type]) => ({ name, type })) },
        options: { createIfNotExists: true },
      });
    },
  };
  build(api);
  return { name, nodes, pinData: {}, connections, active: false, settings: { executionOrder: 'v1' }, id };
}

// --- 0. Setup ----------------------------------------------------------------

const setupTables = workflow('whDedupeSetupDt1', 'Webhook Dedupe · 0 Setup (Data Tables)', (w) => {
  w.note(
    'About',
    [
      '## Webhook Dedupe · Setup (Data Tables)',
      `Run this once: open it and press **Execute workflow**. It creates the data table \`${CLAIMS}\` for the Data Table receiver and \`${LAB}\` for the lab. Running it again changes nothing.`,
      '',
      'The webhook does the same for scripted installs (`POST dedupe/setup/data-tables`). If you delete it, delete **Done** too: that node only answers the webhook, and without the webhook a run ends on it with "No Webhook node found".',
    ],
    [-40, -320],
    560,
    260,
  );
  w.node('Run once', 'manualTrigger', 1, [0, 0], {});
  w.hook('Or call', 'setup/data-tables', [0, 180]);
  w.createTable('Create claims table', [240, 0], CLAIMS, CLAIM_COLUMNS);
  w.createTable('Create lab table', [480, 0], LAB, LAB_COLUMNS);
  w.answer('Done', [720, 0], 200, '{ ok: true }');
  w.link('Run once', 'Create claims table');
  w.chain('Or call', 'Create claims table', 'Create lab table', 'Done');
});

const setupPostgres = workflow('whDedupeSetupPg1', 'Webhook Dedupe · 0 Setup (Postgres)', (w) => {
  w.note(
    'About',
    [
      '## Webhook Dedupe · Setup (Postgres)',
      'Run this once: open it and press **Execute workflow**. It creates the table `webhook_claims` for the Postgres receiver and `dedupe_lab_claims` for the lab, in the database of the **Webhook Dedupe Postgres** credential. Running it again changes nothing.',
      '',
      'The primary key on `event_key` is the whole point: it is what makes the claim atomic.',
    ],
    [-40, -260],
    560,
    200,
  );
  w.node('Run once', 'manualTrigger', 1, [0, 0], {});
  w.hook('Or call', 'setup/postgres', [0, 180]);
  w.sql('Create tables', [240, 0], [
    'CREATE TABLE IF NOT EXISTS webhook_claims (',
    '  event_key   text PRIMARY KEY,',
    "  status      text NOT NULL DEFAULT 'processing',",
    '  owner       text NOT NULL,',
    '  attempts    integer NOT NULL DEFAULT 1,',
    '  claimed_at  timestamptz NOT NULL DEFAULT now(),',
    '  finished_at timestamptz',
    ');',
    'CREATE TABLE IF NOT EXISTS dedupe_lab_claims (',
    '  method    text NOT NULL,',
    '  event_key text NOT NULL,',
    '  owner     text NOT NULL,',
    '  PRIMARY KEY (method, event_key)',
    ');',
  ]);
  w.answer('Done', [480, 0], 200, '{ ok: true }');
  w.link('Run once', 'Create tables');
  w.chain('Or call', 'Create tables', 'Done');
});

// --- Receivers ---------------------------------------------------------------

// Shared first nodes: read the event, refuse one without an id.
function receiverStart(w, path) {
  w.hook('Webhook', path, [0, 0]);
  w.node('Event', 'code', 2, [220, 0], {
    jsCode: [
      '// Settings.',
      "const SOURCE = 'demo'; // scope of the key: one value per sender, so ids of two senders cannot collide",
      'const LEASE_SECONDS = 900; // a claim still "processing" after this long may be taken over. Set it longer than your slowest run.',
      '',
      'const body = $input.first().json.body ?? {};',
      '// Where the sender puts its event id. Change this line for your sender.',
      "const id = body.id === undefined || body.id === null ? '' : String(body.id).trim();",
      "const hex = () => Math.floor(Math.random() * 0x100000000).toString(16).padStart(8, '0');",
      'return [',
      '  {',
      '    json: {',
      "      event_key: id ? `${SOURCE}:${id}` : '',",
      '      // The run token. Every later write checks it, so a run that lost its',
      '      // claim cannot mark the event done.',
      '      owner: `${$execution.id}-${hex()}${hex()}`,',
      '      lease_seconds: LEASE_SECONDS,',
      '      event: body,',
      '    },',
      '  },',
      '];',
    ].join('\n'),
  });
  w.when('Has an id?', [440, 0], '!!$json.event_key');
  w.answer('No id: refuse', [660, 200], 400, "{ ok: false, error: 'missing event id' }");
  w.chain('Webhook', 'Event', 'Has an id?');
  w.link('Has an id?', 'No id: refuse', 1);
}

// Shared last nodes: the downstream call and the answers.
function receiverWork(w, x, y) {
  w.node(
    'Do the work',
    'httpRequest',
    4.2,
    [x, y],
    {
      method: 'POST',
      url: WORK_URL,
      sendHeaders: true,
      headerParameters: { parameters: [{ name: 'Idempotency-Key', value: "={{ $('Event').first().json.event_key }}" }] },
      sendBody: true,
      specifyBody: 'json',
      jsonBody: "={{ JSON.stringify($('Event').first().json.event) }}",
      options: { response: { response: { fullResponse: true, neverError: true } }, timeout: 60000 },
    },
    { onError: 'continueRegularOutput' },
  );
  w.when('Worked?', [x + 220, y], '$json.statusCode >= 200 && $json.statusCode < 300');
  w.answer('Processed', [x + 880, y - 100], 200, "{ ok: true, duplicate: false, status: 'done' }");
  w.answer('Superseded', [x + 880, y + 60], 409, "{ ok: false, duplicate: true, status: 'superseded' }");
  w.answer('Failed: sender retries', [x + 660, y + 220], 500, "{ ok: false, duplicate: false, status: 'failed' }");
  w.answer('Duplicate: done', [x + 880, y + 420], 200, "{ ok: true, duplicate: true, status: 'done' }");
  w.answer('Duplicate: in progress', [x + 880, y + 580], 409, "{ ok: false, duplicate: true, status: 'processing' }");
  w.link('Do the work', 'Worked?');
}

const CONFLICT_NOTE = [
  '### The conflict path',
  '- **Duplicate: done** answers `200`. The event was handled, the sender can stop.',
  '- **Duplicate: in progress** answers `409`. Another run holds the claim and has not finished. A `2xx` here would tell the sender to stop while that run can still crash. With `409` it retries later and then finds the event done, failed or stale.',
  '- **Failed** answers `500` and frees the claim, so the next delivery runs again.',
  '- **Superseded**: this run was so slow that a retry took the claim over. It may not mark the event done.',
  '',
  'If your sender gives up after a few non-2xx answers, change `409` to `200` and accept that a crash in that moment needs a manual replay.',
];

const receiverPostgres = workflow('whDedupeRecvPg01', 'Webhook Dedupe · 1 Receiver (Postgres)', (w) => {
  w.note(
    'About',
    [
      '## Webhook Dedupe · Receiver on Postgres',
      '`POST dedupe/postgres` with a JSON body that has an `id`. The event is handled once, however many times and however close together it is delivered.',
      '',
      '**Claim** is one statement on a table with `event_key` as primary key. Exactly one run gets its own token back. A claim that failed, or that has been `processing` longer than the lease, can be taken by the next delivery in the same statement.',
      '',
      '**Mark done** and **Mark failed** only change the row while it still carries this run\'s token.',
      '',
      'Settings are at the top of the **Event** node. Put your own call in **Do the work**; it gets the event key as `Idempotency-Key`.',
    ],
    [-40, -400],
    640,
    340,
  );
  w.note('Conflict', CONFLICT_NOTE, [1540, -400], 640, 300, 5);
  receiverStart(w, 'postgres');
  w.sql(
    'Claim',
    [660, 0],
    [
      'INSERT INTO webhook_claims (event_key, status, owner, claimed_at)',
      "VALUES ($1, 'processing', $2, now())",
      'ON CONFLICT (event_key) DO UPDATE',
      "  SET status = 'processing', owner = EXCLUDED.owner, claimed_at = now(), attempts = webhook_claims.attempts + 1",
      "  WHERE webhook_claims.status = 'failed'",
      "     OR (webhook_claims.status = 'processing'",
      '         AND webhook_claims.claimed_at < now() - make_interval(secs => $3::int))',
      'RETURNING owner, attempts;',
    ],
    '[$json.event_key, $json.owner, $json.lease_seconds]',
  );
  w.when('Mine?', [880, 0], "($json.owner ?? '') === $('Event').first().json.owner");
  receiverWork(w, 1100, 0);
  w.sql(
    'Mark done',
    [1540, -100],
    ["UPDATE webhook_claims SET status = 'done', finished_at = now()", "WHERE event_key = $1 AND owner = $2 AND status = 'processing'", 'RETURNING owner;'],
    "[$('Event').first().json.event_key, $('Event').first().json.owner]",
  );
  w.when('Still mine?', [1760, -100], "($json.owner ?? '') === $('Event').first().json.owner");
  w.sql(
    'Mark failed',
    [1540, 220],
    ["UPDATE webhook_claims SET status = 'failed', finished_at = now()", "WHERE event_key = $1 AND owner = $2 AND status = 'processing'", 'RETURNING owner;'],
    "[$('Event').first().json.event_key, $('Event').first().json.owner]",
  );
  w.sql('Current state', [1100, 500], ['SELECT status FROM webhook_claims WHERE event_key = $1;'], "[$('Event').first().json.event_key]");
  w.when('Already done?', [1320, 500], "$json.status === 'done'");

  w.link('Has an id?', 'Claim', 0);
  w.link('Claim', 'Mine?');
  w.link('Mine?', 'Do the work', 0);
  w.link('Mine?', 'Current state', 1);
  w.link('Worked?', 'Mark done', 0);
  w.link('Worked?', 'Mark failed', 1);
  w.link('Mark done', 'Still mine?');
  w.link('Still mine?', 'Processed', 0);
  w.link('Still mine?', 'Superseded', 1);
  w.link('Mark failed', 'Failed: sender retries');
  w.link('Current state', 'Already done?');
  w.link('Already done?', 'Duplicate: done', 0);
  w.link('Already done?', 'Duplicate: in progress', 1);
});

const receiverTable = workflow('whDedupeRecvDt01', 'Webhook Dedupe · 2 Receiver (Data Tables)', (w) => {
  const me = "$('Event').first().json.owner";
  const first = "$('First row for this key').first().json";
  w.note(
    'About',
    [
      '## Webhook Dedupe · Receiver on Data Tables',
      '`POST dedupe/data-table` with a JSON body that has an `id`. Same behaviour as the Postgres receiver, with no database of your own.',
      '',
      'A Data Table has no unique constraint, so "look, then insert" lets several runs through. This one turns it around: **insert first, then look**. Every run adds its own row, then reads the row with the lowest `id` for the key. The run whose row that is has the claim. The others delete their row again.',
      '',
      'The first row stays as the record of the event. A failed or stale claim is taken over with one conditional update on that row, decided by the `owner` in the row that comes back.',
      '',
      'Settings are at the top of the **Event** node. Put your own call in **Do the work**.',
    ],
    [-40, -440],
    640,
    380,
  );
  w.note('Conflict', CONFLICT_NOTE, [1980, -440], 640, 300, 5);
  receiverStart(w, 'data-table');
  w.rows('Add my row', [660, 0], CLAIMS, 'insert', {
    columns: mapped(CLAIM_COLUMNS, { event_key: '={{ $json.event_key }}', owner: '={{ $json.owner }}', status: 'processing', claimed_at: '={{ $now.toISO() }}' }),
    options: {},
  });
  w.rows('First row for this key', [880, 0], CLAIMS, 'get', {
    ...conditions([['event_key', "={{ $('Event').first().json.event_key }}"]]),
    returnAll: false,
    limit: 1,
    orderBy: true,
    orderByColumn: 'id',
    orderByDirection: 'ASC',
  });
  w.when('Am I first?', [1100, 0], "$json.id === $('Add my row').first().json.id");
  w.node('Claimed', 'set', 3.4, [1540, 0], {
    assignments: { assignments: [{ id: uuid('claimed/claim_id'), name: 'claim_id', value: '={{ $json.id }}', type: 'number' }] },
    options: {},
  });
  receiverWork(w, 1760, 0);

  // Not first: remove my row, then see whether the first row can be taken over.
  w.rows('Remove my row', [1320, 400], CLAIMS, 'deleteRows', {
    ...conditions([['id', "={{ $('Add my row').first().json.id }}"]]),
    options: {},
  });
  w.when(
    'Failed or stale?',
    [1540, 400],
    `${first}.status === 'failed' || (${first}.status === 'processing' && new Date(${first}.claimed_at).getTime() < Date.now() - $('Event').first().json.lease_seconds * 1000)`,
  );
  w.rows('Take over', [1760, 300], CLAIMS, 'update', {
    ...conditions([
      ['id', `={{ ${first}.id }}`],
      ['owner', `={{ ${first}.owner }}`],
      ['status', `={{ ${first}.status }}`],
    ]),
    columns: mapped(CLAIM_COLUMNS, { owner: `={{ ${me} }}`, status: 'processing', claimed_at: '={{ $now.toISO() }}' }),
    options: {},
  });
  w.when('Took it?', [1980, 300], `($json.owner ?? '') === ${me}`);
  w.when('Already done?', [1980, 500], `${first}.status === 'done'`);

  const mark = (name, position, status) =>
    w.rows(name, position, CLAIMS, 'update', {
      ...conditions([
        ['id', "={{ $('Claimed').first().json.claim_id }}"],
        ['owner', `={{ ${me} }}`],
        ['status', 'processing'],
      ]),
      columns: mapped(CLAIM_COLUMNS, { status }),
      options: {},
    });
  mark('Mark done', [2200, -100], 'done');
  // Decide by the row that came back, not by how many rows came back.
  w.when('Still mine?', [2420, -100], `($json.owner ?? '') === ${me} && $json.status === 'done'`);
  mark('Mark failed', [2200, 220], 'failed');

  w.link('Has an id?', 'Add my row', 0);
  w.chain('Add my row', 'First row for this key', 'Am I first?');
  w.link('Am I first?', 'Claimed', 0);
  w.link('Am I first?', 'Remove my row', 1);
  w.link('Claimed', 'Do the work');
  w.link('Remove my row', 'Failed or stale?');
  w.link('Failed or stale?', 'Take over', 0);
  w.link('Failed or stale?', 'Already done?', 1);
  w.link('Take over', 'Took it?');
  w.link('Took it?', 'Claimed', 0);
  w.link('Took it?', 'Duplicate: in progress', 1);
  w.link('Already done?', 'Duplicate: done', 0);
  w.link('Already done?', 'Duplicate: in progress', 1);
  w.link('Worked?', 'Mark done', 0);
  w.link('Worked?', 'Mark failed', 1);
  w.link('Mark done', 'Still mine?');
  w.link('Still mine?', 'Processed', 0);
  w.link('Still mine?', 'Superseded', 1);
  w.link('Mark failed', 'Failed: sender retries');
});

// --- 9. Lab ------------------------------------------------------------------

// Six ways to claim an event, each behind its own webhook and reduced to the
// claim alone. POST { event_key } and the answer says whether this call was
// let through. The tests send the same key many times at once and count.
export const METHODS = [
  ['dt-check-then-insert', 'Data Table: If Row Does Not Exist, then Insert'],
  ['dt-upsert', 'Data Table: Upsert, let through when the row is new'],
  ['dt-insert-then-first', 'Data Table: Insert, then read the first row for the key'],
  ['remove-duplicates', 'Remove Duplicates: Remove Items Processed in Previous Executions'],
  ['pg-on-conflict', 'Postgres: INSERT ... ON CONFLICT DO NOTHING RETURNING'],
  ['pg-unique-error', 'Postgres: plain INSERT, unique violation on the error output'],
];

const lab = workflow('whDedupeLab00001', 'Webhook Dedupe · 9 Lab', (w) => {
  const row = 300;
  w.note(
    'About',
    [
      '## Webhook Dedupe · Lab',
      'Six ways to claim an event, one webhook each: `POST dedupe/lab/<method>` with `{ "event_key": "..." }`. The answer is `{ claimed: true }` when this call was let through.',
      '',
      'This workflow is for measuring, not for production. The tests send one key 40 times at once and count how many calls were let through. Use the two receiver workflows for real traffic.',
      '',
      ...METHODS.map(([path, title]) => `- \`${path}\`: ${title}`),
    ],
    [-40, -420],
    760,
    340,
  );

  METHODS.forEach(([method], index) => {
    const y = index * row;
    const hook = `${method}`;
    const key = `$('${hook}').first().json.body.event_key`;
    const owner = '$execution.id';
    w.hook(hook, `lab/${method}`, [0, y]);
    const yes = w.answer(`${method} · let through`, [1100, y - 70], 200, `{ method: '${method}', event_key: ${key}, claimed: true }`);
    const no = w.answer(`${method} · duplicate`, [1100, y + 90], 200, `{ method: '${method}', event_key: ${key}, claimed: false }`);
    const labRow = { method, event_key: `={{ ${key} }}`, owner: `={{ ${owner} }}` };
    const sameKey = conditions([
      ['method', method],
      ['event_key', `={{ ${key} }}`],
    ]);

    if (method === 'dt-check-then-insert') {
      w.rows(`${method} · not seen?`, [240, y], LAB, 'rowNotExists', sameKey);
      w.when(`${method} · new?`, [460, y], '!!$json.body');
      w.rows(`${method} · save`, [680, y - 70], LAB, 'insert', { columns: mapped(LAB_COLUMNS, labRow), options: {} });
      w.chain(hook, `${method} · not seen?`, `${method} · new?`);
      w.link(`${method} · new?`, `${method} · save`, 0);
      w.link(`${method} · save`, yes);
      w.link(`${method} · new?`, no, 1);
    }
    if (method === 'dt-upsert') {
      w.rows(`${method} · upsert`, [240, y], LAB, 'upsert', { ...sameKey, columns: mapped(LAB_COLUMNS, labRow), options: {} });
      // An upsert cannot say whether it inserted or updated. The closest
      // thing is that a fresh row has the same created and updated time.
      w.when(`${method} · new?`, [460, y], '$json.id !== undefined && new Date($json.createdAt).getTime() === new Date($json.updatedAt).getTime()');
      w.chain(hook, `${method} · upsert`, `${method} · new?`);
      w.link(`${method} · new?`, yes, 0);
      w.link(`${method} · new?`, no, 1);
    }
    if (method === 'dt-insert-then-first') {
      w.rows(`${method} · insert`, [240, y], LAB, 'insert', { columns: mapped(LAB_COLUMNS, labRow), options: {} });
      w.rows(`${method} · first row`, [460, y], LAB, 'get', { ...sameKey, returnAll: false, limit: 1, orderBy: true, orderByColumn: 'id', orderByDirection: 'ASC' });
      w.when(`${method} · mine?`, [680, y], `$json.id === $('${method} · insert').first().json.id`);
      w.chain(hook, `${method} · insert`, `${method} · first row`, `${method} · mine?`);
      w.link(`${method} · mine?`, yes, 0);
      w.link(`${method} · mine?`, no, 1);
    }
    if (method === 'remove-duplicates') {
      w.node(
        `${method} · seen before?`,
        'removeDuplicates',
        2,
        [240, y],
        {
          operation: 'removeItemsSeenInPreviousExecutions',
          logic: 'removeItemsWithAlreadySeenKeyValues',
          dedupeValue: '={{ $json.body.event_key }}',
          options: { scope: 'node', historySize: 1000000 },
        },
        { alwaysOutputData: true },
      );
      w.when(`${method} · new?`, [460, y], '!!$json.body');
      w.chain(hook, `${method} · seen before?`, `${method} · new?`);
      w.link(`${method} · new?`, yes, 0);
      w.link(`${method} · new?`, no, 1);
    }
    if (method === 'pg-on-conflict') {
      w.sql(
        `${method} · claim`,
        [240, y],
        ['INSERT INTO dedupe_lab_claims (method, event_key, owner)', 'VALUES ($1, $2, $3)', 'ON CONFLICT (method, event_key) DO NOTHING', 'RETURNING owner;'],
        `['${method}', $json.body.event_key, String(${owner})]`,
      );
      w.when(`${method} · mine?`, [460, y], `($json.owner ?? '') === String(${owner})`);
      w.chain(hook, `${method} · claim`, `${method} · mine?`);
      w.link(`${method} · mine?`, yes, 0);
      w.link(`${method} · mine?`, no, 1);
    }
    if (method === 'pg-unique-error') {
      w.sql(
        `${method} · insert`,
        [240, y],
        ['INSERT INTO dedupe_lab_claims (method, event_key, owner)', 'VALUES ($1, $2, $3)', 'RETURNING owner;'],
        `['${method}', $json.body.event_key, String(${owner})]`,
        // No "Always Output Data" here: with it, a failed insert would also
        // send an empty item down the success output.
        { onError: 'continueErrorOutput', alwaysOutputData: false },
      );
      // Only a unique violation means "duplicate". Anything else is a fault.
      w.when(`${method} · unique violation?`, [460, y + 90], "/duplicate key|unique constraint/i.test(JSON.stringify($json))");
      w.answer(`${method} · fault`, [1100, y + 250], 500, `{ method: '${method}', error: 'claim failed' }`);
      w.link(hook, `${method} · insert`);
      w.link(`${method} · insert`, yes, 0);
      w.link(`${method} · insert`, `${method} · unique violation?`, 1);
      w.link(`${method} · unique violation?`, no, 0);
      w.link(`${method} · unique violation?`, `${method} · fault`, 1);
    }
  });

  // How many rows a Data Table method left behind for one key.
  const y = METHODS.length * row;
  w.hook('rows', 'lab/rows', [0, y]);
  w.rows('rows · read', [240, y], LAB, 'get', {
    ...conditions([
      ['method', '={{ $json.body.method }}'],
      ['event_key', '={{ $json.body.event_key }}'],
    ]),
    returnAll: true,
  });
  w.node('rows · count', 'code', 2, [460, y], {
    jsCode: 'return [{ json: { rows: $input.all().filter((item) => item.json.id !== undefined).length } }];',
  });
  w.answer('rows · answer', [680, y], 200, '{ rows: $json.rows }');
  w.chain('rows', 'rows · read', 'rows · count', 'rows · answer');
});
lab.settings.saveDataSuccessExecution = 'none';

// --- write -------------------------------------------------------------------

export const workflows = {
  '0-setup-data-tables.json': setupTables,
  '0-setup-postgres.json': setupPostgres,
  '1-receiver-postgres.json': receiverPostgres,
  '2-receiver-data-table.json': receiverTable,
  '9-lab.json': lab,
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const out = join(root, 'workflows');
  mkdirSync(out, { recursive: true });
  for (const [file, definition] of Object.entries(workflows)) {
    writeFileSync(join(out, file), `${JSON.stringify(definition, null, 2)}\n`);
    console.log(`workflows/${file}: ${definition.nodes.length} nodes`);
  }
}
