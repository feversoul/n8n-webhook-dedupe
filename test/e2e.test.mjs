// End-to-end tests: the real workflows on a real n8n (test/stack.sh or
// npm run test:e2e), called over HTTP the way a webhook sender would.
//
// Part 1 measures the six claim methods of the lab workflow under concurrent
// duplicates. Methods that hold are asserted. Methods that race are measured
// and printed, not asserted, because how often they race depends on timing.
// Part 2 tests the two receivers, including the conflict path.

import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';

const BASE = process.env.N8N_URL ?? 'http://localhost:55681';
const KEY = process.env.WEBHOOK_KEY ?? 'test-webhook-key';
const SINK = process.env.DOWNSTREAM_URL ?? 'http://localhost:58083';
const BACKEND = process.env.BACKEND ?? 'n8n';
const LEASE_MS = Number(process.env.LEASE_SECONDS ?? 4) * 1000;
const RUN = Date.now().toString(36);

const ROUNDS = 15;
const COPIES = 40;

async function call(path, body, key = KEY) {
  const response = await fetch(`${BASE}/webhook/dedupe/${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(key ? { 'x-webhook-key': key } : {}) },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* not JSON */
  }
  return { status: response.status, json, text };
}

const sink = {
  calls: async (id) => (await fetch(`${SINK}/calls?key=${encodeURIComponent(`demo:${id}`)}`)).json(),
  control: (id, rule) => fetch(`${SINK}/control`, { method: 'POST', body: JSON.stringify({ key: `demo:${id}`, ...rule }) }),
};

const times = (n, make) => Promise.all(Array.from({ length: n }, (_, index) => make(index)));

async function until(check, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error('timed out waiting');
    await sleep(500);
  }
}

before(async () => {
  await until(async () => {
    try {
      return (await call('lab/rows', { method: 'ready', event_key: 'ready' })).status === 200;
    } catch {
      return false;
    }
  }, 240_000);
});

// --- Part 1: the lab ----------------------------------------------------------

const HOLDS = ['dt-insert-then-first', 'pg-on-conflict', 'pg-unique-error'];
const MEASURED = ['dt-check-then-insert', 'dt-upsert', 'remove-duplicates'];
const report = [];

// One event delivered COPIES times at the same instant, ROUNDS times.
async function sameEvent(method) {
  const through = [];
  let errors = 0;
  let rows = 0;
  for (let round = 0; round < ROUNDS; round++) {
    const event_key = `${RUN}-same-${round}`;
    const answers = await times(COPIES, () => call(`lab/${method}`, { event_key }));
    errors += answers.filter((a) => a.status !== 200).length;
    through.push(answers.filter((a) => a.json?.claimed === true).length);
    if (method.startsWith('dt-')) rows = Math.max(rows, (await call('lab/rows', { method, event_key })).json.rows);
  }
  return { through, errors, rows };
}

// COPIES different events at the same instant, then each one delivered again.
// The second delivery of every event must be refused.
async function differentEvents(method) {
  const keys = Array.from({ length: COPIES }, (_, index) => `${RUN}-diff-${index}`);
  const first = await Promise.all(keys.map((event_key) => call(`lab/${method}`, { event_key })));
  let again = 0;
  for (const event_key of keys) {
    if ((await call(`lab/${method}`, { event_key })).json?.claimed === true) again += 1;
  }
  return { first: first.filter((a) => a.json?.claimed === true).length, errors: first.filter((a) => a.status !== 200).length, again };
}

for (const method of [...MEASURED, ...HOLDS]) {
  const holds = HOLDS.includes(method);
  test(`${BACKEND} lab: ${method} ${holds ? 'lets exactly one through' : '(measured, not asserted)'}`, async (t) => {
    const same = await sameEvent(method);
    const different = await differentEvents(method);
    const worst = Math.max(...same.through);
    const racedRounds = same.through.filter((n) => n !== 1).length;
    report.push({ method, same, different, worst, racedRounds });
    t.diagnostic(`${COPIES} copies at once, let through per round: ${same.through.join(' ')}`);
    t.diagnostic(`${COPIES} different events at once: ${different.first} let through, ${different.again} let through again on a second delivery`);
    if (same.errors + different.errors) t.diagnostic(`calls answered with an error: ${same.errors + different.errors}`);
    if (holds) {
      assert.equal(same.errors + different.errors, 0, 'every call answers 200');
      assert.deepEqual(same.through, Array(ROUNDS).fill(1), 'exactly one copy is let through in every round');
      assert.equal(different.first, COPIES, 'every different event is let through once');
      assert.equal(different.again, 0, 'no event is let through a second time');
    }
  });
}

after(() => {
  if (report.length === 0) return;
  console.log(`\n${BACKEND}: ${COPIES} copies of one event at once, ${ROUNDS} rounds`);
  for (const { method, same, different, worst, racedRounds } of report) {
    const total = same.through.reduce((sum, n) => sum + n, 0);
    console.log(
      `  ${method.padEnd(22)} rounds with more than one let through: ${String(racedRounds).padStart(2)}/${ROUNDS}` +
        `  worst round: ${String(worst).padStart(2)}/${COPIES}  total let through: ${String(total).padStart(3)} (should be ${ROUNDS})` +
        `  errors: ${same.errors + different.errors}  second deliveries let through: ${different.again}/${COPIES}` +
        (['dt-check-then-insert', 'dt-upsert'].includes(method) ? `  most rows for one key: ${same.rows}` : ''),
    );
  }
});

// --- Part 2: the receivers ----------------------------------------------------

for (const receiver of ['postgres', 'data-table']) {
  const name = `${BACKEND} receiver ${receiver}`;
  const deliver = (id, extra = {}) => call(receiver, { id, ...extra });
  const id = (label) => `${RUN}-${receiver}-${label}`;
  const processed = (answers) => answers.filter((a) => a.status === 200 && a.json?.duplicate === false);
  // Every answer is one of the documented ones.
  const known = (a) =>
    (a.status === 200 && a.json?.status === 'done') ||
    (a.status === 409 && a.json?.duplicate === true && ['processing', 'superseded'].includes(a.json?.status)) ||
    (a.status === 500 && a.json?.status === 'failed');

  test(`${name}: refuses a call without the key and an event without an id`, async () => {
    assert.equal((await call(receiver, { id: id('nokey') }, null)).status, 403);
    const answer = await call(receiver, { type: 'order.paid' });
    assert.equal(answer.status, 400);
    assert.equal(answer.json.error, 'missing event id');
    assert.equal((await sink.calls(id('nokey'))).calls, 0);
  });

  test(`${name}: ${COPIES} copies of one event at once, the work runs once (10 rounds)`, async () => {
    for (let round = 0; round < 10; round++) {
      const event = id(`same-${round}`);
      const answers = await times(COPIES, () => deliver(event, { amount: 10 }));
      assert.ok(answers.every(known), `unexpected answer: ${JSON.stringify(answers.find((a) => !known(a)))}`);
      assert.equal(processed(answers).length, 1, 'exactly one delivery is processed');
      assert.deepEqual(await sink.calls(event), { calls: 1, ok: 1 }, 'the downstream call was made once');
      // The conflict path: once the event is done, a duplicate gets 200.
      const late = await deliver(event);
      assert.deepEqual([late.status, late.json], [200, { ok: true, duplicate: true, status: 'done' }]);
      assert.equal((await sink.calls(event)).calls, 1);
    }
  });

  test(`${name}: 25 different events, 4 copies each, all at once: every event once`, async () => {
    const events = Array.from({ length: 25 }, (_, index) => id(`mixed-${index}`));
    const answers = await Promise.all(events.flatMap((event) => [0, 1, 2, 3].map(() => deliver(event))));
    assert.ok(answers.every(known));
    assert.equal(processed(answers).length, 25);
    for (const event of events) assert.deepEqual(await sink.calls(event), { calls: 1, ok: 1 });
  });

  test(`${name}: a duplicate that arrives while the first is still running gets 409`, async () => {
    const event = id('running');
    await sink.control(event, { delay_ms: 2000 });
    const first = deliver(event);
    await sleep(700);
    const second = await deliver(event);
    assert.deepEqual([second.status, second.json], [409, { ok: false, duplicate: true, status: 'processing' }]);
    assert.equal((await first).status, 200);
    assert.deepEqual(await sink.calls(event), { calls: 1, ok: 1 });
  });

  test(`${name}: failed work answers 500 and frees the claim, 20 retries at once run it once more`, async () => {
    const event = id('fails');
    await sink.control(event, { fail: 1 });
    const first = await deliver(event);
    assert.deepEqual([first.status, first.json], [500, { ok: false, duplicate: false, status: 'failed' }]);
    const retries = await times(20, () => deliver(event));
    assert.ok(retries.every(known));
    assert.equal(processed(retries).length, 1, 'exactly one retry is processed');
    assert.deepEqual(await sink.calls(event), { calls: 2, ok: 1 });
    assert.equal((await deliver(event)).json.status, 'done');
    assert.equal((await sink.calls(event)).calls, 2);
  });

  test(`${name}: a run slower than the lease is taken over once, and may not mark the event done`, async () => {
    const event = id('slow');
    await sink.control(event, { delay_ms: LEASE_MS + 5000 });
    const slow = deliver(event);
    await sleep(1000);
    assert.equal((await deliver(event)).status, 409, 'inside the lease the claim holds');
    await sleep(LEASE_MS + 500);
    await sink.control(event, {});
    const retries = await times(20, () => deliver(event));
    assert.ok(retries.every(known));
    assert.equal(processed(retries).length, 1, 'exactly one retry takes the stale claim');
    const late = await slow;
    assert.deepEqual([late.status, late.json], [409, { ok: false, duplicate: true, status: 'superseded' }]);
    // Both runs reached the downstream call, with the same Idempotency-Key.
    assert.deepEqual(await sink.calls(event), { calls: 2, ok: 2 });
    assert.equal((await deliver(event)).json.status, 'done');
  });
}
