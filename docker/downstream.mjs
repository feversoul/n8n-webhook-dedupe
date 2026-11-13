// Stand-in for the call a receiver makes once per event ("the work").
// It counts the calls per Idempotency-Key so a test, or you, can see how
// often an event really went through.
//
//   POST /work               the call itself; answers 200, or what /control set
//   GET  /calls?key=...      { calls, ok }  calls received and calls answered 200
//   POST /control            { key, fail, delay_ms }  fail the next <fail> calls
//                            for the key with 500, answer after <delay_ms>

import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';

const calls = new Map(); // key -> { calls, ok }
const control = new Map(); // key -> { fail, delay_ms }

const read = async (request) => {
  let text = '';
  for await (const chunk of request) text += chunk;
  try {
    return JSON.parse(text || '{}');
  } catch {
    return {};
  }
};
const send = (response, status, body) => {
  response.writeHead(status, { 'content-type': 'application/json' });
  response.end(JSON.stringify(body));
};

createServer(async (request, response) => {
  const url = new URL(request.url, 'http://downstream');
  if (request.method === 'POST' && url.pathname === '/work') {
    await read(request);
    const key = String(request.headers['idempotency-key'] ?? '');
    const seen = calls.get(key) ?? { calls: 0, ok: 0 };
    calls.set(key, seen);
    seen.calls += 1;
    const rule = control.get(key) ?? {};
    const fail = rule.fail > 0;
    if (fail) rule.fail -= 1;
    if (rule.delay_ms > 0) await sleep(rule.delay_ms);
    if (fail) return send(response, 500, { ok: false });
    seen.ok += 1;
    return send(response, 200, { ok: true, key });
  }
  if (request.method === 'GET' && url.pathname === '/calls') {
    return send(response, 200, calls.get(url.searchParams.get('key') ?? '') ?? { calls: 0, ok: 0 });
  }
  if (request.method === 'POST' && url.pathname === '/control') {
    const body = await read(request);
    control.set(String(body.key), { fail: Number(body.fail) || 0, delay_ms: Number(body.delay_ms) || 0 });
    return send(response, 200, { ok: true });
  }
  if (url.pathname === '/health') return send(response, 200, { ok: true });
  send(response, 404, { error: 'not found' });
}).listen(8080, () => console.log('downstream stand-in on :8080'));
