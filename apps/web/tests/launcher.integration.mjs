import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

async function freePort() {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  return port;
}

test('production launcher serves live/core/disconnected states and persists only valid local preferences', { timeout: 30000 }, async () => {
  const state = await mkdtemp(join(tmpdir(), 'meshthing-web-integration-'));
  const requests = [];
  let unavailable = false;
  const fixture = createServer((req, res) => {
    requests.push(`${req.method} ${req.url}`);
    if (unavailable) { res.writeHead(503).end(); return; }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(req.url === '/jobs'
      ? [{ request_id: 'synthetic-request', status: 'queued', created_at_unix_ms: 1000 }]
      : { event: 'done', active: false, prompt: 'PRIVATE_FIXTURE', text: 'PRIVATE_FIXTURE', topology: [{ device: 'Synthetic phone', range: [0, 4] }], devices: [{ status: 'returned', layers: 4, allocated_bytes: 1024 }] }));
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const runtime = `http://127.0.0.1:${fixture.address().port}`;
  let child;
  async function start(source, connected = true) {
    const port = await freePort();
    const origin = `http://127.0.0.1:${port}`;
    child = spawn(process.execPath, ['scripts/serve.mjs', '--port', `${port}`, '--source', source, ...(connected ? ['--runtime', runtime] : [])], {
      cwd: new URL('..', import.meta.url),
      env: { ...process.env, MESHTHING_UI_STATE_DIR: state, MESHTHING_RUNTIME_URL: '' },
      stdio: ['ignore', 'pipe', 'pipe']
    });
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Launcher did not become ready')), 10000);
      child.once('exit', (code) => { clearTimeout(timeout); reject(new Error(`Launcher exited ${code}`)); });
      child.stdout.on('data', (data) => { if (data.toString().includes(origin)) { clearTimeout(timeout); resolve(); } });
      child.stderr.on('data', (data) => { clearTimeout(timeout); reject(new Error(data.toString())); });
    });
    return origin;
  }
  async function stop() {
    if (child && child.exitCode === null) { const ended = once(child, 'exit'); child.kill('SIGTERM'); await ended; }
  }
  try {
    let origin = await start('live-demo');
    assert.equal((await fetch(origin)).status, 200);
    const stateReply = await fetch(`${origin}/api/mesh`);
    assert.equal(stateReply.headers.get('cache-control'), 'no-store');
    const snapshot = await stateReply.json();
    assert.equal(snapshot.connected, true);
    assert.equal(snapshot.devices[0].name, 'Synthetic phone');
    assert.doesNotMatch(JSON.stringify(snapshot), /PRIVATE_FIXTURE/);
    const settings = { displayName: 'Integration mesh', refreshSeconds: 10 };
    const put = (body, headers = {}) => fetch(`${origin}/api/preferences`, { method: 'PUT', headers: { 'Content-Type': 'application/json', Origin: origin, ...headers }, body: JSON.stringify(body) });
    assert.equal((await put(settings, { Origin: 'https://example.com' })).status, 403);
    assert.equal((await put({ displayName: '', refreshSeconds: 0 })).status, 400);
    assert.deepEqual(await (await put(settings)).json(), settings);
    unavailable = true;
    assert.equal((await (await fetch(`${origin}/api/mesh`)).json()).connected, false);
    unavailable = false;
    await stop();
    origin = await start('core');
    assert.deepEqual(await (await fetch(`${origin}/api/preferences`)).json(), settings);
    const core = await (await fetch(`${origin}/api/mesh`)).json();
    assert.equal(core.jobs[0].id, 'synthetic-request');
    assert.equal(core.devices.length, 0);
    await stop();
    origin = await start('live-demo', false);
    assert.equal((await (await fetch(`${origin}/api/mesh`)).json()).endpoint, null);
    assert.ok(requests.every((path) => ['GET /jobs', 'GET /state'].includes(path)), 'UI must never mutate the runtime');
  } finally {
    await stop();
    await new Promise((resolve) => fixture.close(resolve));
    await rm(state, { recursive: true, force: true });
  }
});
