import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { requireCoreBinary, runtimeReady, stopChild } from './runtime-process.mjs';

test('one runtime owns static UI, enrollment, API queue, settings and shutdown', { timeout: 60000 }, async (t) => {
  const binary = await requireCoreBinary();
  const state = await mkdtemp(join(tmpdir(), 'layercake-runtime-'));
  const requests = []; let active = 0; let maxActive = 0;
  const fixture = createServer(async (req, res) => {
    if (req.url === '/health') { res.end('{}'); return; }
    res.setHeader('Content-Type', 'application/json');
    if (req.url === '/v1/models') { res.end('{"object":"list","data":[{"id":"test-model"}]}'); return; }
    if (req.url !== '/v1/chat/completions') { res.writeHead(404).end(); return; }
    let body = ''; for await (const part of req) body += part;
    const data = JSON.parse(body); requests.push(data); maxActive = Math.max(maxActive, ++active);
    await new Promise(r => setTimeout(r, 80)); active--;
    if (data.stall) { res.setHeader('Content-Type','text/event-stream'); res.write('data: {"choices":[{"delta":{"content":"waiting"}}]}\n\n'); return; }
    if (data.stream) { res.setHeader('Content-Type','text/event-stream'); res.end('data: {"choices":[{"delta":{"content":"hello"}}]}\n\ndata: [DONE]\n\n'); }
    else res.end('{"choices":[{"message":{"role":"assistant","content":"hello"}}]}');
  });
  fixture.listen(0, '127.0.0.1'); await once(fixture,'listening');
  let child, origin;
  async function start(upstream = true) {
    child = spawn(binary, ['ui','--no-open','--port','0','--state-dir',state,'--assets',resolve('build'),...(upstream ? ['--upstream',`127.0.0.1:${fixture.address().port}`] : [])], { stdio: ['ignore','pipe','pipe'] });
    origin = await runtimeReady(child, /Layercake: (http:\/\/127\.0\.0\.1:\d+)/);
  }
  async function stop() { await stopChild(child); }
  t.after(async () => {
    try { await stop(); } finally {
      fixture.closeAllConnections();
      await new Promise(r => fixture.close(r));
      await rm(state, { recursive: true, force: true });
    }
  });
  const get = async path => (await fetch(origin+path)).json();
  const send = (path, body, method = 'POST', extra = {}) => fetch(origin+path, {method,headers:{Origin:origin,'Content-Type':'application/json',...extra},body:JSON.stringify(body)});
  try {
    await start();
    const root = await fetch(origin); assert.equal(root.status,200); assert.match(await root.text(), /_app/);
    const status = await get('/api/status'); assert.equal(status.pid,child.pid); assert.equal(status.api.base_url,origin+'/v1'); assert.equal(status.api.inference_ready,false);
    assert.equal((await send('/v1/chat/completions',{messages:[]})).status,503);
    assert.equal((await fetch(origin+'/v1/models')).status,503);
    assert.deepEqual(await get('/peers'), []);
    const telemetry = await get('/api/mesh');
    assert.equal(telemetry.peerStatusAvailable, true);
    assert.deepEqual(telemetry.peers, []);
  } finally { await stop(); }
  try {
    await start();
    assert.equal((await send('/api/enrollment',{op:'create'},'POST',{Origin:'https://example.com'})).status,403);
    assert.equal((await send('/api/enrollment',{op:'discover',peers:[]})).status,400);
    assert.equal(await new Promise((ok, fail) => { const req = request(origin+'/api/status', {headers:{Host:'evil.example'}}, res => { res.resume(); ok(res.statusCode); }); req.on('error',fail); req.end(); }),403);
    assert.equal((await send('/api/enrollment',{op:'create',name:'Integration',device_name:'Desktop'})).status,200);
    const enrolled = await get('/api/enrollment'); assert.equal(enrolled.members[0].api.base_url,origin+'/v1');
    assert.equal((await get('/api/status')).api.inference_ready,true);
    assert.equal((await get('/v1/models')).data[0].id,'test-model');
    const chat = () => send('/v1/chat/completions',{messages:[{role:'user',content:'hello'}],conversation_id:'private'});
    const replies = await Promise.all([chat(),chat(),chat()]); for(const r of replies) {assert.equal(r.status,200); await r.text();}
    assert.equal(maxActive,1); assert.equal(requests.length,3); assert.ok(requests.every(r=>!('conversation_id' in r)));
    const stream = await send('/v1/chat/completions',{messages:[],stream:true}); assert.match(await stream.text(), /\[DONE\]/);
    const jobs = await get('/jobs'); assert.equal(jobs.length,4); assert.ok(jobs.every(j=>j.status==='completed'));
    const settings = {displayName:'Kitchen mesh',refreshSeconds:10};
    assert.equal((await send('/api/preferences',{displayName:'',refreshSeconds:0},'PUT')).status,400);
    assert.deepEqual(await (await send('/api/preferences',settings,'PUT')).json(),settings);
    const stalled = await send('/v1/chat/completions',{messages:[],stream:true,stall:true});
    assert.equal(stalled.status,200);
    const beforeStop = Date.now();
    const oldOrigin = origin; await stop();
    assert.ok(Date.now()-beforeStop < 10000,'stalled streams must not block runtime shutdown'); await assert.rejects(fetch(oldOrigin));
    await start(false); assert.deepEqual(await get('/api/preferences'),settings); assert.equal((await get('/api/enrollment')).id,enrolled.id);
    assert.equal((await get('/api/status')).api.inference_ready,false); assert.equal((await send('/v1/chat/completions',{})).status,503);
    assert.equal((await send('/api/enrollment',{op:'leave'})).status,200);
    await stop(); await start(false); assert.equal((await get('/api/enrollment')).mesh,null);
  } finally { await stop(); }
});
