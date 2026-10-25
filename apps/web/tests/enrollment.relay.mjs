import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { once } from 'node:events';

test('fresh web mesh creates signed invites, approves a real peer, and persists membership', { timeout: 120000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'layercake-web-mesh-'));
  const origin = 'http://127.0.0.1:3227';
  const binary = resolve('../../target/debug/llm-cluster');
  let web, peer;
  const start = async () => {
    web = spawn(process.execPath, ['scripts/serve.mjs', '--port', '3227'], { env: { ...process.env, MESHTHING_RUNTIME_URL: '', MESHTHING_UI_STATE_DIR: join(directory, 'ui'), LAYERCAKE_MESH_STATE_DIR: join(directory, 'web'), LAYERCAKE_CORE_BIN: binary }, stdio: ['ignore','pipe','pipe'] });
    await new Promise((ok, fail) => {
      const timer = setTimeout(() => fail(new Error('web startup timed out')), 10000);
      web.stdout.on('data', data => { if (data.toString().includes(origin)) { clearTimeout(timer); ok(); } });
      web.once('exit', () => { clearTimeout(timer); fail(new Error('web exited')); });
    });
  };
  const stop = async () => { if (web && web.exitCode === null) { const exit = once(web, 'exit'); web.kill(); await exit; } };
  const get = async () => { const r = await fetch(origin+'/api/enrollment'); assert.equal(r.status,200); return r.json(); };
  const post = async body => {
    const r = await fetch(origin+'/api/enrollment', { method:'POST',headers:{ Origin:origin,'Content-Type':'application/json' },body:JSON.stringify(body) });
    const result = await r.json(); assert.equal(r.status,200,JSON.stringify(result)); return result;
  };
  try {
    await start();
    assert.equal((await get()).mesh,null);
    assert.equal((await fetch(origin+'/api/enrollment',{method:'POST',headers:{Origin:'https://example.com','Content-Type':'application/json'},body:'{"op":"create"}'})).status,403);
    assert.equal((await fetch(origin+'/api/enrollment',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:'{"op":"discover","peers":[]}'})).status,400);
    const created = await post({op:'create',name:'QR integration',device_name:'Web'});
    const invitation = (await post({op:'invite'})).invite.ticket;
    assert.ok(invitation.startsWith('layercake://join/'));
    peer = spawn(binary,['mesh','--state-dir',join(directory,'peer')],{stdio:['pipe','pipe','ignore']});
    const lines = createInterface({ input:peer.stdout });
    const command = async body => { const reply = once(lines,'line'); peer.stdin.write(JSON.stringify(body)+'\n'); const [line] = await reply; const result = JSON.parse(line); assert.ok(result.ok,result.error); return result.state; };
    // Public relay cold starts can exceed the first connection deadline.
    // Retrying the same applicant/invitation is idempotent in the core.
    let joined;
    for (let attempt = 0; attempt < 3; attempt++) {
      try { joined = await command({op:'join',invitation,device_name:'QR peer'}); break; }
      catch (error) { if (attempt === 2) throw error; }
    }
    assert.equal(joined.mesh,null);
    const pending = (await get()).pending[0];
    assert.ok(pending);
    await post({op:'decide',issuer:pending.issuer,request:pending.id,approve:true});
    await command({op:'refresh'});
    assert.equal((await command({op:'snapshot'})).members.length,2);
    await stop(); await start();
    const restored = await get();
    assert.equal(restored.id,created.id); assert.equal(restored.members.length,2);
    assert.equal((await post({op:'leave'})).mesh,null);
    assert.equal((await command({op:'snapshot'})).members.length,2, 'leaving does not delete the other device’s mesh');
    await stop(); await start(); assert.equal((await get()).mesh,null);
  } finally {
    if (peer) { const ended = once(peer,'exit'); peer.stdin.end(); await ended; }
    await stop(); await rm(directory,{recursive:true,force:true});
  }
});
