import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { coreBinary, runtimeReady, stopChild } from './runtime-process.mjs';

test('test binary resolution honors explicit override, Cargo target directory, and Windows suffix', () => {
  assert.equal(coreBinary({ CARGO_TARGET_DIR: 'custom' }, '/workspace', 'linux'), resolve('/workspace/custom/debug/llm-cluster'));
  assert.equal(coreBinary({ CARGO_TARGET_DIR: '/builds' }, '/workspace', 'win32'), resolve('/builds/debug/llm-cluster.exe'));
  assert.equal(coreBinary({ CARGO_TARGET_DIR: '/builds', LAYERCAKE_CORE_BIN: './core' }, '/workspace', 'linux'), resolve('/workspace/core'));
  assert.ok(coreBinary({}, '/unrelated', 'linux').endsWith(join('target', 'debug', 'llm-cluster')));
});

test('failed spawn rejects startup and cleanup does not wait for a nonexistent process', { timeout: 3000 }, async () => {
  const child = spawn(join(tmpdir(), randomUUID(), 'missing'), [], { stdio: ['ignore', 'pipe', 'pipe'] });
  await assert.rejects(runtimeReady(child, /Layercake: (.+)/), { code: 'ENOENT' });
  await stopChild(child);
});

test('early exit rejects startup and a live child is reaped during cleanup', { timeout: 5000 }, async () => {
  const exited = spawn(process.execPath, ['-e', 'process.exit(4)'], { stdio: ['ignore', 'pipe', 'pipe'] });
  await assert.rejects(runtimeReady(exited, /Layercake: (.+)/), /Runtime exited 4/);
  const live = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: ['ignore', 'pipe', 'pipe'] });
  await once(live, 'spawn');
  await stopChild(live);
  assert.ok(live.exitCode !== null || live.signalCode !== null);
});
