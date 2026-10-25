import { test } from 'node:test';
import assert from 'node:assert/strict';
import { normalize } from '../src/lib/server/normalize.ts';
import { loopbackOrigin } from '../scripts/validation.mjs';

test('runtime projection preserves measurements without publishing private content or claiming liveness', () => {
  const snapshot = normalize({
    event: 'done', active: false, rate: 1.2, prompt: 'private prompt', text: 'private response',
    topology: [{ device: 'Test phone', hardware: 'CPU', range: [4, 8], command: ['private path'] }],
    devices: [{ status: 'returned', executions: 7, layers: 64, allocated_bytes: 2048, last: { compute_ms: 12.3 } }]
  }, 'live-demo', 'http://127.0.0.1:8095', 'Test model');
  assert.equal(snapshot.active, false);
  assert.equal(snapshot.devices[0].computeMs, 12.3);
  assert.equal(snapshot.devices[0].bufferBytes, 2048);
  assert.equal(snapshot.devices[0].status, 'returned');
  assert.equal(snapshot.layers, 64);
  assert.doesNotMatch(JSON.stringify(snapshot), /private|online|trusted/);
});

test('unmeasured stages and malformed numeric data stay unknown', () => {
  const snapshot = normalize({ event: 'idle', active: false, topology: [{ device: 'Phone', range: [0, 4] }], rate: 'fast' }, 'live-demo', 'http://127.0.0.1:8095', 'Model');
  assert.equal(snapshot.devices[0].bufferBytes, null);
  assert.equal(snapshot.devices[0].status, 'unmeasured');
  assert.equal(snapshot.layers, null);
  assert.equal(snapshot.decodeRate, null);
  assert.throws(() => normalize({}, 'live-demo', '', 'Model'));
  assert.throws(() => normalize({}, 'core', '', 'Model'));
});

test('core queue is supported without inventing devices or exposing conversation identifiers', () => {
  const snapshot = normalize([{ request_id: 'job-1', status: 'running', created_at_unix_ms: 1234, conversation_id: 'private' }], 'core', 'http://127.0.0.1:8080', 'Model');
  assert.equal(snapshot.active, true);
  assert.deepEqual(snapshot.devices, []);
  assert.deepEqual(snapshot.jobs, [{ id: 'job-1', status: 'running', createdAt: 1234 }]);
});

test('runtime target accepts only explicit loopback HTTP origins', () => {
  assert.equal(loopbackOrigin('http://127.0.0.1:8095'), 'http://127.0.0.1:8095');
  assert.equal(loopbackOrigin('http://[::1]:8095'), 'http://[::1]:8095');
  for (const value of ['https://example.com', 'http://192.168.1.10:8095', 'http://localhost:8095/state', 'http://user:pass@localhost:8095', 'http://localhost:8095/?token=value']) assert.throws(() => loopbackOrigin(value));
});
