import { test } from 'node:test';
import assert from 'node:assert/strict';
import { computeLabel } from '../src/lib/enrollment.ts';

test('model status shows the loaded layer range as inclusive', () => {
  assert.deepEqual(computeLabel({ phase: 'ready', message: '', begin: 16, end: 32 }), { label: 'Layers 16–31 loaded', tone: 'ready' });
  assert.deepEqual(computeLabel({ phase: 'loading', message: '', begin: 0, end: 8 }), { label: 'Loading layers 0–7…', tone: 'working' });
});

test('model status distinguishes offline, unconfigured and failed devices', () => {
  assert.equal(computeLabel({ phase: 'ready', message: '', begin: 0, end: 8 }, false).label, 'Offline');
  assert.equal(computeLabel(null).label, 'No local model');
  assert.equal(computeLabel({ phase: 'failed', message: 'hash mismatch' }).tone, 'failed');
  assert.equal(computeLabel({ phase: 'verifying', message: '' }).tone, 'working');
});
