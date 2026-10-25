import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readPreferences, savePreferences, validatePreferences } from '../src/lib/server/preferences.ts';

test('preferences validate and persist atomically in the chosen state directory', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'meshthing-ui-test-'));
  const previous = process.env.MESHTHING_UI_STATE_DIR;
  process.env.MESHTHING_UI_STATE_DIR = directory;
  try {
    assert.deepEqual(await readPreferences(), { displayName: 'My mesh', refreshSeconds: 5 });
    await savePreferences({ displayName: ' Home mesh ', refreshSeconds: 10 });
    assert.deepEqual(await readPreferences(), { displayName: 'Home mesh', refreshSeconds: 10 });
    await assert.rejects(savePreferences({ displayName: '', refreshSeconds: 5 }));
    assert.equal((await readPreferences()).displayName, 'Home mesh');
    assert.deepEqual(await readdir(directory), ['preferences.json']);
  } finally {
    if (previous === undefined) delete process.env.MESHTHING_UI_STATE_DIR;
    else process.env.MESHTHING_UI_STATE_DIR = previous;
    await rm(directory, { recursive: true, force: true });
  }
});

test('invalid settings cannot become runtime commands or invalid polling intervals', () => {
  for (const value of [null, {}, { displayName: '\n', refreshSeconds: 5 }, { displayName: 'mesh', refreshSeconds: 0 }, { displayName: 'x'.repeat(49), refreshSeconds: 5 }]) assert.throws(() => validatePreferences(value));
  assert.deepEqual(validatePreferences({ displayName: 'mesh', refreshSeconds: 2, command: 'ignored' }), { displayName: 'mesh', refreshSeconds: 2 });
});
