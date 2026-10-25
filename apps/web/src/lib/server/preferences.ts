import { mkdir, readFile, rename, writeFile, unlink } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { defaultPreferences, type Preferences } from '../types';

const filename = () => join(process.env.MESHTHING_UI_STATE_DIR || join(homedir(), '.local', 'state', 'meshthing-web'), 'preferences.json');
export function validatePreferences(value: unknown): Preferences {
  if (!value || typeof value !== 'object') throw new Error('Preferences must be an object.');
  const { displayName, refreshSeconds } = value as Record<string, unknown>;
  if (typeof displayName !== 'string' || !displayName.trim() || displayName.trim().length > 48 || /[\x00-\x1f]/.test(displayName)) throw new Error('Choose a display name of 1–48 characters.');
  if (![2, 5, 10, 30].includes(refreshSeconds as number)) throw new Error('Choose a supported refresh interval.');
  return { displayName: displayName.trim(), refreshSeconds: refreshSeconds as number };
}
export async function readPreferences(): Promise<Preferences> {
  try { return validatePreferences(JSON.parse(await readFile(filename(), 'utf8'))); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { ...defaultPreferences };
    throw error;
  }
}
export async function savePreferences(value: unknown): Promise<Preferences> {
  const preferences = validatePreferences(value);
  const path = filename();
  await mkdir(join(path, '..'), { recursive: true, mode: 0o700 });
  const temp = `${path}.${randomUUID()}.tmp`;
  try {
    await writeFile(temp, JSON.stringify(preferences, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
    await rename(temp, path);
  } finally { await unlink(temp).catch(() => {}); }
  return preferences;
}
