import { readPreferences, savePreferences, validatePreferences } from '#lib/server/preferences.ts';
import type { RequestHandler } from './$types';
export async function GET() {
  try { return Response.json(await readPreferences(), { headers: { 'Cache-Control': 'no-store' } }); }
  catch { return Response.json({ error: 'Cannot read UI preferences.' }, { status: 500 }); }
}
export const PUT: RequestHandler = async ({ request, url }) => {
  if (request.headers.get('origin') !== url.origin) return Response.json({ error: 'Same-origin request required.' }, { status: 403 });
  if (!request.headers.get('content-type')?.startsWith('application/json')) return Response.json({ error: 'Expected JSON.' }, { status: 415 });
  let preferences;
  try {
    const text = await request.text();
    if (text.length > 2048) throw new Error('Preferences are too large.');
    preferences = validatePreferences(JSON.parse(text));
  } catch { return Response.json({ error: 'Use a name of 1–48 characters and a refresh interval of 2, 5, 10 or 30 seconds.' }, { status: 400 }); }
  try { return Response.json(await savePreferences(preferences), { headers: { 'Cache-Control': 'no-store' } }); }
  catch { return Response.json({ error: 'Could not save preferences. Check the state directory permissions.' }, { status: 500 }); }
};
