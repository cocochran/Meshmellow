import { enrollmentCommand, validateCommand } from '#lib/server/enrollment.ts';
import type { RequestHandler } from './$types';
const headers = { 'Cache-Control': 'no-store' };
export const GET: RequestHandler = async () => {
  try { return Response.json(await enrollmentCommand({ op: 'snapshot' }), { headers }); }
  catch (e) { return Response.json({ error: e instanceof Error ? e.message : 'Core unavailable.' }, { status: 503, headers }); }
};
export const POST: RequestHandler = async ({ request, url }) => {
  if (request.headers.get('origin') !== url.origin) return Response.json({ error: 'Same-origin request required.' }, { status: 403, headers });
  if (!request.headers.get('content-type')?.startsWith('application/json')) return Response.json({ error: 'Expected JSON.' }, { status: 415, headers });
  let command;
  try {
    // Bound streamed input even when Content-Length is absent.
    const reader = request.body?.getReader();
    if (!reader) throw new Error('Missing command.');
    const chunks: Uint8Array[] = []; let size = 0;
    while (true) {
      const part = await reader.read(); if (part.done) break;
      size += part.value.length;
      if (size > 8192) { await reader.cancel(); throw new Error('Command is too large.'); }
      chunks.push(part.value);
    }
    command = validateCommand(JSON.parse(Buffer.concat(chunks).toString('utf8')));
  } catch (e) { return Response.json({ error: e instanceof Error ? e.message : 'Invalid command.' }, { status: 400, headers }); }
  try { return Response.json(await enrollmentCommand(command), { headers }); }
  catch (e) { return Response.json({ error: e instanceof Error ? e.message : 'Mesh command failed.' }, { status: 400, headers }); }
};
