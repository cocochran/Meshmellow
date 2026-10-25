import { readSnapshot } from '#lib/server/runtime.ts';
export async function GET() {
  return Response.json(await readSnapshot(), { headers: { 'Cache-Control': 'no-store' } });
}
