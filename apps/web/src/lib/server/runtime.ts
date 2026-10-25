import { loopbackOrigin } from '../../../scripts/validation.mjs';
import { normalize } from './normalize';
import type { MeshSnapshot, Source } from '../types';

export async function readSnapshot(): Promise<MeshSnapshot> {
  const source: Source = process.env.MESHTHING_SOURCE === 'core' ? 'core' : 'live-demo';
  const model = process.env.MESHTHING_MODEL_LABEL || 'Mesh model';
  let endpoint: string | null = null;
  const empty = (message: string): MeshSnapshot => ({
    connected: false, source, endpoint, checkedAt: new Date().toISOString(), message,
    active: false, event: 'unavailable', model, devices: [], peers: [], peerStatusAvailable: false, layers: null,
    decodeRate: null, outputSteps: null, jobs: []
  });
  if (!process.env.MESHTHING_RUNTIME_URL) return empty('Connect a running mesh to see its devices and measurements.');
  try {
    endpoint = loopbackOrigin(process.env.MESHTHING_RUNTIME_URL);
    const readJson = async (path: string) => {
      const response = await fetch(`${endpoint}${path}`, {
        signal: AbortSignal.timeout(3000), redirect: 'error', headers: { Accept: 'application/json' }
      });
      if (!response.ok) throw new Error(`The runtime returned HTTP ${response.status}.`);
      if (!response.body) throw new Error('The runtime returned an empty response.');
      const reader = response.body.getReader();
      let size = 0;
      const chunks: Uint8Array[] = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > 1024 * 1024) { await reader.cancel(); throw new Error('Runtime response exceeds limit.'); }
        chunks.push(value);
      }
      return JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown;
    };
    if (source === 'core') {
      const jobs = await readJson('/jobs');
      let peers: unknown = null;
      try {
        peers = await readJson('/peers');
      } catch {
        // Older cores expose the job API without connected-peer status.
      }
      return normalize({ jobs, peers }, source, endpoint, model);
    }
    return normalize(await readJson('/state'), source, endpoint, model);
  } catch {
    return empty('Cannot read the runtime. Check that it is running and the source matches its API.');
  }
}
