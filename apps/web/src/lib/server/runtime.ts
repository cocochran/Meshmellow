import { loopbackOrigin } from '../../../scripts/validation.mjs';
import { normalize } from './normalize';
import type { MeshSnapshot, Source } from '../types';

export async function readSnapshot(): Promise<MeshSnapshot> {
  const source: Source = process.env.MESHTHING_SOURCE === 'core' ? 'core' : 'live-demo';
  const model = process.env.MESHTHING_MODEL_LABEL || 'Mesh model';
  let endpoint: string | null = null;
  const empty = (message: string): MeshSnapshot => ({
    connected: false, source, endpoint, checkedAt: new Date().toISOString(), message,
    active: false, event: 'unavailable', model, devices: [], layers: null,
    decodeRate: null, outputSteps: null, jobs: []
  });
  if (!process.env.MESHTHING_RUNTIME_URL) return empty('Connect a running mesh to see its devices and measurements.');
  try {
    endpoint = loopbackOrigin(process.env.MESHTHING_RUNTIME_URL);
    const response = await fetch(`${endpoint}${source === 'core' ? '/jobs' : '/state'}`, {
      signal: AbortSignal.timeout(3000), redirect: 'error', headers: { Accept: 'application/json' }
    });
    if (!response.ok) return empty(`The runtime returned HTTP ${response.status}. Check the selected source.`);
    if (!response.body) return empty('The runtime returned an empty response.');
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
    return normalize(JSON.parse(Buffer.concat(chunks).toString('utf8')), source, endpoint, model);
  } catch {
    return empty('Cannot read the runtime. Check that it is running and the source matches its API.');
  }
}
