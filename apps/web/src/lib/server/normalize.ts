import type { MeshSnapshot, Peer, Source } from '../types';

type Obj = Record<string, unknown>;
const object = (value: unknown): Obj => value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Obj : {};
const array = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
const text = (value: unknown, fallback: string) => typeof value === 'string' ? value.slice(0, 160) : fallback;
const number = (value: unknown) => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;

function normalizePeers(value: unknown): Peer[] {
  const peers: Peer[] = [];
  for (const entry of array(value).slice(0, 64)) {
    const peer = object(entry);
    const id = peer.node_id;
    // Iroh's display form is not hex; accept a bounded printable ID string instead.
    if (typeof id !== 'string' || id.length === 0 || id.length > 128 || /[^\x21-\x7e]/.test(id)) continue;
    peers.push({
      id,
      connectedAt: number(peer.connected_at_unix_ms),
      lastHeartbeat: number(peer.last_heartbeat_unix_ms)
    });
  }
  return peers;
}

export function normalize(raw: unknown, source: Source, endpoint: string, model: string): MeshSnapshot {
  const value = object(raw);
  if (source === 'live-demo' && (typeof value.event !== 'string' || typeof value.active !== 'boolean')) throw new Error('Unsupported runtime state.');
  if (source === 'core' && !Array.isArray(raw) && !Array.isArray(value.jobs)) throw new Error('Unsupported core response.');
  const jobs = source === 'core' ? array(Array.isArray(raw) ? raw : value.jobs) : [];
  const peerStatusAvailable = source !== 'core' || Array.isArray(value.peers);
  const peers = source === 'core' ? normalizePeers(value.peers) : [];
  const samples = array(value.devices);
  // Topology is configuration, not liveness or proof of trust. Do not expose prompts,
  // generated output, commands, trace payloads, filesystem paths, or raw state.
  const topology = array(value.topology);
  const devices = (topology.length ? topology : samples).slice(0, 64).map((entry, i) => {
    const stage = object(entry);
    const sample = object(samples[i]);
    const range = array(stage.range);
    const last = object(sample.last);
    return {
      id: `stage-${i}`, name: text(stage.device, `Stage ${i + 1}`),
      hardware: text(stage.hardware, 'Backend not reported'),
      begin: number(range[0]) ?? number(sample.begin), end: number(range[1]) ?? number(sample.end),
      status: text(sample.status, 'unmeasured'), executions: number(sample.executions) ?? 0,
      computeMs: number(last.compute_ms),
      bufferBytes: number(last.observed_peak_buffer_bytes) ?? number(sample.allocated_bytes)
    };
  });
  const layerCounts = samples.map((s) => number(object(s).layers)).filter((n): n is number => n !== null);
  return {
    connected: true, source, endpoint, checkedAt: new Date().toISOString(), message: null,
    active: value.active === true || (source === 'core' && jobs.some((j) => object(j).status === 'running')),
    event: source === 'core' ? 'jobs' : text(value.event, 'unknown'), model,
    devices, peers, peerStatusAvailable, layers: layerCounts.length ? Math.max(...layerCounts) : null,
    decodeRate: number(value.rate), outputSteps: number(value.output_steps) ?? number(value.steps),
    jobs: source === 'core' ? jobs.slice(-256).map((j) => {
      const job = object(j);
      return { id: text(job.request_id, 'unknown'), status: text(job.status, 'unknown'), createdAt: number(job.created_at_unix_ms) };
    }).reverse() : []
  };
}
