export interface NodeApi { base_url: string; scope: string; inference_ready: boolean; message: string }
/** A device's native model progress, reported by the device itself. */
export interface ComputeStatus {
  phase: 'verifying' | 'benchmarking' | 'standby' | 'loading' | 'ready' | 'failed' | string;
  message: string;
  layers?: number | null;
  begin?: number | null;
  end?: number | null;
}
export type ComputeTone = 'ready' | 'working' | 'idle' | 'failed' | 'none';
const layerRange = (c: ComputeStatus) => c.begin != null && c.end != null ? `${c.begin}–${c.end - 1}` : null;
/** One short label for a device's model state, plus a tone for its indicator. */
export function computeLabel(compute: ComputeStatus | null | undefined, online = true): { label: string; tone: ComputeTone } {
  if (!online) return { label: 'Offline', tone: 'none' };
  if (!compute) return { label: 'No local model', tone: 'none' };
  const range = layerRange(compute);
  switch (compute.phase) {
    case 'verifying': return { label: 'Checking model file…', tone: 'working' };
    case 'benchmarking': return { label: 'Benchmarking…', tone: 'working' };
    case 'standby': return { label: 'Model ready · no layers assigned', tone: 'idle' };
    case 'loading': return { label: range ? `Loading layers ${range}…` : 'Loading layers…', tone: 'working' };
    case 'ready': return { label: range ? `Layers ${range} loaded` : 'Loaded', tone: 'ready' };
    case 'failed': return { label: 'Model failed to load', tone: 'failed' };
    default: return { label: compute.phase, tone: 'idle' };
  }
}
export interface EnrollmentState {
  can_kick?: boolean;
  inference?: { compute?: ComputeStatus | null } | null;
  api?: NodeApi | null;
  id: string;
  mesh: { id: string; name: string } | null;
  invite: { id: string; expires: number; ticket: string } | null;
  joining: { name: string; expires: number } | null;
  members: { id: string; name: string; self: boolean; online: boolean; api?: NodeApi | null; compute?: ComputeStatus | null }[];
  pending: { id: string; applicant: string; name: string; issuer: string; expires: number }[];
  notice: string;
}
export const isInvitation = (text: string) => text.length <= 4096 && text.trim().startsWith('layercake://join/');
