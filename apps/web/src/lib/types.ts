import type { ComputeStatus } from './enrollment.ts';
export type Source = 'live-demo' | 'core';
export interface Device {
  id: string;
  name: string;
  hardware: string;
  begin: number | null;
  end: number | null;
  status: string;
  executions: number;
  computeMs: number | null;
  bufferBytes: number | null;
  compute?: ComputeStatus | null;
}
export interface Job { id: string; status: string; createdAt: number | null }
export interface Peer {
  id: string;
  connectedAt: number | null;
  lastHeartbeat: number | null;
}
export interface MeshSnapshot {
  localCompute?: ComputeStatus | null;
  inferenceReady?: boolean;
  execution?: Execution | null;
  hardware?: { id: string; totalBytes: number | null; availableBytes: number | null }[];
  connected: boolean;
  source: Source;
  endpoint: string | null;
  checkedAt: string;
  message: string | null;
  active: boolean;
  event: string;
  model: string;
  devices: Device[];
  peers: Peer[];
  peerStatusAvailable: boolean;
  layers: number | null;
  decodeRate: number | null;
  outputSteps: number | null;
  jobs: Job[];
}
export interface StageObservation {
  id: string; begin: number; end: number; status: string; executions: number;
  computeMs: number | null; bufferBytes: number | null; peakRssBytes: number | null;
  inputBytes: number; outputBytes: number; updatedAt: number;
}
export interface Execution {
  job: string; active: boolean; phase: string; startedAt: number; elapsedMs: number;
  outputTokens: number; decodeTps: number | null; ttftMs: number | null; returnBytes: number;
  stages: StageObservation[];
  samples: { at: number; tokens: number; tps: number | null; buffers: number; activationBytes: number }[];
  events: { at: number; node: string; begin: number; step: number; event: string; bytes: number | null }[];
}
export interface Preferences { displayName: string; refreshSeconds: number }
export const defaultPreferences: Preferences = { displayName: 'My mesh', refreshSeconds: 5 };
