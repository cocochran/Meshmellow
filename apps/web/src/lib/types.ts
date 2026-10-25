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
}
export interface Job { id: string; status: string; createdAt: number | null }
export interface MeshSnapshot {
  connected: boolean;
  source: Source;
  endpoint: string | null;
  checkedAt: string;
  message: string | null;
  active: boolean;
  event: string;
  model: string;
  devices: Device[];
  layers: number | null;
  decodeRate: number | null;
  outputSteps: number | null;
  jobs: Job[];
}
export interface Preferences { displayName: string; refreshSeconds: number }
export const defaultPreferences: Preferences = { displayName: 'My mesh', refreshSeconds: 5 };
