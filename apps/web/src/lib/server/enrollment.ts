import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import type { EnrollmentState } from '../enrollment';

// One persistent core per web process. Never pass a browser-supplied executable,
// storage path, network address, or arbitrary command to the child.
class Core {
  child?: ChildProcessWithoutNullStreams;
  buffer = '';
  pending?: { resolve: (state: EnrollmentState) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> };
  tail: Promise<unknown> = Promise.resolve();
  queued = 0;
  constructor() { process.once('exit', () => this.child?.kill()); }
  start() {
    if (this.child) return;
    const binary = process.env.LAYERCAKE_CORE_BIN || resolve('../../target/debug/llm-cluster');
    const directory = process.env.LAYERCAKE_MESH_STATE_DIR || join(homedir(), '.local/state/layercake/mesh');
    const child = spawn(binary, ['mesh', '--state-dir', directory], { stdio: 'pipe' });
    this.child = child;
    child.stderr.resume(); // Core errors are returned as JSON; never log invitations.
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk: string) => {
      if (this.child !== child) return;
      this.buffer += chunk;
      if (this.buffer.length > 262144) { this.fail(new Error('Core response is too large.')); return; }
      const end = this.buffer.indexOf('\n');
      if (end < 0) return;
      const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1);
      const pending = this.pending;
      this.pending = undefined;
      if (!pending) { this.fail(new Error('Unexpected core response.')); return; }
      clearTimeout(pending.timer);
      try {
        const reply = JSON.parse(line);
        if (!reply.ok) throw new Error(reply.error || 'Core command failed.');
        pending.resolve(reply.state);
      } catch (error) { pending.reject(error instanceof Error ? error : new Error('Invalid core response.')); }
    });
    child.on('error', () => { if (this.child === child) this.fail(new Error('Build the core first: cargo build --bin llm-cluster')); });
    child.on('exit', () => { if (this.child === child) this.fail(new Error('The mesh core stopped. Retry to reconnect.')); });
    child.stdin.on('error', () => { if (this.child === child) this.fail(new Error('Cannot send a command to the core.')); });
  }
  fail(error: Error) {
    const pending = this.pending; this.pending = undefined;
    if (pending) { clearTimeout(pending.timer); pending.reject(error); }
    const child = this.child; this.child = undefined; this.buffer = '';
    child?.kill();
  }
  command(command: Record<string, unknown>): Promise<EnrollmentState> {
    if (this.queued >= 8) return Promise.reject(new Error('Mesh is busy. Try again shortly.'));
    this.queued++;
    const result = this.tail.then(() => new Promise<EnrollmentState>((resolve, reject) => {
      this.start();
      this.pending = { resolve, reject, timer: setTimeout(() => this.fail(new Error('Core request timed out. Refresh to check its outcome.')), 35000) };
      this.child!.stdin.write(JSON.stringify(command) + '\n');
    }));
    this.tail = result.catch(() => {}).finally(() => { this.queued--; });
    return result;
  }
}
const core = new Core();
export const enrollmentCommand = (command: Record<string, unknown>) => core.command(command);

export function validateCommand(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object') throw new Error('Expected a mesh command.');
  const v = value as Record<string, unknown>;
  const text = (key: string, max = 48) => {
    const s = v[key];
    if (typeof s !== 'string' || !s.trim() || s.length > max || /[\x00-\x1f]/.test(s)) throw new Error(`Invalid ${key}.`);
    return s.trim();
  };
  switch (v.op) {
    case 'create': return { op: v.op, name: text('name'), device_name: text('device_name') };
    case 'join': return { op: v.op, invitation: text('invitation', 4096), device_name: text('device_name') };
    case 'leave': case 'invite': case 'cancel_join': return { op: v.op };
    case 'decide':
      if (typeof v.approve !== 'boolean') throw new Error('Choose approve or decline.');
      return { op: v.op, issuer: text('issuer', 64), request: text('request', 36), approve: v.approve };
    default: throw new Error('Unsupported mesh command.');
  }
}
