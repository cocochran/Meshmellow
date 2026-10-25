import { access } from 'node:fs/promises';
import { constants } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export function coreBinary(env = process.env, cwd = process.cwd(), platform = process.platform) {
  if (env.LAYERCAKE_CORE_BIN) return resolve(cwd, env.LAYERCAKE_CORE_BIN);
  const target = env.CARGO_TARGET_DIR
    ? resolve(cwd, env.CARGO_TARGET_DIR)
    : fileURLToPath(new URL('../../../target/', import.meta.url));
  return resolve(target, 'debug', platform === 'win32' ? 'llm-cluster.exe' : 'llm-cluster');
}

export async function requireCoreBinary() {
  const binary = coreBinary();
  await access(binary, process.platform === 'win32' ? constants.F_OK : constants.X_OK);
  return binary;
}

export function runtimeReady(child, pattern, timeoutMs = 15000) {
  return new Promise((resolveReady, reject) => {
    let output = '', errors = '';
    const finish = (error, value) => {
      clearTimeout(timer);
      child.off('error', onError); child.off('exit', onExit);
      child.stdout.off('data', onData); child.stderr.off('data', onStderr);
      child.stdout.resume(); child.stderr.resume();
      error ? reject(error) : resolveReady(value);
    };
    const onError = error => finish(error);
    const onExit = code => finish(new Error(`Runtime exited ${code}: ${errors}`));
    const onStderr = chunk => { errors = (errors + chunk).slice(-16384); };
    const onData = chunk => {
      output = (output + chunk).slice(-16384);
      const match = pattern.exec(output);
      if (match) finish(null, match[1]);
    };
    const timer = setTimeout(() => finish(new Error(`Runtime startup timed out: ${errors}`)), timeoutMs);
    child.once('error', onError); child.once('exit', onExit);
    child.stdout.on('data', onData); child.stderr.on('data', onStderr);
  });
}

export async function stopChild(child) {
  if (!child?.pid || child.exitCode !== null || child.signalCode !== null) return;
  await new Promise((resolveStopped, reject) => {
    let forced;
    const done = error => {
      clearTimeout(timer); clearTimeout(forced); child.off('close', closed);
      error ? reject(error) : resolveStopped();
    };
    const closed = () => done();
    child.once('close', closed);
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      forced = setTimeout(() => done(new Error('Runtime did not exit after forced stop')), 2000);
    }, 5000);
    child.kill('SIGTERM');
  });
}
