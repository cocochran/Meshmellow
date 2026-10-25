#!/usr/bin/env node
import { createServer } from 'node:http';
import { parseArgs } from 'node:util';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { loopbackOrigin } from './validation.mjs';

const { values } = parseArgs({ options: {
  port: { type: 'string', default: '3210' },
  runtime: { type: 'string' },
  source: { type: 'string', default: 'live-demo' },
  model: { type: 'string' },
  open: { type: 'boolean', default: false },
  help: { type: 'boolean', default: false }
} });
if (values.help) {
  console.log('Layercake UI\n\n./mesh-ui [--port 3210] [--runtime http://127.0.0.1:8095] [--source live-demo|core] [--model "Model label"] [--open]\n\nReads runtime state only. Ctrl+C stops the UI, leaving workers running.');
  process.exit(0);
}
const port = Number(values.port);
if (!Number.isInteger(port) || port < 1024 || port > 65535) throw new Error('Port must be an integer from 1024 to 65535.');
if (!['live-demo', 'core'].includes(values.source)) throw new Error('Source must be live-demo or core.');
const runtime = values.runtime || process.env.MESHTHING_RUNTIME_URL;
if (runtime) process.env.MESHTHING_RUNTIME_URL = loopbackOrigin(runtime);
process.env.MESHTHING_SOURCE = values.source;
if (values.model) process.env.MESHTHING_MODEL_LABEL = values.model;
process.env.LAYERCAKE_CORE_BIN ||= fileURLToPath(new URL('../../../target/debug/llm-cluster', import.meta.url));
const build = new URL('../build/handler.js', import.meta.url);
if (!existsSync(fileURLToPath(build))) {
  console.error('Build the UI first: npm --prefix apps/web ci && npm --prefix apps/web run build');
  process.exit(1);
}
// Our listener supplies this value; never trust a caller's forwarded protocol.
process.env.PROTOCOL_HEADER = 'x-meshthing-protocol';
const { handler } = await import(build.href);
const origin = `http://127.0.0.1:${port}`;
const allowedHosts = new Set([`127.0.0.1:${port}`, `localhost:${port}`]);
const server = createServer((req, res) => {
  if (!allowedHosts.has(req.headers.host ?? '')) {
    res.writeHead(403).end('Use the local URL printed in your terminal.');
    return;
  }
  req.headers['x-meshthing-protocol'] = 'http';
  if (!['GET', 'HEAD'].includes(req.method ?? '') && req.headers.origin !== `http://${req.headers.host}`) {
    res.writeHead(403).end('A same-origin request is required.');
    return;
  }
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('X-Frame-Options', 'DENY');
  handler(req, res);
});
server.on('error', (error) => { console.error(`Cannot start Layercake UI: ${error.message}`); process.exitCode = 1; });
server.listen(port, '127.0.0.1', () => {
  console.log(`\n  Layercake · your devices, one model\n  ${origin}\n\n  ${runtime ? `Reading ${values.source} at ${runtime}` : 'No runtime connected. See ./mesh-ui --help.'}\n  Ctrl+C stops this UI only.\n`);
  if (values.open) {
    const command = process.platform === 'darwin' ? 'open' : process.platform === 'win32' ? 'explorer.exe' : 'xdg-open';
    const child = spawn(command, [origin], { stdio: 'ignore' });
    child.on('error', () => console.log(`Open ${origin} in your browser.`));
    child.unref();
  }
});
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => {
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
});
