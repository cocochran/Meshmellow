<script lang="ts">
  import type { MeshSnapshot } from './types.ts';
  import type { EnrollmentState } from './enrollment.ts';
  import Icon from './Icon.svelte';

  let { snapshot, membership, connected }: { snapshot: MeshSnapshot | null; membership: EnrollmentState | null; connected: boolean } = $props();
  const palette = ['#42685c', '#89602f', '#526a8d', '#865567', '#625d8d'];
  const run = $derived(snapshot?.execution);
  const live = $derived(connected && !!run?.active);
  const clock = $derived(Number(snapshot?.checkedAt ?? 0));
  const hardware = $derived(snapshot?.hardware ?? []);
  const memoryReports = $derived(hardware.filter(d => d.totalBytes !== null));
  const totalRam = $derived(memoryReports.reduce((n, d) => n + (d.totalBytes ?? 0), 0));
  const nodes = $derived((run?.stages ?? snapshot?.devices ?? []).map((stage, i) => ({
    ...stage,
    name: membership?.members.find(m => m.id === stage.id)?.name ?? snapshot?.devices.find(d => d.id === stage.id)?.name ?? stage.id.slice(0, 10),
    online: membership?.members.find(m => m.id === stage.id)?.online ?? false,
    color: palette[i % palette.length],
    observation: run?.stages.find(d => d.id === stage.id),
    memory: hardware.find(d => d.id === stage.id)
  })));
  const samples = $derived(run?.samples ?? []);
  const buffers = $derived(run?.stages.reduce((n, s) => n + (s.bufferBytes ?? 0), 0) ?? 0);
  const activations = $derived(run?.stages.filter(s => s.begin > 0).reduce((n, s) => n + s.inputBytes, 0) ?? 0);
  const participants = $derived(run?.stages.filter(s => s.executions > 0) ?? []);
  const maxCompute = $derived(Math.max(1, ...nodes.map(n => n.observation?.computeMs ?? 0)));
  const recent = $derived([...(run?.events ?? [])].reverse().slice(0, 5));
  const ready = $derived(snapshot?.inferenceReady ?? snapshot?.event === 'ready');
  const phase = $derived(!connected ? 'Connection lost' : live ? run?.phase === 'loading' ? 'Loading model slices' : run?.phase === 'prefill' ? 'Processing prompt' : 'Generating tokens' : run?.phase === 'done' ? 'Last request complete' : run?.phase === 'error' ? 'Last request failed' : run?.phase === 'cancelled' ? 'Last request cancelled' : ready ? 'Ready for a request' : 'Waiting for workers');
  const bytes = (n: number | null | undefined) => n == null ? '—' : n >= 1024 ** 3 ? `${(n / 1024 ** 3).toFixed(1)} GiB` : n >= 1024 ** 2 ? `${(n / 1024 ** 2).toFixed(1)} MiB` : n >= 1024 ? `${(n / 1024).toFixed(1)} KiB` : `${n} B`;
  const seconds = (n: number | null | undefined) => n == null ? '—' : `${(n / 1000).toFixed(2)} s`;
  const hot = (at: number | undefined) => live && at !== undefined && clock - at < 800;
  type Series = 'tps' | 'activationBytes' | 'buffers';
  function values(key: Series) { return samples.filter(s => s[key] !== null); }
  function ceiling(key: Series) { return Math.max(1, ...values(key).map(s => s[key] ?? 0)); }
  function points(key: Series) {
    const duration = Math.max(1, samples.at(-1)?.at ?? 1);
    const max = ceiling(key);
    return values(key).map(s => `${8 + s.at / duration * 344},${86 - (s[key] ?? 0) / max * 70}`).join(' ');
  }
  const graphs: { key: Series; label: string; color: string }[] = [
    { key: 'tps', label: 'Token speed', color: '#42685c' },
    { key: 'activationBytes', label: 'Activations received · all hops', color: '#526a8d' },
    { key: 'buffers', label: 'Native buffer allocations', color: '#89602f' }
  ];
</script>

<section class="network" aria-labelledby="network-title">
  <header class="network-head">
    <div><h2 id="network-title">Execution route</h2><p>{run ? 'Stages from the latest request, in execution order.' : nodes.length ? 'The current route. Run a prompt to measure each stage.' : 'Devices with a loaded model slice appear here in execution order.'}</p></div>
    <span class="phase" class:running={live} class:ready={!live && ready && connected && run?.phase !== 'error'} class:failed={run?.phase === 'error' || !connected}><i></i>{phase}</span>
  </header>

  {#if run}
  <div class="gauges">
    <div><span>Decode speed</span><strong>{run?.decodeTps?.toFixed(1) ?? '—'}<small>tok/s</small></strong><p>{live ? 'Current request average' : 'Latest request average'}</p></div>
    <div><span>Time to first token</span><strong>{seconds(run?.ttftMs)}</strong><p>Includes loading model slices</p></div>
    <div><span>Output tokens</span><strong>{run?.outputTokens ?? '—'}<small>tokens</small></strong><p>{run ? `${seconds(run.elapsedMs)} elapsed` : 'Waiting for a request'}</p></div>
    <div><span>Total host RAM</span><strong>{memoryReports.length ? bytes(totalRam) : '—'}</strong><p>{memoryReports.length} of {membership?.members.filter(m => m.self || m.online).length ?? 0} connected devices reporting</p></div>
  </div>
  {/if}

  {#if nodes.length}
  <div class="route-heading"><span>{run ? 'Request path' : 'Assigned path'}</span><span>{run?.job ? `JOB ${run.job.slice(0, 10)}` : `${nodes.length} assigned devices`} · {snapshot?.layers ?? '—'} layers</span></div>
  <div class="flow-scroll">
    <div class="flow" style={`--nodes: ${Math.max(1, nodes.length)}`}>
      {#each nodes as node, i (node.id)}
        <article class="node" class:hot={hot(node.observation?.updatedAt)} style={`--node-color:${node.color}`}>
          <div class="node-top"><span class="node-icon"><Icon name={i === 0 ? 'laptop' : i === nodes.length - 1 && nodes.length > 2 ? 'phone' : 'server'} size={21}/></span><span class="node-state">{live ? node.observation?.status === 'computing' ? 'Computing' : node.observation?.status === 'loading' ? 'Loading' : 'Ready' : node.observation?.executions ? 'Executed' : node.online || node.id === membership?.id ? 'Connected' : 'Offline'}</span></div>
          <h3>{node.name}</h3><div class="node-identity">{node.id.slice(0, 10)} · {i === 0 ? 'Origin' : i === nodes.length - 1 ? 'Final stage' : 'Middle stage'}</div>
          <div class="node-layers"><span>Layers</span><strong>{node.begin ?? '?'}–{node.end == null ? '?' : node.end - 1}</strong><span>{node.begin != null && node.end != null ? node.end - node.begin : '?'} / {snapshot?.layers ?? '?'}</span></div>
          <div class="layer-ticks" aria-hidden="true">{#each Array.from({ length: snapshot?.layers ?? 16 }) as _, layer}<i class:assigned={node.begin != null && node.end != null && layer >= node.begin && layer < node.end}></i>{/each}</div>
          <dl><div><dt>Native compute</dt><dd>{node.observation?.computeMs?.toFixed(1) ?? '—'}<small> ms / step</small></dd></div><div><dt>Buffers</dt><dd>{bytes(node.observation?.bufferBytes)}</dd></div><div><dt>Host RAM</dt><dd>{bytes(node.memory?.totalBytes)}</dd></div><div><dt>Steps executed</dt><dd>{node.observation?.executions ?? 0}</dd></div></dl>
          <div class="compute-bar" title="Latest native compute relative to the slowest stage"><i style={`width:${(node.observation?.computeMs ?? 0) / maxCompute * 100}%`}></i></div>
        </article>
        {#if i < nodes.length - 1}
          <div class="hop" class:flowing={hot(nodes[i + 1].observation?.updatedAt)}><span>activations</span><div class="wire"><i></i><b>›</b></div><strong>{bytes(nodes[i + 1].observation?.inputBytes)}</strong></div>
        {/if}
      {/each}
    </div>
    {#if nodes.length > 1}<div class="return-wire" class:flowing={run != null && samples.length > 0 && hot(run.startedAt + (samples.at(-1)?.at ?? 0))}><i></i><span>← tokens and control return to the origin <strong>{bytes(run?.returnBytes)}</strong></span></div>{/if}
  </div>

  {:else}
  <p class="empty">No route yet. A route forms when at least two connected devices have verified the same model; their status is shown in Devices above.</p>
  {/if}

  {#if run}
  <div class="participation"><Icon name={run?.phase === 'done' ? 'check' : 'activity'} size={16}/><span>{#if participants.length}<strong>{participants.length} devices {live ? 'have contributed' : 'executed steps'}:</strong> {participants.map(s => nodes.find(n => n.id === s.id)?.name ?? s.id.slice(0, 8)).join(' → ')}{:else}A connected device is confirmed here once it executes a model step.{/if}</span></div>

  <div class="graphs">
    {#each graphs as graph}
      <div class="chart" style={`--chart-color:${graph.color}`}>
        <div class="chart-heading"><span>{graph.label}</span><strong>{graph.key === 'tps' ? run?.decodeTps == null ? '—' : `${run.decodeTps.toFixed(1)} tok/s` : run ? bytes(graph.key === 'buffers' ? buffers : activations) : '—'}</strong></div>
        <div class="plot">
          <svg viewBox="0 0 360 100" preserveAspectRatio="none" role="img" aria-label={`${graph.label} over the request duration`}>
            <path class="grid" d="M8 16H352 M8 51H352 M8 86H352"/>
            {#if values(graph.key).length > 1}<polygon class="area" points={`8,86 ${points(graph.key)} 352,86`}/><polyline points={points(graph.key)}/>{/if}
          </svg>
          {#if values(graph.key).length < 2}<span class="plot-empty">Waiting for token measurements</span>{/if}
          {#if values(graph.key).length > 1}<span class="scale">{graph.key === 'tps' ? `${ceiling(graph.key).toFixed(1)} tok/s` : bytes(ceiling(graph.key))}</span>{/if}
        </div>
        <div class="axis"><span>0 s</span><span>{seconds(samples.at(-1)?.at)}</span></div>
      </div>
    {/each}
  </div>

  <details class="trace"><summary>Execution trace <span>{run?.events.length ?? 0} recent observations</span></summary><div>{#each recent as event}<p><time>+{((event.at - (run?.startedAt ?? event.at)) / 1000).toFixed(2)}s</time><strong>{nodes.find(n => n.id === event.node)?.name ?? event.node.slice(0, 10)}</strong><span>Step {event.step} · {event.event === 'stage_start' ? 'received input' : 'finished compute'}</span><code>{bytes(event.bytes)}</code></p>{/each}{#if !recent.length}<p>No execution events yet.</p>{/if}</div></details>
  <footer>Measured native execution · decode speed excludes the first token · host RAM includes operating-system use; buffers are model allocations · {connected ? live ? 'updating 5× / second' : 'last request retained' : 'disconnected; measurements may be stale'}</footer>
  {/if}
</section>

<style>
  .network{background:var(--surface);color:var(--ink);border:1px solid var(--line);border-radius:7px;margin:0 0 24px;overflow:hidden}
  .network-head{padding:20px 22px;display:flex;justify-content:space-between;align-items:center;gap:16px}.network h2{font-size:14px;font-weight:650}.network-head p{font-size:12px;color:var(--muted);margin-top:4px;line-height:1.6}
  .phase{font-size:11px;display:inline-flex;align-items:center;gap:7px;white-space:nowrap;border:1px solid var(--line);background:#faf9f7;border-radius:4px;padding:5px 9px;color:#736a60}.phase i{width:6px;height:6px;border-radius:50%;background:#a49d92}.phase.ready{color:#34714b}.phase.ready i{background:#4e8061}.phase.running{color:#34714b}.phase.running i{background:#43895c;animation:breathe 1s infinite}.phase.failed{color:#a14336}.phase.failed i{background:#a14336}
  .gauges{display:grid;grid-template-columns:repeat(4,1fr);border-top:1px solid var(--line);padding:16px 8px}.gauges>div{padding:0 14px;border-right:1px solid var(--line)}.gauges>div:last-child{border:0}.gauges span{font-size:11px;color:var(--muted)}.gauges strong{display:block;font:650 24px/1.5 var(--font-display);letter-spacing:-.6px;white-space:nowrap;font-variant-numeric:tabular-nums}.gauges small{font:400 11px var(--font-sans);color:var(--muted);letter-spacing:0;margin-left:5px}.gauges p{font-size:11px;color:var(--muted);line-height:1.5}
  .route-heading{display:flex;justify-content:space-between;gap:12px;padding:12px 22px;background:#faf9f7;border-block:1px solid var(--line);font-size:11px;color:#746d64}.route-heading span:last-child{font-family:ui-monospace,monospace;font-size:10px}
  .flow-scroll{padding:18px 22px 0;overflow:auto}.flow{display:flex;align-items:center;min-width:fit-content}
  .node{flex:1;min-width:180px;background:#fff;border:1px solid var(--line);border-top:3px solid var(--node-color);border-radius:6px;padding:14px;transition:background .15s,border-color .15s}.node.hot{background:#f6f9f5;border-color:var(--node-color)}
  .node-top{display:flex;align-items:center;justify-content:space-between;gap:10px}.node-icon{color:var(--node-color);display:flex}.node-state{font-size:11px;color:#736a60}.node h3{font-size:14px;font-weight:650;margin:12px 0 3px}.node-identity{font:10px ui-monospace,monospace;color:var(--muted)}
  .node-layers{display:flex;align-items:baseline;justify-content:space-between;gap:6px;margin-top:16px}.node-layers span{font-size:11px;color:var(--muted)}.node-layers strong{font:600 18px ui-monospace,monospace;color:var(--node-color)}
  .layer-ticks{display:flex;gap:2px;margin:8px 0 14px}.layer-ticks i{height:6px;background:#efece7;flex:1;border-radius:1px}.layer-ticks i.assigned{background:var(--node-color)}
  dl{margin:0}dl div{display:flex;justify-content:space-between;gap:8px;line-height:2}dt{font-size:11px;color:var(--muted)}dd{font:11px/2 ui-monospace,monospace;margin:0;white-space:nowrap;color:#57514a}dd small{font-size:10px;color:var(--muted)}
  .compute-bar{height:3px;background:#f0ece6;margin-top:10px;border-radius:3px;overflow:hidden}.compute-bar i{display:block;height:100%;background:#a77b61;transition:width .2s}
  .hop{width:70px;flex-shrink:0;text-align:center;color:var(--muted)}.hop>span{font-size:9px}.hop strong{font:10px ui-monospace,monospace;font-weight:400}.wire{height:22px;position:relative;overflow:hidden}.wire:before{content:'';position:absolute;top:11px;height:1px;background:#cfc8bf;left:4px;right:6px}.wire b{position:absolute;right:0;top:-1px;font-size:18px;color:#a39b90}.wire i{display:none;position:absolute;top:9px;left:0;width:7px;height:5px;border-radius:4px;background:#43895c}.flowing .wire i{display:block;animation:packet .55s linear infinite}
  .return-wire{height:28px;min-width:fit-content;border:1px solid #d9d4cd;border-top:0;margin:0 40px 20px;border-radius:0 0 10px 10px;position:relative;text-align:center}.return-wire span{position:relative;top:20px;background:var(--surface);padding:0 10px;color:var(--muted);font-size:10px;white-space:nowrap}.return-wire strong{color:#57514a;font:400 10px ui-monospace,monospace;margin-left:4px}.return-wire>i{display:none}.return-wire.flowing>i{display:block;width:8px;height:3px;background:#43895c;position:absolute;bottom:-2px;animation:return 1s linear infinite}
  .participation{display:flex;gap:9px;align-items:center;padding:12px 22px;border-top:1px solid var(--line);color:var(--muted);font-size:12px;line-height:1.6}.participation strong{color:var(--ink);font-weight:550}
  .graphs{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:4px 22px 18px}.chart{border:1px solid var(--line);border-radius:6px;background:#fff;padding:12px 12px 8px;min-width:0}.chart-heading{display:flex;justify-content:space-between;align-items:baseline;gap:8px}.chart-heading span{font-size:11px;color:var(--muted)}.chart-heading strong{font:600 13px ui-monospace,monospace;color:var(--chart-color)}
  .plot{position:relative;height:90px;margin-top:8px}.plot svg{width:100%;height:100%;overflow:visible}.grid{stroke:#ece8e2;stroke-width:1;stroke-dasharray:3 4;fill:none}.plot polyline{stroke:var(--chart-color);stroke-width:2;fill:none;vector-effect:non-scaling-stroke;stroke-linejoin:round}.area{fill:var(--chart-color);opacity:.08}.plot-empty{position:absolute;inset:0;display:grid;place-items:center;font-size:11px;color:var(--muted);text-align:center}.scale{position:absolute;top:0;right:0;font:9px ui-monospace,monospace;color:var(--muted)}.axis{display:flex;justify-content:space-between;font:10px ui-monospace,monospace;color:var(--muted)}
  .trace{border-top:1px solid var(--line);padding:0 22px}.trace summary{padding:12px 0;color:#45403b;cursor:pointer;font-size:12px}.trace summary span{float:right;color:var(--muted);font-size:11px}.trace p{display:flex;gap:14px;align-items:center;font-size:11px;line-height:1.7}.trace time{color:var(--muted);min-width:48px;font-family:ui-monospace,monospace}.trace strong{font-weight:550;min-width:100px}.trace p>span{flex:1;color:var(--muted)}.trace code{font-size:11px}
  .network footer{padding:10px 22px;background:#faf9f7;color:var(--muted);font-size:10px;line-height:1.7;border-top:1px solid var(--line)}
  .empty{font-size:12px;color:var(--muted);padding:14px 22px 20px;border-top:1px solid var(--line);line-height:1.6}
  @keyframes packet{from{left:-8px}to{left:100%}}@keyframes return{from{right:0}to{right:100%}}@keyframes breathe{50%{opacity:.4}}
  @media(max-width:1150px){.hop{width:52px}.node{min-width:165px}}
  @media(max-width:760px){.network-head{padding:16px;flex-wrap:wrap}.route-heading,.participation,.network footer,.trace,.empty{padding-left:16px;padding-right:16px}.flow-scroll{padding:16px 16px 0}.gauges{grid-template-columns:1fr 1fr;gap:16px 0}.gauges>div:nth-child(2){border:0}.gauges strong{font-size:20px}.graphs{grid-template-columns:1fr;padding:4px 16px 16px}.plot{height:72px}.trace p{flex-wrap:wrap;gap:8px}}
  @media(prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important}.flowing .wire i{left:45%}.return-wire.flowing>i{right:50%}}
</style>
