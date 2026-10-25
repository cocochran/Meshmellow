<script lang="ts">
  import type { MeshSnapshot } from './types.ts';
  import type { EnrollmentState } from './enrollment.ts';
  import Icon from './Icon.svelte';

  let { snapshot, membership, connected }: { snapshot: MeshSnapshot | null; membership: EnrollmentState | null; connected: boolean } = $props();
  const palette = ['#c1e98a', '#efbe7c', '#8cc9e1', '#c7a4e8'];
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
  const phase = $derived(!connected ? 'Connection lost' : live ? run?.phase === 'loading' ? 'Loading model slices' : run?.phase === 'prefill' ? 'Processing prompt' : 'Generating tokens' : run?.phase === 'done' ? 'Request complete' : run?.phase === 'error' ? 'Request failed' : run?.phase === 'cancelled' ? 'Request cancelled' : 'Ready for a request');
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
    { key: 'tps', label: 'Token speed', color: '#c1e98a' },
    { key: 'activationBytes', label: 'Activations received · all hops', color: '#8cc9e1' },
    { key: 'buffers', label: 'Native buffer allocations', color: '#efbe7c' }
  ];
</script>

<section class="network" aria-labelledby="network-title">
  <header class="network-head">
    <div><span class="overline">MESH OBSERVATORY</span><h2 id="network-title">Watch the work move.</h2><p>{run ? 'Actual stages from the latest request. Each hop is a device doing inference.' : 'Your current route. Submit a prompt below to watch devices work together.'}</p></div>
    <span class="phase" class:running={live} class:failed={run?.phase === 'error' || !connected}><i></i>{phase}</span>
  </header>

  <div class="gauges">
    <div><span>Decode speed</span><strong>{run?.decodeTps?.toFixed(1) ?? '—'}<small>tok/s</small></strong><p>{live ? 'Current request average' : 'Latest request average'}</p></div>
    <div><span>Time to first token</span><strong>{seconds(run?.ttftMs)}</strong><p>Includes loading model slices</p></div>
    <div><span>Output tokens</span><strong>{run?.outputTokens ?? '—'}<small>tokens</small></strong><p>{run ? `${seconds(run.elapsedMs)} elapsed` : 'Waiting for a request'}</p></div>
    <div><span>Total host RAM</span><strong>{memoryReports.length ? bytes(totalRam) : '—'}</strong><p>{memoryReports.length} of {membership?.members.filter(m => m.online).length ?? 0} connected devices reporting</p></div>
  </div>

  <div class="route-heading"><span>{run ? 'REQUEST PATH' : 'ASSIGNED PATH'}</span><span>{run?.job ? `JOB ${run.job.slice(0, 10)}` : `${nodes.length} assigned devices`} · {snapshot?.layers ?? '—'} layers</span></div>
  <div class="flow-scroll">
    <div class="flow" style={`--nodes: ${Math.max(1, nodes.length)}`}>
      {#each nodes as node, i (node.id)}
        <article class="node" class:hot={hot(node.observation?.updatedAt)} style={`--node-color:${node.color}`}>
          <div class="node-top"><span class="node-icon"><Icon name={i === 0 ? 'laptop' : i === nodes.length - 1 && nodes.length > 2 ? 'phone' : 'server'} size={21}/></span><span class="node-state">{live ? node.observation?.status === 'computing' ? 'COMPUTING' : node.observation?.status === 'loading' ? 'LOADING' : 'READY' : node.observation?.executions ? 'EXECUTED' : node.online ? 'CONNECTED' : 'OFFLINE'}</span></div>
          <h3>{node.name}</h3><div class="node-identity">{node.id.slice(0, 10)} · {i === 0 ? 'Origin' : i === nodes.length - 1 ? 'Final stage' : 'Middle stage'}</div>
          <div class="node-layers"><span>LAYERS</span><strong>{node.begin ?? '?'}—{node.end == null ? '?' : node.end - 1}</strong><span>{node.begin != null && node.end != null ? node.end - node.begin : '?'} / {snapshot?.layers ?? '?'}</span></div>
          <div class="layer-ticks" aria-hidden="true">{#each Array.from({ length: snapshot?.layers ?? 16 }) as _, layer}<i class:assigned={node.begin != null && node.end != null && layer >= node.begin && layer < node.end}></i>{/each}</div>
          <dl><div><dt>Native compute</dt><dd>{node.observation?.computeMs?.toFixed(1) ?? '—'}<small> ms / step</small></dd></div><div><dt>Buffers</dt><dd>{bytes(node.observation?.bufferBytes)}</dd></div><div><dt>Host RAM</dt><dd>{bytes(node.memory?.totalBytes)}</dd></div><div><dt>Steps executed</dt><dd>{node.observation?.executions ?? 0}</dd></div></dl>
          <div class="compute-bar" title="Latest native compute relative to the slowest stage"><i style={`width:${(node.observation?.computeMs ?? 0) / maxCompute * 100}%`}></i></div>
        </article>
        {#if i < nodes.length - 1}
          <div class="hop" class:flowing={hot(nodes[i + 1].observation?.updatedAt)}><span>ACTIVATIONS</span><div class="wire"><i></i><b>›</b></div><strong>{bytes(nodes[i + 1].observation?.inputBytes)}</strong></div>
        {/if}
      {/each}
      {#if !nodes.length}<p class="empty">Waiting for native workers to join the route.</p>{/if}
    </div>
    {#if nodes.length > 1}<div class="return-wire" class:flowing={run != null && samples.length > 0 && hot(run.startedAt + (samples.at(-1)?.at ?? 0))}><i></i><span>← TOKEN + CONTROL RETURN TO ORIGIN <strong>{bytes(run?.returnBytes)}</strong></span></div>{/if}
  </div>

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
</section>

<style>
  .network{--muted:#a6b8aa;background:#17271f;color:#eef3e9;border:1px solid #314337;border-radius:16px;margin:0 0 24px;overflow:hidden;box-shadow:0 12px 28px #17271f0c}
  .network-head{padding:25px 26px 20px;display:flex;justify-content:space-between;align-items:flex-start;gap:18px}.overline{font-size:10px;letter-spacing:.17em;font-weight:700;color:#c1e98a}.network h2{font-size:26px;letter-spacing:-.8px;margin:7px 0;color:#f2f5ec}.network-head p{font-size:12px;color:var(--muted);margin:0;max-width:470px;line-height:1.6}.phase{font-size:11px;display:flex;align-items:center;gap:7px;white-space:nowrap;border:1px solid #465746;border-radius:20px;padding:8px 11px;color:#c8d8be}.phase i{width:6px;height:6px;border-radius:50%;background:#93ac83}.phase.running i{background:#c1e98a;animation:breathe 1s infinite}.phase.failed{color:#ffbb9e}
  .gauges{display:grid;grid-template-columns:repeat(4,1fr);background:#1e3026;border-block:1px solid #354538;padding:18px 12px}.gauges>div{padding:0 14px;border-right:1px solid #3b4b3c}.gauges>div:last-child{border:0}.gauges span{font-size:11px;color:#c7d1c3}.gauges strong{display:block;font:500 27px/1.7 ui-monospace,SFMono-Regular,monospace;letter-spacing:-1.2px;white-space:nowrap}.gauges small{font:400 11px ui-monospace,monospace;color:#a4b49c;letter-spacing:0;margin-left:6px}.gauges p{font-size:10px;color:var(--muted);margin:0;line-height:1.5}
  .route-heading{display:flex;justify-content:space-between;padding:22px 26px 15px;font:10px ui-monospace,monospace;letter-spacing:.07em;color:var(--muted)}.flow-scroll{padding:0 26px;overflow:auto}.flow{display:flex;align-items:center;min-width:fit-content}.node{flex:1;min-width:180px;background:#20332a;border:1px solid #405244;border-top:3px solid var(--node-color);border-radius:9px;padding:14px;transition:background .15s,box-shadow .15s}.node.hot{background:#2b4331;box-shadow:0 0 18px #c1e98a20;border-color:var(--node-color)}.node-top{display:flex;align-items:center;justify-content:space-between;gap:10px}.node-icon{color:var(--node-color);display:flex}.node-state{font:9px ui-monospace,monospace;color:var(--node-color);letter-spacing:.06em}.node h3{font-size:16px;margin:14px 0 5px;color:#eff4e9}.node-identity{font:10px ui-monospace,monospace;color:var(--muted)}.node-layers{display:flex;align-items:baseline;justify-content:space-between;gap:6px;margin-top:21px}.node-layers span{font:9px ui-monospace,monospace;color:var(--muted)}.node-layers strong{font:22px ui-monospace,monospace;color:var(--node-color)}.layer-ticks{display:flex;gap:3px;margin:9px 0 17px}.layer-ticks i{height:7px;background:#3a4a3e;flex:1;border-radius:1px}.layer-ticks i.assigned{background:var(--node-color)}dl{margin:0}dl div{display:flex;justify-content:space-between;gap:8px;line-height:2.1}dt{font-size:10px;color:var(--muted)}dd{font:11px/2.1 ui-monospace,monospace;margin:0;white-space:nowrap}dd small{font-size:9px;color:var(--muted)}.compute-bar{height:3px;background:#3a4a3e;margin-top:12px;border-radius:3px;overflow:hidden}.compute-bar i{display:block;height:100%;background:var(--node-color);transition:width .2s}
  .hop{width:66px;flex-shrink:0;text-align:center;color:var(--muted)}.hop>span{font:7px ui-monospace,monospace;letter-spacing:.03em}.hop strong{font:9px ui-monospace,monospace}.wire{height:24px;position:relative;overflow:hidden}.wire:before{content:'';position:absolute;top:12px;height:1px;background:#627460;left:0;right:0}.wire b{position:absolute;right:0;top:0;font-size:20px;color:#85947b}.wire i{display:none;position:absolute;top:10px;left:0;width:7px;height:5px;border-radius:4px;background:#c1e98a;box-shadow:0 0 8px #c1e98a}.flowing .wire i{display:block;animation:packet .55s linear infinite}.return-wire{height:33px;min-width:fit-content;border:1px solid #607859;border-top:0;margin:0 30px 24px;border-radius:0 0 12px 12px;position:relative;text-align:center}.return-wire span{position:relative;top:23px;background:#17271f;padding:0 12px;color:#a8be9b;font:8px ui-monospace,monospace;white-space:nowrap}.return-wire strong{color:#d4e4c7;font-weight:400;margin-left:6px}.return-wire>i{display:none}.return-wire.flowing>i{display:block;width:8px;height:3px;background:#c1e98a;position:absolute;bottom:-2px;animation:return 1s linear infinite}
  .participation{display:flex;gap:9px;align-items:center;padding:13px 26px 17px;color:#bcccaf;font-size:11px;line-height:1.6}.participation strong{color:#e0ebd7;font-weight:500}.graphs{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;padding:0 26px 20px}.chart{border:1px solid #3b4c3d;border-radius:8px;background:#1a2b22;padding:13px 12px 9px;min-width:0}.chart-heading{display:flex;flex-direction:column;gap:7px}.chart-heading span{font-size:10px;color:var(--muted)}.chart-heading strong{font:16px ui-monospace,monospace;color:var(--chart-color)}.plot{position:relative;height:100px;margin-top:8px}.plot svg{width:100%;height:100%;overflow:visible}.grid{stroke:#364939;stroke-width:1;stroke-dasharray:3 4;fill:none}.plot polyline{stroke:var(--chart-color);stroke-width:2;fill:none;vector-effect:non-scaling-stroke;stroke-linejoin:round}.area{fill:var(--chart-color);opacity:.07}.plot-empty{position:absolute;inset:0;display:grid;place-items:center;font-size:9px;color:var(--muted);text-align:center}.scale{position:absolute;top:0;right:0;font:8px ui-monospace,monospace;color:var(--muted)}.axis{display:flex;justify-content:space-between;font:9px ui-monospace,monospace;color:var(--muted)}.trace{border-top:1px solid #354538;padding:0 26px}.trace summary{padding:14px 0;color:#ccdac2;cursor:pointer;font-size:11px}.trace summary span{float:right;color:var(--muted);font-size:10px}.trace p{display:flex;gap:14px;align-items:center;font-size:10px;line-height:1.6}.trace time{color:#a7bd97;min-width:44px;font-family:ui-monospace,monospace}.trace strong{font-weight:500;min-width:90px}.trace p>span{flex:1;color:var(--muted)}.trace code{font-size:10px}.network footer{padding:12px 26px;background:#14231a;color:#a6b89c;font-size:9px;line-height:1.7;border-top:1px solid #314337}.empty{font-size:13px;color:var(--muted);padding:20px 0}
  @keyframes packet{from{left:-8px}to{left:100%}}@keyframes return{from{right:0}to{right:100%}}@keyframes breathe{50%{opacity:.4}}
  @media(max-width:1150px){.gauges strong{font-size:23px}.gauges>div{padding:0 10px}.hop{width:48px}.node{min-width:165px}.network-head{flex-wrap:wrap}}
  @media(max-width:700px){.network-head,.route-heading{padding-left:16px;padding-right:16px}.flow-scroll{padding:0 16px}.gauges{grid-template-columns:1fr 1fr;gap:18px}.gauges>div:nth-child(2){border:0}.graphs{grid-template-columns:1fr;padding:0 16px 16px}.chart-heading{flex-direction:row;align-items:center;justify-content:space-between}.plot{height:80px}.participation,.network footer{padding-left:16px;padding-right:16px}.trace{padding:0 16px}.trace p{flex-wrap:wrap;gap:8px}.network h2{font-size:23px}.route-heading{font-size:9px;gap:8px}.node{min-width:180px}}
  @media(prefers-reduced-motion:reduce){*,*::before,*::after{animation:none!important;transition:none!important}.flowing .wire i{left:45%}.return-wire.flowing>i{right:50%}}
</style>
