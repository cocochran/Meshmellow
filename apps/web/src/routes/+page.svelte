<script lang="ts">
  import { onMount } from 'svelte';
  import Enrollment from '#lib/Enrollment.svelte';
  import NetworkView from '#lib/NetworkView.svelte';
  import type { EnrollmentState } from '#lib/enrollment.ts';
  import Icon from '#lib/Icon.svelte';
  import { defaultPreferences, type MeshSnapshot, type Preferences, type Device } from '#lib/types.ts';

  const sections = [
    { id: 'overview', label: 'Overview', icon: 'grid' },
    { id: 'devices', label: 'Devices', icon: 'devices' },
    { id: 'model', label: 'Model', icon: 'model' },
    { id: 'activity', label: 'Activity', icon: 'activity' },
    { id: 'settings', label: 'Settings', icon: 'settings' }
  ];
  const colors = ['#42685c', '#89602f', '#526a8d', '#865567', '#625d8d'];
  let section = $state('overview');
  let snapshot = $state<MeshSnapshot | null>(null);
  let preferences = $state<Preferences>({ ...defaultPreferences });
  let displayName = $state(defaultPreferences.displayName);
  let refreshSeconds = $state(5);
  let loading = $state(true);
  let refreshing = $state(false);
  let error = $state('');
  let notice = $state('');
  let saving = $state(false);
  let prompt = $state('What is 2 plus 2? Answer briefly.');
  let completion = $state('');
  let generationError = $state('');
  let generating = $state(false);
  let generationController: AbortController | null = null;
  async function generate(event: SubmitEvent) {
    event.preventDefault();
    if (generating || !prompt.trim()) return;
    generating = true; completion = ''; generationError = '';
    document.getElementById('network-title')?.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' });
    const controller = new AbortController();
    generationController = controller;
    try {
      const response = await fetch('/v1/chat/completions', {
        method: 'POST', signal: controller.signal,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'meshthing-shard', messages: [{ role: 'user', content: prompt.trim() }], max_tokens: 32, stream: false })
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error?.message || result.error || 'Shared inference failed.');
      completion = result.choices?.[0]?.message?.content ?? '';
      await refresh();
    } catch (e) {
      generationError = e instanceof Error && e.name === 'AbortError' ? 'Request cancelled.' : e instanceof Error ? e.message : 'Shared inference failed.';
    } finally { generating = false; generationController = null; }
  }
  let detailDialog: HTMLDialogElement;
  let enrollmentPanel: Enrollment;
  let membership = $state<EnrollmentState | null>(null);
  let selectedId = $state<string | null>(null);
  let deviceQuery = $state('');
  let deviceOrder = $state('route');
  let pollController: AbortController | null = null;
  let mounted = false;
  const devices = $derived(snapshot?.devices ?? []);
  const peers = $derived(snapshot?.peers ?? []);
  const deviceCount = $derived(membership?.members.length ?? devices.length);
  const connected = $derived(snapshot?.connected === true && !error);
  const runHeading = $derived(!connected ? 'Runtime unavailable' : snapshot?.active ? 'A generation is running' : snapshot?.event === 'done' ? 'Latest run finished' : snapshot?.event === 'error' ? 'Latest run failed' : snapshot?.event === 'paused' ? 'Runtime is paused' : 'Runtime is idle');
  const measuredDevices = $derived(devices.filter((d) => d.computeMs !== null));
  const maxCompute = $derived(Math.max(1, ...measuredDevices.map((d) => d.computeMs ?? 0)));
  const visibleDevices = $derived.by(() => {
    const query = deviceQuery.trim().toLowerCase();
    const filtered = devices.filter((d) => `${d.name} ${d.hardware}`.toLowerCase().includes(query));
    return deviceOrder === 'compute' ? [...filtered].sort((a, b) => (b.computeMs ?? -1) - (a.computeMs ?? -1)) : filtered;
  });
  const selectedDevice = $derived(devices.find((d) => d.id === selectedId));
  const observedBuffers = $derived(devices.reduce((sum, d) => sum + (d.bufferBytes ?? 0), 0));
  const allBuffersKnown = $derived(devices.length > 0 && devices.every((d) => d.bufferBytes !== null));
  const rangeLabel = (d: Device) => d.begin !== null && d.end !== null ? `${d.begin}–${d.end - 1}` : 'Unreported';
  const sizeLabel = (bytes: number | null) => bytes === null ? '—' : `${(bytes / 1024 ** 3).toFixed(1)} GiB`;
  const timeLabel = (timestamp: number | null) => timestamp === null ? 'Not reported' : new Date(timestamp).toLocaleTimeString();
  const iconFor = (d: Device) => /ipad|tablet/i.test(d.name) ? 'tablet' : /galaxy|pixel|phone/i.test(d.name) ? 'phone' : /cuda|rtx/i.test(d.hardware) ? 'server' : 'laptop';
  const statusLabel = (d: Device) => !connected ? 'Unavailable' : snapshot?.source === 'core' ? (d.status === 'offline' ? 'Offline' : 'Connected') : !snapshot?.active ? 'Last observed' : d.status === 'inflight' ? 'Computing' : d.status === 'loading' ? 'Loading' : 'In current run';

  async function refresh() {
    if (refreshing) return;
    refreshing = true;
    const controller = new AbortController();
    pollController = controller;
    const timeout = setTimeout(() => controller.abort(), 6000);
    try {
      const response = await fetch('/api/mesh', { signal: controller.signal, cache: 'no-store' });
      if (!response.ok) throw new Error('The dashboard could not refresh. Try again.');
      const next: MeshSnapshot = await response.json();
      if (mounted) { snapshot = next; error = ''; }
    } catch {
      if (mounted) error = 'The dashboard could not refresh. Previous measurements may be out of date.';
    } finally {
      clearTimeout(timeout);
      if (mounted) { refreshing = false; loading = false; }
    }
  }
  onMount(() => {
    mounted = true;
    let timer: ReturnType<typeof setTimeout>;
    const tick = async () => {
      if (document.visibilityState === 'visible') await refresh();
      if (mounted) timer = setTimeout(tick, section === 'overview' ? (generating || snapshot?.active ? 200 : 1000) : preferences.refreshSeconds * 1000);
    };
    void tick();
    const controller = new AbortController();
    fetch('/api/preferences', { signal: controller.signal }).then(async (r) => {
      if (!r.ok) throw new Error();
      const saved: Preferences = await r.json();
      if (mounted) { preferences = saved; displayName = saved.displayName; refreshSeconds = saved.refreshSeconds; }
    }).catch(() => { if (mounted) notice = 'Saved preferences could not be loaded. Using defaults.'; });
    return () => { mounted = false; clearTimeout(timer); controller.abort(); pollController?.abort(); generationController?.abort(); };
  });
  function showDevice(device: Device) { selectedId = device.id; detailDialog.showModal(); }
  function showPairing() { void enrollmentPanel.showInvitation(); }
  async function saveSettings(event: SubmitEvent) {
    event.preventDefault(); saving = true; notice = '';
    try {
      const response = await fetch('/api/preferences', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ displayName, refreshSeconds }) });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Could not save preferences.');
      preferences = result; displayName = result.displayName;
      notice = 'Preferences saved on this computer.';
    } catch (e) { notice = e instanceof Error ? e.message : 'Could not save preferences.'; }
    finally { saving = false; }
  }
  async function copyEndpoint() {
    try { await navigator.clipboard.writeText(snapshot?.endpoint || ''); notice = 'Runtime address copied.'; }
    catch { notice = 'Could not copy. Select the runtime address to copy it manually.'; }
  }
</script>

<svelte:head><title>Layercake · {membership?.mesh?.name || 'Welcome'}</title><meta name="description" content="Monitor devices, model layers, and runtime performance in your Layercake network." /></svelte:head>

<a class="skip-link" href="#main-content">Skip to content</a>
<div class="app-shell">
  <aside class="sidebar">
    <a class="brand" href="/" aria-label="Layercake overview"><span class="brand-mark" aria-hidden="true"><img src="/layercake-logo.png" width="41" height="41" alt="" /></span><span>Layercake</span></a>
    <div class="workspace"><span class="workspace-icon">L</span><div><strong>{membership?.mesh?.name || 'Welcome'}</strong><span>Local workspace</span></div></div>
    <span class="eyebrow sidebar-label">WORKSPACE</span>
    <nav aria-label="Main navigation">
      {#each sections as item}
        <button class:current={section === item.id} disabled={!membership?.mesh && item.id !== 'settings' && item.id !== 'overview'} aria-current={section === item.id ? 'page' : undefined} onclick={() => { section = item.id; notice = ''; }}><Icon name={item.icon} /><span>{item.label}</span>{#if item.id === 'devices' && deviceCount}<span class="nav-count">{deviceCount}</span>{/if}</button>
      {/each}
    </nav>
    <div class="sidebar-bottom">
      <div class="sidebar-runtime"><span class="eyebrow">RUNTIME</span><strong><span class="dot" class:disconnected={!connected}></span>{loading ? 'Connecting…' : connected ? 'Connected' : 'Disconnected'}</strong><span>Core runtime · This device</span></div>
      <div class="local-server">Layercake <span class="mono">v0.1</span></div>
    </div>
  </aside>

  <div class="main-column">
    <header class="topbar"><div class="breadcrumb">{membership?.mesh?.name || 'Welcome'}<span>/</span><strong>{sections.find((s) => s.id === section)?.label}</strong></div><div class="topbar-right"><span class="connection" class:offline={!connected}><span class="dot"></span>{loading ? 'Connecting' : connected ? 'Runtime connected' : 'Runtime disconnected'}</span></div></header>
    <main id="main-content">
      <div class="page-heading"><div><h1>{section === 'overview' ? 'Overview' : section === 'devices' ? 'Devices' : section === 'model' ? 'Model distribution' : section === 'activity' ? 'Activity' : 'Settings'}</h1><p>{section === 'overview' ? 'Your model, and the devices running it.' : section === 'devices' ? 'Inspect layer assignments and compare stage compute times.' : section === 'model' ? 'Layer placement across the configured route.' : section === 'activity' ? 'Requests and the latest runtime state.' : 'Manage local preferences and runtime connection.'}</p></div>{#if membership?.mesh}<button class="button secondary" onclick={showPairing}><Icon name="plus" size={16} /> Add device</button>{/if}</div>

      {#if membership?.mesh || section === 'settings'}
      {#if notice}<div class="notice" role="status">{notice}<button aria-label="Dismiss message" onclick={() => notice = ''}><Icon name="close" size={16}/></button></div>{/if}
      {#if error || (!loading && !connected)}<div class="connection-message" role="status"><Icon name="info" /><div><strong>{error ? 'Refresh interrupted' : 'Your runtime isn’t connected'}</strong><p>{error || snapshot?.message}</p>{#if !snapshot?.endpoint}<code>./mesh-ui</code>{/if}</div><button class="text-button" disabled={refreshing} onclick={refresh}>Retry <Icon name="refresh" size={15}/></button></div>{/if}

      {#if section === 'overview'}
        <NetworkView {snapshot} {membership} {connected}/>
        <section class="panel" aria-labelledby="inference-title">
          <div class="panel-heading"><div><h2 id="inference-title">Ask the mesh</h2><p>{snapshot?.message || 'Waiting for contributing clients.'}</p></div></div>
          <form class="settings-form" onsubmit={generate}>
            <label for="mesh-prompt">Prompt</label>
            <textarea id="mesh-prompt" bind:value={prompt} rows="3" maxlength="2048" required></textarea>
            <div><button class="button primary" disabled={generating || !(snapshot?.inferenceReady ?? snapshot?.event === 'ready')}>{generating ? 'Running across devices…' : 'Run shared inference'}</button>
            {#if generating}<button type="button" class="button secondary" onclick={() => generationController?.abort()}>Cancel</button>{/if}</div>
            {#if generationError}<p role="alert">{generationError}</p>{/if}
            {#if completion}<div class="completion" role="status">{completion}</div>{/if}
          </form>
        </section>
      {/if}

      {#if section === 'overview' || section === 'devices'}
        <section class="panel device-panel"><div class="panel-heading"><div><h2>{'Devices'} <span class="count-pill">{snapshot?.source === 'core' && !snapshot.peerStatusAvailable ? '—' : deviceCount}</span></h2><p>{snapshot?.source === 'core' ? 'Joined devices appear automatically. Slice assignments appear after native compute is ready.' : snapshot?.active ? 'Current run · latest reported measurement per stage' : 'Last reported measurements · not a live availability check'}</p></div><button class="text-button" disabled={refreshing} onclick={refresh}><Icon name="refresh" size={15}/>{refreshing ? 'Refreshing…' : 'Refresh'}</button></div>
          {#if snapshot?.source !== 'core' && devices.length}<div class="table-toolbar"><label class="device-search"><Icon name="devices" size={16}/><input aria-label="Filter devices" type="search" bind:value={deviceQuery} placeholder="Filter devices…" /></label><select aria-label="Device order" bind:value={deviceOrder}><option value="route">Route order</option><option value="compute">Slowest compute first</option></select></div>{/if}
          {#if loading}<div class="empty-state"><span class="loading-orbit"></span><h3>Reading runtime</h3><p>Waiting for the first observation.</p></div>
          {:else if snapshot?.source === 'core' && !snapshot.peerStatusAvailable}<div class="empty-state"><Icon name="info" size={35}/><h3>Peer status isn’t available</h3><p>This core version exposes its job queue, but not connected-peer status.</p></div>
          {:else if snapshot?.source === 'core' && !membership?.members.length}<div class="empty-state"><Icon name="devices" size={35}/><h3>No peers connected</h3><p>Peers appear here when they establish an authenticated control link.</p></div>
          {:else if snapshot?.source === 'core'}<div class="job-list">{#each membership?.members ?? [] as member}{@const stage = devices.find(d => d.id === member.id)}<div class="job-row"><strong>{member.name}{member.self ? ' (this device)' : ''}</strong><span class="status-pill">{member.online ? 'Connected' : 'Offline'}</span><span>{stage ? `Layers ${rangeLabel(stage)}` : 'No model slice assigned yet'}</span><code>{member.id.slice(0, 12)}</code></div>{/each}</div>
          {:else if !devices.length}<div class="empty-state"><Icon name="devices" size={30}/><h3>No devices reported</h3><p>Connect the runtime to see its configured devices and layer assignments.</p></div>
          {:else if !visibleDevices.length}<div class="empty-state compact"><h3>No matching devices</h3><p>Try a different device name or hardware type.</p><button class="text-button" onclick={() => deviceQuery = ''}>Clear filter</button></div>
          {:else}
          <!-- The table scrolls horizontally on narrow screens; keyboard users need to focus its scroll region. -->
          <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
          <div class="device-table" tabindex="0" role="region" aria-label="Device measurements"><div class="table-head"><span>Device</span><span>State</span><span>Layers</span><span>Native compute <span class="column-hint">latest</span></span><span>Buffers</span><span></span></div>{#each visibleDevices as device}<button class="device-row" onclick={() => showDevice(device)} aria-label={`View ${device.name}`}><span class="device-name"><span class="device-icon" style={`--device-color: ${colors[devices.indexOf(device) % colors.length]}`}><Icon name={iconFor(device)} size={20}/></span><span><strong>{device.name}</strong><small>{device.hardware}</small></span></span><span class="status-pill" class:computing={snapshot?.active && device.status === 'inflight'}><span class="dot"></span>{statusLabel(device)}</span><span class="mono range-cell">{rangeLabel(device)}</span><span class="compute-cell"><span class="mono">{device.computeMs?.toFixed(1) ?? '—'}<small>{device.computeMs !== null ? ' ms' : ''}</small></span><span class="compute-track" aria-hidden="true"><i style={`width: ${device.computeMs === null ? 0 : device.computeMs / maxCompute * 100}%`}></i></span></span><span class="mono buffer-cell">{sizeLabel(device.bufferBytes)}</span><Icon name="chevron" size={15}/></button>{/each}</div>{/if}
          {#if snapshot?.source !== 'core'}<div class="panel-footer"><Icon name="info" size={14}/><span>Compute time excludes network transfer and waiting. Buffers are reported allocations, not total device memory.</span></div>{/if}
        </section>
      {/if}

      {#if section === 'overview' || section === 'model'}
        <section class="panel model-panel"><div class="panel-heading"><div><h2>Layer allocation</h2><p>{snapshot?.model || 'No model configured'} · {snapshot?.layers ? `${snapshot.layers} decoder layers` : 'Layer count not reported'}</p></div><span class="subtle-tag">{connected ? 'Runtime configuration' : 'Not connected'}</span></div>
          {#if devices.length}<div class="layer-map" aria-label="Layer distribution">{#each devices as device, i}<button style={`--device-color: ${colors[i % colors.length]}; flex: ${device.end !== null && device.begin !== null ? Math.max(1, device.end - device.begin) : 1}`} aria-label={`${device.name}, layers ${rangeLabel(device)}`} onclick={() => showDevice(device)}><span>{rangeLabel(device)}</span></button>{/each}</div><div class="layer-legend">{#each devices as device, i}<span><i style={`background: ${colors[i % colors.length]}`}></i>{device.name}</span>{/each}</div>{:else}<div class="empty-inline">Layer assignments appear when your runtime reports a configured model.</div>{/if}
          {#if section === 'model'}<div class="info-grid"><div><span class="eyebrow">PLACEMENT</span><h3>Placement</h3><p>Device benchmarks will determine which layers fit and which route runs best. Current assignments come from your runtime configuration.</p></div><div><span class="eyebrow">MODEL MANAGEMENT</span><h3>Model management</h3><p>Each contributing client loads its assigned slice. The execution route above shows the configured layer ranges.</p></div></div>{/if}
        </section>
      {/if}

      {#if section === 'activity'}
        <section class="panel"><div class="panel-heading"><div><h2>{snapshot?.source === 'core' ? 'Request queue' : 'Latest runtime observation'}</h2><p>Read from the selected local runtime.</p></div><button class="text-button" onclick={refresh} disabled={refreshing}><Icon name="refresh" size={15}/>Refresh</button></div>
          {#if snapshot?.source === 'core'}{#if snapshot.jobs.length}<div class="job-list">{#each snapshot.jobs as job}<div class="job-row"><code>{job.id}</code><span class="status-pill">{job.status}</span><span>{job.createdAt ? new Date(job.createdAt).toLocaleTimeString() : 'Unknown time'}</span></div>{/each}</div>{:else}<div class="empty-state"><Icon name="activity" size={32}/><h3>No queued requests to show</h3><p>{connected ? 'Requests will appear here when submitted to the core API.' : 'Connect the core API to read its queue.'}</p></div>{/if}
          {:else}<div class="run-summary"><span class="device-icon"><Icon name="activity" size={28}/></span><div><h3>{runHeading}</h3><p>{snapshot?.outputSteps ?? '—'} reported output steps · {snapshot?.decodeRate?.toFixed(2) ?? '—'} decode steps / sec</p><p class="fine-print">Stop reason is not supplied by this endpoint. Finished does not imply natural end-of-generation.</p></div></div>{/if}
        </section>
      {/if}

      {#if section === 'settings'}
        <div class="settings-grid"><section class="panel"><div class="panel-heading"><div><h2>Workspace preferences</h2><p>Saved locally. These do not change mesh membership.</p></div></div><form class="settings-form" onsubmit={saveSettings}><label for="display-name">Workspace display name</label><input id="display-name" bind:value={displayName} maxlength="48" required /><p>A friendly name for this web interface.</p><label for="refresh-interval">Refresh interval</label><select id="refresh-interval" bind:value={refreshSeconds}><option value={2}>Every 2 seconds</option><option value={5}>Every 5 seconds</option><option value={10}>Every 10 seconds</option><option value={30}>Every 30 seconds</option></select><p>Polling pauses while this browser tab is hidden.</p><button class="button primary" disabled={saving}>{saving ? 'Saving…' : 'Save preferences'}<Icon name="check" size={16}/></button></form></section>
        <section class="panel"><div class="panel-heading"><div><h2>Runtime connection</h2><p>The core runtime owns this dashboard and its API.</p></div></div><div class="connection-details"><span class="eyebrow">LOCAL ENDPOINT</span><div class="endpoint"><code>{snapshot?.endpoint || 'Not configured'}</code>{#if snapshot?.endpoint}<button aria-label="Copy runtime address" onclick={copyEndpoint}><Icon name="copy" size={17}/></button>{/if}</div><dl><div><dt>Source</dt><dd>{snapshot?.source === 'core' ? 'Core API · job queue' : 'Live runtime · stage telemetry'}</dd></div><div><dt>Access</dt><dd>This device only</dd></div><div><dt>Pairing</dt><dd>Signed invitations + approval</dd></div></dl><p class="fine-print">API URLs and inference readiness appear with each trusted device. Stopping the runtime also stops this dashboard.</p></div></section></div>
      {/if}

      {/if}
      <Enrollment bind:this={enrollmentPanel} stages={devices} onchange={value => membership = value}/>
      <footer class="page-footer"><span>Local mesh · Iroh</span><span>{snapshot ? `Last checked ${new Date(snapshot.checkedAt).toLocaleTimeString()}` : 'Waiting for first observation'}</span></footer>
    </main>
  </div>
</div>

<dialog bind:this={detailDialog} onclose={() => selectedId = null} aria-labelledby="device-title"><div class="dialog-heading"><span class="eyebrow">DEVICE DETAILS</span><button aria-label="Close device details" onclick={() => detailDialog.close()}><Icon name="close"/></button></div>{#if selectedDevice}<div class="dialog-device-icon"><Icon name={iconFor(selectedDevice)} size={36}/></div><h2 id="device-title">{selectedDevice.name}</h2><p>{selectedDevice.hardware}</p><dl class="detail-list"><div><dt>Layer range</dt><dd>{rangeLabel(selectedDevice)}</dd></div><div><dt>Observation</dt><dd>{statusLabel(selectedDevice)}</dd></div><div><dt>Last native compute</dt><dd>{selectedDevice.computeMs?.toFixed(1) ?? '—'} ms</dd></div><div><dt>Observed buffers</dt><dd>{sizeLabel(selectedDevice.bufferBytes)}</dd></div><div><dt>Stage executions</dt><dd>{selectedDevice.executions}</dd></div></dl><div class="soft-note"><Icon name="info" size={18}/><p>These are runtime measurements, not a full memory budget or a device benchmark. Trust status is not reported yet.</p></div>{:else}<h2 id="device-title">Device no longer reported</h2><p>Refresh the mesh to see the current route.</p>{/if}<button class="button secondary full-width" onclick={() => detailDialog.close()}>Done</button></dialog>

<style>
textarea{font:inherit;width:100%;box-sizing:border-box;border:1px solid #d9ccc0;border-radius:8px;padding:12px;background:#fffaf3;color:#4d3326;resize:vertical}.completion{white-space:pre-wrap;padding:16px;border-radius:8px;background:#f1f5ef;line-height:1.6}
</style>
