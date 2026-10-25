<script lang="ts">
  import { onMount } from 'svelte';
  import Icon from '#lib/Icon.svelte';
  import { defaultPreferences, type MeshSnapshot, type Preferences, type Device } from '#lib/types.ts';

  const sections = [
    { id: 'overview', label: 'Overview', icon: 'grid' },
    { id: 'devices', label: 'Devices', icon: 'devices' },
    { id: 'model', label: 'Model', icon: 'model' },
    { id: 'activity', label: 'Activity', icon: 'activity' },
    { id: 'settings', label: 'Settings', icon: 'settings' }
  ];
  const colors = ['#769b72', '#c2b178', '#93a7ba', '#b79bad', '#7faaa3'];
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
  let detailDialog: HTMLDialogElement;
  let pairDialog: HTMLDialogElement;
  let selectedId = $state<string | null>(null);
  let pairingTab = $state<'invite' | 'join'>('invite');
  let emojiCode = $state('');
  let pollController: AbortController | null = null;
  let mounted = false;
  const devices = $derived(snapshot?.devices ?? []);
  const connected = $derived(snapshot?.connected === true && !error);
  const runHeading = $derived(!connected ? 'Runtime unavailable' : snapshot?.active ? 'A generation is running' : snapshot?.event === 'done' ? 'Latest run finished' : snapshot?.event === 'error' ? 'Latest run failed' : snapshot?.event === 'paused' ? 'Runtime is paused' : 'Runtime is idle');
  const selectedDevice = $derived(devices.find((d) => d.id === selectedId));
  const observedBuffers = $derived(devices.reduce((sum, d) => sum + (d.bufferBytes ?? 0), 0));
  const allBuffersKnown = $derived(devices.length > 0 && devices.every((d) => d.bufferBytes !== null));
  const rangeLabel = (d: Device) => d.begin !== null && d.end !== null ? `${d.begin}–${d.end - 1}` : 'Unreported';
  const sizeLabel = (bytes: number | null) => bytes === null ? '—' : `${(bytes / 1024 ** 3).toFixed(1)} GiB`;
  const iconFor = (d: Device) => /ipad|tablet/i.test(d.name) ? 'tablet' : /galaxy|pixel|phone/i.test(d.name) ? 'phone' : /cuda|rtx/i.test(d.hardware) ? 'server' : 'laptop';
  const statusLabel = (d: Device) => !connected ? 'Unavailable' : !snapshot?.active ? 'Last observed' : d.status === 'inflight' ? 'Computing' : d.status === 'loading' ? 'Loading' : 'In current run';

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
      if (mounted) timer = setTimeout(tick, preferences.refreshSeconds * 1000);
    };
    void tick();
    const controller = new AbortController();
    fetch('/api/preferences', { signal: controller.signal }).then(async (r) => {
      if (!r.ok) throw new Error();
      const saved: Preferences = await r.json();
      if (mounted) { preferences = saved; displayName = saved.displayName; refreshSeconds = saved.refreshSeconds; }
    }).catch(() => { if (mounted) notice = 'Saved preferences could not be loaded. Using defaults.'; });
    return () => { mounted = false; clearTimeout(timer); controller.abort(); pollController?.abort(); };
  });
  function showDevice(device: Device) { selectedId = device.id; detailDialog.showModal(); }
  function showPairing() { emojiCode = ''; pairingTab = 'invite'; pairDialog.showModal(); }
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

<svelte:head><title>Layercake · {preferences.displayName}</title><meta name="description" content="Your devices, one model. A local control panel for your Layercake network." /></svelte:head>

<div class="app-shell">
  <aside class="sidebar">
    <a class="brand" href="/" aria-label="Layercake overview"><img src="/layercake-logo.png" width="33" height="33" alt="" /><span>layercake<span class="brand-dot">.</span></span></a>
    <div class="workspace"><span class="workspace-icon">L</span><div><strong>{preferences.displayName}</strong><span>Local workspace</span></div><span class="workspace-dot"></span></div>
    <span class="eyebrow sidebar-label">WORKSPACE</span>
    <nav aria-label="Main navigation">
      {#each sections as item}
        <button class:current={section === item.id} aria-current={section === item.id ? 'page' : undefined} onclick={() => { section = item.id; notice = ''; }}><Icon name={item.icon} /><span>{item.label}</span>{#if item.id === 'devices' && devices.length}<span class="nav-count">{devices.length}</span>{/if}</button>
      {/each}
    </nav>
    <div class="sidebar-bottom">
      <div class="local-note"><Icon name="leaf" size={23} /><strong>Better together.</strong><p>Put the devices you already own to work.</p></div>
      <div class="local-server"><span class="dot"></span> Local web UI <span class="mono">v0.1</span></div>
    </div>
  </aside>

  <div class="main-column">
    <header class="topbar"><div class="cake-trim" aria-hidden="true"><i></i><i></i><i></i><img src="/layercake-logo.png" alt="" width="36" height="32" /></div><div class="breadcrumb">Workspace <span>/</span> <strong>{sections.find((s) => s.id === section)?.label}</strong></div><div class="topbar-right"><span class="connection" class:offline={!connected}><span class="dot"></span>{loading ? 'Connecting' : connected ? 'Runtime connected' : 'Runtime disconnected'}</span><span class="avatar" aria-label="Local workspace">L</span></div></header>
    <main id="main-content">
      <div class="page-heading"><div><span class="eyebrow">YOUR PERSONAL COMPUTE NETWORK</span><h1>{section === 'overview' ? 'Your mesh, at a glance.' : section === 'devices' ? 'Every device has a part.' : section === 'model' ? 'One model. Shared compute.' : section === 'activity' ? 'What’s happening.' : 'Make yourself at home.'}</h1><p>{section === 'overview' ? 'A little from each device. More possibilities together.' : section === 'devices' ? 'Explore the stages reported by your runtime.' : section === 'model' ? 'See how your model is distributed across the mesh.' : section === 'activity' ? 'Follow the latest run and queued requests.' : 'Preferences for this computer’s web interface.'}</p></div><button class="button primary" onclick={showPairing}><Icon name="plus" size={17} /> Add device</button></div>

      {#if notice}<div class="notice" role="status">{notice}<button aria-label="Dismiss message" onclick={() => notice = ''}><Icon name="close" size={16}/></button></div>{/if}
      {#if error || (!loading && !connected)}<div class="connection-message" role="status"><Icon name="info" /><div><strong>{error ? 'Refresh interrupted' : 'Your runtime isn’t connected'}</strong><p>{error || snapshot?.message}</p>{#if !snapshot?.endpoint}<code>./mesh-ui --runtime http://127.0.0.1:8095</code>{/if}</div><button class="text-button" disabled={refreshing} onclick={refresh}>Retry <Icon name="refresh" size={15}/></button></div>{/if}

      {#if section === 'overview'}
        <section class="hero">
          <div class="hero-copy"><span class="hero-kicker"><span class="dot"></span> ONE MESH · ONE MODEL</span><h2>Your devices.<br/>A bigger picture.</h2><p>Bring your compute together and give larger models room to run.</p><button class="button light" onclick={() => section = 'model'}>Explore your model <Icon name="arrow" size={16}/></button></div>
          <div class="mesh-art" aria-hidden="true"><div class="orbit orbit-one"></div><div class="orbit orbit-two"></div><svg viewBox="0 0 420 260"><path d="M210 130 90 65 M210 130 330 65 M210 130 75 205 M210 130 345 200" /></svg><span class="art-node art-mac"><Icon name="laptop" size={31}/></span><span class="art-node art-phone"><Icon name="phone" size={28}/></span><span class="art-node art-tablet"><Icon name="tablet" size={29}/></span><span class="art-node art-server"><Icon name="server" size={30}/></span><span class="art-center"><Icon name="model" size={38}/></span><span class="art-caption">MANY DEVICES. ONE POSSIBILITY.</span></div>
        </section>
        <div class="metrics-grid">
          <div class="metric"><span>Configured stages <Icon name="devices" size={18}/></span><strong>{loading ? '—' : devices.length}<small>devices</small></strong><p>Membership and liveness not yet reported</p></div>
          <div class="metric"><span>Latest decode rate <Icon name="activity" size={18}/></span><strong>{snapshot?.decodeRate?.toFixed(2) ?? '—'}<small>steps / sec</small></strong><p>{snapshot?.active ? 'Reported by the current run' : 'Last run · not a live speed estimate'}</p></div>
          <div class="metric"><span>Observed buffers <Icon name="model" size={18}/></span><strong>{allBuffersKnown ? (observedBuffers / 1024 ** 3).toFixed(1) : '—'}<small>GiB</small></strong><p>Reported stage buffers · not total memory</p></div>
        </div>
      {/if}

      {#if section === 'overview' || section === 'devices'}
        <section class="panel device-panel"><div class="panel-heading"><div><h2>Your devices <span class="count-pill">{devices.length}</span></h2><p>{snapshot?.active ? 'Stages in the current run.' : 'Configured route and last reported measurements.'}</p></div><button class="text-button" disabled={refreshing} onclick={refresh}><Icon name="refresh" size={15}/>{refreshing ? 'Refreshing…' : 'Refresh'}</button></div>
          {#if loading}<div class="empty-state"><span class="loading-orbit"></span><h3>Finding your mesh</h3><p>Reading the local runtime.</p></div>
          {:else if !devices.length}<div class="empty-state"><Icon name="devices" size={35}/><h3>{snapshot?.source === 'core' && connected ? 'Device telemetry isn’t available yet' : 'Your devices will appear here'}</h3><p>{snapshot?.source === 'core' && connected ? 'This core exposes its job queue. Device and layer telemetry will follow.' : 'Connect the runtime to see its configured devices and layer assignments.'}</p></div>
          {:else}<div class="device-table"><div class="table-head"><span>DEVICE</span><span>OBSERVATION</span><span>LAYERS</span><span>BUFFERS</span><span></span></div>{#each devices as device, i}<button class="device-row" onclick={() => showDevice(device)} aria-label={`View ${device.name}`}><span class="device-name"><span class="device-icon" style={`--device-color: ${colors[i % colors.length]}`}><Icon name={iconFor(device)} size={23}/></span><span><strong>{device.name}</strong><small>{device.hardware}</small></span></span><span class="status-pill" class:computing={snapshot?.active && device.status === 'inflight'}><span class="dot"></span>{statusLabel(device)}</span><span class="mono range-cell">{rangeLabel(device)}</span><span class="mono buffer-cell">{sizeLabel(device.bufferBytes)}</span><Icon name="chevron" size={16}/></button>{/each}</div>{/if}
          <div class="panel-footer"><Icon name="info" size={14}/><span>Devices contribute contiguous layers. A configured stage does not confirm a device is online.</span></div>
        </section>
      {/if}

      {#if section === 'overview' || section === 'model'}
        <section class="panel model-panel"><div class="panel-heading"><div><h2>One model, across your mesh</h2><p>{snapshot?.model || 'Mesh model'} · {snapshot?.layers ? `${snapshot.layers} decoder layers` : 'Layer count not reported'}</p></div><span class="subtle-tag">{connected ? 'Runtime configuration' : 'Not connected'}</span></div>
          {#if devices.length}<div class="layer-map" aria-label="Layer distribution">{#each devices as device, i}<button style={`--device-color: ${colors[i % colors.length]}; flex: ${device.end !== null && device.begin !== null ? Math.max(1, device.end - device.begin) : 1}`} aria-label={`${device.name}, layers ${rangeLabel(device)}`} onclick={() => showDevice(device)}><span>{rangeLabel(device)}</span></button>{/each}</div><div class="layer-legend">{#each devices as device, i}<span><i style={`background: ${colors[i % colors.length]}`}></i>{device.name}</span>{/each}</div>{:else}<div class="empty-inline">Layer assignments appear when your runtime reports a configured model.</div>{/if}
          {#if section === 'model'}<div class="info-grid"><div><span class="eyebrow">PLACEMENT</span><h3>Measured, then assigned.</h3><p>Device benchmarks will determine which layers fit and which route runs best. Current assignments come from your runtime configuration.</p></div><div><span class="eyebrow">MODEL MANAGEMENT</span><h3>More room to grow.</h3><p>Model import, downloads and capacity recommendations are waiting for core integration. This UI does not change the running model.</p></div></div>{/if}
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
        <section class="panel"><div class="panel-heading"><div><h2>Runtime connection</h2><p>Configured when you launch the web server.</p></div></div><div class="connection-details"><span class="eyebrow">LOCAL ENDPOINT</span><div class="endpoint"><code>{snapshot?.endpoint || 'Not configured'}</code>{#if snapshot?.endpoint}<button aria-label="Copy runtime address" onclick={copyEndpoint}><Icon name="copy" size={17}/></button>{/if}</div><dl><div><dt>Source</dt><dd>{snapshot?.source === 'core' ? 'Core API · job queue' : 'Live runtime · stage telemetry'}</dd></div><div><dt>Access</dt><dd>Read-only</dd></div><div><dt>Pairing</dt><dd>Awaiting core support</dd></div></dl><p class="fine-print">Start with <code>--runtime</code> and <code>--source</code> to choose the local connection. Worker configuration stays with the core.</p></div></section></div>
      {/if}

      <footer class="page-footer"><span><Icon name="leaf" size={14}/> Your hardware. Your mesh.</span><span>{snapshot ? `Last checked ${new Date(snapshot.checkedAt).toLocaleTimeString()}` : 'Waiting for first observation'}</span></footer>
    </main>
  </div>
</div>

<dialog bind:this={detailDialog} onclose={() => selectedId = null} aria-labelledby="device-title"><div class="dialog-heading"><span class="eyebrow">DEVICE DETAILS</span><button aria-label="Close device details" onclick={() => detailDialog.close()}><Icon name="close"/></button></div>{#if selectedDevice}<div class="dialog-device-icon"><Icon name={iconFor(selectedDevice)} size={36}/></div><h2 id="device-title">{selectedDevice.name}</h2><p>{selectedDevice.hardware}</p><dl class="detail-list"><div><dt>Layer range</dt><dd>{rangeLabel(selectedDevice)}</dd></div><div><dt>Observation</dt><dd>{statusLabel(selectedDevice)}</dd></div><div><dt>Last native compute</dt><dd>{selectedDevice.computeMs?.toFixed(1) ?? '—'} ms</dd></div><div><dt>Observed buffers</dt><dd>{sizeLabel(selectedDevice.bufferBytes)}</dd></div><div><dt>Stage executions</dt><dd>{selectedDevice.executions}</dd></div></dl><div class="soft-note"><Icon name="info" size={18}/><p>These are runtime measurements, not a full memory budget or a device benchmark. Trust status is not reported yet.</p></div>{:else}<h2 id="device-title">Device no longer reported</h2><p>Refresh the mesh to see the current route.</p>{/if}<button class="button secondary full-width" onclick={() => detailDialog.close()}>Done</button></dialog>

<dialog bind:this={pairDialog} aria-labelledby="pair-title"><div class="dialog-heading"><span class="eyebrow">BRING ANOTHER DEVICE</span><button aria-label="Close pairing" onclick={() => pairDialog.close()}><Icon name="close"/></button></div><h2 id="pair-title">Better with company.</h2><p>Enter an emoji code, then let a trusted device welcome you in.</p><div class="segmented"><button class:chosen={pairingTab === 'invite'} onclick={() => pairingTab = 'invite'}>Add to this mesh</button><button class:chosen={pairingTab === 'join'} onclick={() => pairingTab = 'join'}>Join a mesh</button></div>{#if pairingTab === 'invite'}<div class="emoji-preview" aria-label="Illustration: tree, moon, turtle, lemon"><span>🌲</span><span>🌙</span><span>🐢</span><span>🍋</span></div><p class="preview-caption">Illustration only · not an active pairing code</p><ol class="pair-steps"><li><span>1</span>Open Layercake on the new device.</li><li><span>2</span>Enter the emoji code from this mesh.</li><li><span>3</span>Approve it from any trusted device.</li></ol>{:else}<label class="field-label" for="emoji-code">Emoji code</label><input id="emoji-code" class="emoji-input" bind:value={emojiCode} maxlength="64" placeholder="Paste your emoji code" autocomplete="off" spellcheck="false"/><p class="fine-print">Membership begins only after a trusted device approves. A device can join one mesh at a time.</p>{/if}<div class="soft-note"><Icon name="info" size={18}/><p>Pairing is being connected to the core. Invites and trust approvals are not available in this build.</p></div><button class="button primary full-width" disabled>{pairingTab === 'invite' ? 'Create emoji invite' : 'Request to join'}</button></dialog>
