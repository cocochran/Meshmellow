<script lang="ts">
  import ApiEndpoint from "./ApiEndpoint.svelte";
  import ModelStatus from './ModelStatus.svelte';
  import { onMount } from 'svelte';
  import CakeQR from './CakeQR.svelte';
  import QRScanner from './QRScanner.svelte';
  import type { EnrollmentState } from './enrollment';
  let { onchange, stages = [], showMembers = true }: { onchange: (value: EnrollmentState | null) => void; stages?: import('./types').Device[]; showMembers?: boolean } = $props();
  let meshState = $state<EnrollmentState | null>(null);
  let loaded = $state(false), busy = $state(false), error = $state(''), note = $state('');
  let mode = $state('create'), name = $state('My mesh'), deviceName = $state('My computer'), invitation = $state('');
  let scanning = $state(false);
  let confirmingLeave = $state(false);
  let kicking = $state<string | null>(null);
  let copiedApi = $state<string | null>(null);
  async function copyApi(url: string) {
    try { await navigator.clipboard.writeText(url); copiedApi = url; setTimeout(() => { if (copiedApi === url) copiedApi = null; }, 1600); }
    catch { note = 'Select the API URL to copy it.'; }
  }
  let dialog: HTMLDialogElement;
  let active = false;
  let revision = 0;
  function accept(next: EnrollmentState) { meshState = next; loaded = true; onchange(next); }
  async function refresh() {
    const current = revision;
    try {
      const r = await fetch('/api/enrollment', { cache: 'no-store', signal: AbortSignal.timeout(10000) });
      const next = await r.json(); if (!r.ok) throw new Error(next.error);
      if (active && current === revision) { accept(next); error = ''; }
    } catch (e) { if (active && current === revision) { loaded = true; error = e instanceof Error ? e.message : 'Cannot read mesh meshState.'; } }
  }
  onMount(() => {
    active = true; let timer: ReturnType<typeof setTimeout>;
    const tick = async () => { if (!busy && !document.hidden) await refresh(); if (active) timer = setTimeout(tick, 2000); };
    void tick(); return () => { active = false; clearTimeout(timer); };
  });
  async function command(op: string, fields: Record<string, unknown> = {}) {
    if (busy) return;
    busy = true; revision++; error = ''; note = '';
    try {
      const r = await fetch('/api/enrollment', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ op, ...fields }), signal: AbortSignal.timeout(40000) });
      const next = await r.json(); if (!r.ok) throw new Error(next.error);
      if (active) accept(next);
    } catch (e) { if (active) error = e instanceof Error ? e.message : 'Mesh command failed.'; }
    finally { busy = false; }
  }
  export async function showInvitation() {
    dialog.showModal();
    if (meshState?.mesh && !meshState.invite && !meshState.pending.length) await command('invite');
  }
  async function create() {
    await command('create', { name, device_name: deviceName });
    if (meshState?.mesh) await showInvitation();
  }
  async function copy() {
    try { await navigator.clipboard.writeText(meshState?.invite?.ticket || ''); note = 'Invitation copied.'; }
    catch { note = 'Select the invitation text below to copy it.'; }
  }
</script>
{#if !loaded}
  <section class="panel welcome"><h2>Opening Layercake…</h2><p>Checking saved membership on this computer.</p></section>
{:else if !meshState?.mesh && !meshState?.joining}
  <section class="panel welcome">
    <img src="/layercake-logo.png" alt="Minecraft cake" width="80" height="80"/>
    <span class="eyebrow">A FRESH SLICE</span><h2>Your mesh starts here.</h2>
    <p>Create a home for your devices, or join someone’s mesh with an invitation.</p>
    <div class="setup-tabs"><button class="button" class:primary={mode === 'create'} onclick={() => { mode = 'create'; scanning = false; }}>Create a mesh</button><button class="button" class:primary={mode === 'join'} onclick={() => mode = 'join'}>Join a mesh</button></div>
    <form onsubmit={event => { event.preventDefault(); void (mode === 'create' ? create() : command('join', { invitation, device_name: deviceName })); }}>
      <label for="device-name">This device’s name</label><input id="device-name" bind:value={deviceName} maxlength="48" required/>
      {#if mode === 'create'}
        <label for="mesh-name">Mesh name</label><input id="mesh-name" bind:value={name} maxlength="48" required/>
      {:else}
        <button type="button" class="button secondary" onclick={() => scanning = true}>Scan cake QR</button>
        {#if scanning}<QRScanner onscan={text => { invitation = text; note = 'Invitation scanned. Request to join when you’re ready.'; }} onclose={() => scanning = false}/>{/if}
        <label for="join-invitation">Or paste an invitation</label><textarea id="join-invitation" bind:value={invitation} maxlength="4096" rows="3" required placeholder="layercake://join/…"></textarea>
      {/if}
      <button class="button primary" disabled={busy || !meshState}>{busy ? 'Working…' : mode === 'create' ? 'Create mesh' : 'Request to join'}</button>
    </form>
    <p class="fine-print">Devices join after a trusted member approves. Native workers receive model slices automatically.</p>
  </section>
{:else if meshState?.joining}
  <section class="panel welcome"><h2>Waiting for a little trust.</h2><p>Ask a trusted member of {meshState.joining.name} to approve this device. Keep both apps open.</p><button class="button secondary" disabled={busy} onclick={() => command('cancel_join')}>Cancel request</button></section>
{:else if meshState?.mesh}
  {#each meshState.pending as request}<section class="panel trust-request" aria-label="Join request"><div><h3>{request.name} wants to join {meshState.mesh.name}</h3><p>Device <code>{request.applicant.slice(0, 12)}</code> · Any trusted device can approve it.</p></div><div class="actions"><button class="button secondary" disabled={busy} onclick={() => command('decide', { issuer: request.issuer, request: request.id, approve: false })}>Decline</button><button class="button primary" disabled={busy} onclick={() => command('decide', { issuer: request.issuer, request: request.id, approve: true })}>Trust device</button></div></section>{/each}
  {#if showMembers}
  <section class="panel membership-panel" aria-labelledby="members-title">
    <div class="panel-heading"><div><h2 id="members-title">Devices <span class="count-pill">{meshState.members.length}</span></h2><p>{meshState.mesh.name} · {meshState.members.filter(m => m.self || m.online).length} of {meshState.members.length} connected · each device reports its own model state</p></div>
      {#if confirmingLeave}<div class="actions"><span class="confirm-text">Leave {meshState.mesh.name} on this computer?</span><button class="button secondary" onclick={() => confirmingLeave = false}>Stay</button><button class="button danger" disabled={busy} onclick={async () => { await command('leave'); confirmingLeave = false; }}>Leave mesh</button></div>
      {:else}<button class="text-button" onclick={() => confirmingLeave = true}>Leave mesh</button>{/if}
    </div>
    <div class="member-head" aria-hidden="true"><span>Device</span><span>Connection</span><span>Model</span><span>OpenAI-compatible API</span><span></span></div>
    <ul class="member-rows">
      {#each meshState.members as member (member.id)}
        {@const stage = stages.find(s => s.id === member.id)}
        {@const online = member.self || member.online}
        <li class="member-row">
          <div class="member-name"><strong>{member.name}</strong><small>{member.self ? 'This device' : 'Trusted'} · <code>{member.id.slice(0, 10)}</code></small></div>
          <span class="connection-state" class:offline={!online}><i></i>{online ? 'Connected' : 'Offline'}</span>
          <div class="member-model">{#if member.compute || !stage}<ModelStatus compute={member.compute} {online} detail/>{:else}<span class="mono">Layers {stage.begin}–{(stage.end ?? 1) - 1}</span>{/if}</div>
          <div class="member-api">{#if member.api}<code title={member.api.message}>{member.api.base_url}</code><button class="copy" onclick={() => copyApi(member.api!.base_url)} aria-label={`Copy API URL for ${member.name}`}>{copiedApi === member.api.base_url ? 'Copied' : 'Copy'}</button><small>{member.api.inference_ready ? 'Accepting prompts' : 'Not accepting prompts yet'} · {member.self ? 'this device only' : 'that device only'}</small>{:else}<small>{member.self ? 'API not running' : 'Not reported'}</small>{/if}</div>
          <div class="member-actions">{#if meshState.can_kick && !member.self}
            {#if kicking === member.id}<button class="button secondary" disabled={busy} onclick={() => kicking = null}>Cancel</button><button class="button danger" disabled={busy} onclick={async () => { await command('kick', { member: member.id }); kicking = null; }}>Remove</button>
            {:else}<button class="text-button" disabled={busy} onclick={() => kicking = member.id} aria-label={`Remove ${member.name} from the mesh`}>Remove</button>{/if}
          {/if}</div>
        </li>
      {/each}
    </ul>
  </section>
  {/if}
{/if}
{#if error}<p class="notice" role="alert">{error} <button class="button secondary" onclick={refresh}>Retry</button></p>{/if}
{#if note}<p role="status">{note}</p>{/if}
{#if meshState?.notice}<p role="status">{meshState.notice}</p>{/if}
<dialog bind:this={dialog} aria-labelledby="invite-title">
  <div class="dialog-heading"><span class="eyebrow">SHARE A SLICE</span><button aria-label="Close invitation" onclick={() => dialog.close()}>✕</button></div>
  <h2 id="invite-title">Better with company.</h2><p>On your other device, choose Join a mesh → Scan cake QR.</p>
  {#if meshState?.invite}
    <CakeQR ticket={meshState.invite.ticket}/>
    <p class="fine-print">One device · Expires {new Date(meshState.invite.expires * 1000).toLocaleTimeString()} · Approval required</p>
    <button class="button primary" onclick={copy}>Copy invitation</button>
    <details><summary>Full invitation</summary><textarea readonly rows="3" aria-label="Full invitation" value={meshState.invite.ticket}></textarea></details>
  {/if}
  <button class="button secondary" disabled={busy || !!meshState?.pending.length} onclick={() => command('invite')}>{meshState?.invite ? 'Replace invitation' : 'Create invitation'}</button>
  {#if busy}<p role="status">Preparing your invitation…</p>{/if}
  {#if error}<p role="alert">{error}</p>{/if}{#if note}<p role="status">{note}</p>{/if}
</dialog>
<style>
.welcome{padding:36px;max-width:680px;margin:20px auto}.welcome>img{image-rendering:pixelated;display:block;margin-bottom:18px}.welcome h2{font-family:var(--font-display);font-size:30px;letter-spacing:-.8px;margin:12px 0}.welcome p{line-height:1.6;color:var(--muted)}.setup-tabs{display:flex;gap:12px;margin:24px 0;flex-wrap:wrap}form{display:grid;gap:14px}input,textarea{width:100%;box-sizing:border-box}textarea{padding:12px;border:1px solid #d9d4cd;border-radius:5px;font:13px ui-monospace,monospace;background:var(--surface,#fff);color:inherit}form>.button{justify-self:start}
.actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.confirm-text{font-size:12px;color:var(--muted)}.danger{background:#a14336;color:#fff;border:1px solid #a14336}.danger:not(:disabled):hover{background:#86362b}
.trust-request{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:16px 22px;border-color:#e3cfa4;background:#fdf8ee;flex-wrap:wrap}.trust-request h3{font-size:14px;font-weight:650}.trust-request p{font-size:12px;color:var(--muted);margin-top:3px}
.member-head,.member-row{display:grid;grid-template-columns:minmax(150px,1.3fr) 110px minmax(180px,1.6fr) minmax(220px,1.6fr) 140px;gap:18px;align-items:start;padding:14px 22px}
.member-head{background:#faf9f7;border-block:1px solid var(--line);font-size:11px;color:#746d64;padding-block:9px}
.member-rows{list-style:none;margin:0;padding:0}.member-row{border-bottom:1px solid #eeebe6}.member-row:last-child{border-bottom:0}
.member-name strong{display:block;font-size:13px;font-weight:600}.member-name small,.member-api small{display:block;font-size:11px;color:var(--muted);margin-top:3px}
.member-name code,.trust-request code{font-size:10px}
.connection-state{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#4c6656;padding-top:1px}.connection-state i{width:6px;height:6px;border-radius:50%;background:#4e8061}.connection-state.offline{color:#856528}.connection-state.offline i{background:#9b7b40}
.member-api{display:flex;flex-wrap:wrap;align-items:center;gap:4px 8px;min-width:0}.member-api code{font-size:11px;color:#57514a;overflow-wrap:anywhere}.member-api small{flex-basis:100%;margin:0}
.copy{font-size:11px;color:#68615a;background:#fff;border:1px solid #d9d4cd;border-radius:4px;padding:1px 7px}.copy:hover{background:#f5f3ef}
.member-actions{display:flex;gap:6px;justify-content:flex-end;flex-wrap:wrap}.member-actions .button{min-height:30px;padding:5px 10px}
.mono{font-family:ui-monospace,monospace;font-size:12px}
@media(max-width:980px){.member-head{display:none}.member-row{grid-template-columns:1fr auto;gap:8px 16px;padding:14px 16px}.member-model,.member-api{grid-column:1/-1}.member-actions{grid-column:1/-1;justify-content:flex-start}}
dialog .button{margin:8px 8px 8px 0}details{margin:14px 0}summary{cursor:pointer;margin:8px 0}
</style>

{#if meshState && !meshState.mesh}<ApiEndpoint api={meshState.api} local={true}/>{/if}
