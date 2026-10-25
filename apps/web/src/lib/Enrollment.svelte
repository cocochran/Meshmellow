<script lang="ts">
  import { onMount } from 'svelte';
  import CakeQR from './CakeQR.svelte';
  import QRScanner from './QRScanner.svelte';
  import type { EnrollmentState } from './enrollment';
  let { onchange }: { onchange: (value: EnrollmentState | null) => void } = $props();
  let meshState = $state<EnrollmentState | null>(null);
  let loaded = $state(false), busy = $state(false), error = $state(''), note = $state('');
  let mode = $state('create'), name = $state('My mesh'), deviceName = $state('My computer'), invitation = $state('');
  let scanning = $state(false);
  let confirmingLeave = $state(false);
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
    <p class="fine-print">One mesh. Qwen3.8-27B. Devices join after a trusted member approves.</p>
  </section>
{:else if meshState?.joining}
  <section class="panel welcome"><h2>Waiting for a little trust.</h2><p>Ask a trusted member of {meshState.joining.name} to approve this device. Keep both apps open.</p><button class="button secondary" disabled={busy} onclick={() => command('cancel_join')}>Cancel request</button></section>
{:else if meshState?.mesh}
  <section class="panel membership-panel"><div class="panel-heading"><div><h2>{meshState.mesh.name}</h2><p>{meshState.members.length} trusted {meshState.members.length === 1 ? 'device' : 'devices'} · Qwen3.8-27B</p></div><button class="button secondary" onclick={showInvitation}>Invite a device</button></div>
    <div class="leave-mesh">
      {#if confirmingLeave}<p>Disconnect this computer and return to Create / Join? Other devices keep their mesh and trust history.</p><button class="button secondary" onclick={() => confirmingLeave = false}>Stay in mesh</button><button class="button primary" disabled={busy} onclick={async () => { await command('leave'); confirmingLeave = false; }}>Leave mesh</button>
      {:else}<button class="button secondary" onclick={() => confirmingLeave = true}>Leave mesh</button>{/if}
    </div>
    <div class="member-list">{#each meshState.members as member}<span><strong>{member.name}</strong><small>{member.self ? 'This computer' : member.online ? 'Connected' : 'Offline'}</small></span>{/each}</div>
    {#each meshState.pending as request}<div class="trust-request"><h3>{request.name} wants a slice.</h3><p>Device {request.applicant.slice(0,12)} · Trusted devices can approve others.</p><button class="button primary" disabled={busy} onclick={() => command('decide', { issuer: request.issuer, request: request.id, approve: true })}>Trust device</button><button class="button secondary" disabled={busy} onclick={() => command('decide', { issuer: request.issuer, request: request.id, approve: false })}>Decline</button></div>{/each}
  </section>
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
.leave-mesh{padding:0 24px 24px}.leave-mesh button{margin-right:10px}.welcome{padding:36px;max-width:680px;margin:20px auto}.welcome>img{image-rendering:pixelated;display:block;margin-bottom:18px}.welcome h2{font-family:Georgia,serif;font-size:38px;margin:12px 0}.welcome p{line-height:1.6}.setup-tabs{display:flex;gap:12px;margin:24px 0;flex-wrap:wrap}form{display:grid;gap:14px}input,textarea{width:100%;box-sizing:border-box}textarea{padding:12px;border:1px solid #b9b2a8;border-radius:8px;font:13px monospace;background:var(--surface,#fff);color:inherit}form>.button{justify-self:start}.membership-panel{margin-bottom:24px}.member-list{display:flex;gap:24px;flex-wrap:wrap;padding:0 24px 24px}.member-list span{display:grid;gap:5px}.member-list small{color:#78675d}.trust-request{padding:20px;border-top:1px solid #ddd}.trust-request button{margin-right:10px}dialog .button{margin:8px 8px 8px 0}details{margin:14px 0}summary{cursor:pointer;margin:8px 0}
</style>
