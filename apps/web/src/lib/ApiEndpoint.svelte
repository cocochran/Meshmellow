<script lang="ts">
  import type { NodeApi } from './enrollment';
  let { api, local = false }: { api?: NodeApi | null; local?: boolean } = $props();
  let copied = $state(false);
  let copyError = $state('');
  async function copy() {
    try { await navigator.clipboard.writeText(api!.base_url); copied = true; copyError = ''; }
    catch { copyError = 'Select the URL to copy it manually.'; }
  }
</script>
<div class="api-endpoint">
  <strong>OpenAI-compatible API</strong>
  {#if api}
    <span class:ready={api.inference_ready}>{api.inference_ready ? 'Ready' : 'Local prompt API unavailable'}</span>
    <code>{api.base_url}</code>
    <small>Base URL · {local ? 'on this device' : 'on that device'} only</small>
    <button type="button" class="button secondary" onclick={copy}>{copied ? 'Copied' : 'Copy URL'}</button>
    <small>{copyError || api.message}</small>
  {:else}
    <span>{local ? 'API not running' : 'API not reported'}</span>
    <small>{local ? 'This device has not started an HTTP inference API.' : 'No current endpoint has been reported by this peer.'}</small>
  {/if}
</div>
<style>
.api-endpoint{display:grid;gap:8px;padding:16px;border:1px solid #d9ccc0;border-radius:12px;background:#fffaf3;max-width:100%;margin-top:10px;color:#4d3326}.api-endpoint strong{font-size:13px}.api-endpoint span,.api-endpoint small{font-size:12px;color:#715b4d}.api-endpoint .ready{color:#326345}.api-endpoint code{font-size:13px;overflow-wrap:anywhere;user-select:all}.api-endpoint button{justify-self:start;padding:6px 12px;font-size:12px}
</style>
