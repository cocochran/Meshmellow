<script lang="ts">
  import { computeLabel, type ComputeStatus } from './enrollment';
  let { compute, online = true, detail = false }: { compute: ComputeStatus | null | undefined; online?: boolean; detail?: boolean } = $props();
  const state = $derived(computeLabel(compute, online));
</script>
<span class="model-status {state.tone}" title={compute?.message}><i aria-hidden="true"></i>{state.label}</span>
{#if detail && online && compute?.message}<small class="model-detail">{compute.message}</small>{/if}
<style>
.model-status{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#715b4d;white-space:nowrap}.model-status i{width:7px;height:7px;border-radius:50%;background:#b3a99d;flex-shrink:0}.model-status.ready{color:#326345}.model-status.ready i{background:#43895c}.model-status.working{color:#8a5a1c}.model-status.working i{background:#d18a2c;animation:pulse 1.2s ease-in-out infinite}.model-status.failed{color:#a1372b}.model-status.failed i{background:#c4473a}.model-status.idle i{background:#8cc9e1}.model-detail{display:block;font-size:11px;color:#78675d;line-height:1.5;max-width:42ch}
@keyframes pulse{50%{opacity:.35}}@media (prefers-reduced-motion:reduce){.model-status.working i{animation:none}}
</style>
