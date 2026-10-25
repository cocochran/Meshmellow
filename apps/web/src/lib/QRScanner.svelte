<script lang="ts">
  import { onMount } from 'svelte';
  import { isInvitation } from './enrollment';
  let { onscan, onclose }: { onscan: (text: string) => void; onclose: () => void } = $props();
  let video: HTMLVideoElement;
  let error = $state('');
  let ready = $state(false);
  onMount(() => {
    let active = true;
    let scanner: import('qr-scanner').default | undefined;
    const hide = () => { if (document.hidden) onclose(); };
    document.addEventListener('visibilitychange', hide);
    void (async () => {
      try {
        if (!window.isSecureContext) throw new Error('Camera access needs localhost or HTTPS. You can paste an invitation instead.');
        const { default: QRScanner } = await import('qr-scanner');
        if (!active) return;
        scanner = new QRScanner(video, result => {
          if (!active) return;
          if (!isInvitation(result.data)) { error = 'That is not a Layercake invitation. Try the cake QR.'; return; }
          onscan(result.data.trim()); onclose();
        }, { preferredCamera: 'environment', returnDetailedScanResult: true, highlightScanRegion: true });
        await scanner.start();
        if (!active) { scanner.destroy(); return; }
        ready = true;
      } catch { if (active) error = 'Camera unavailable. Allow camera access in your browser’s site settings, or close this and paste an invitation.'; }
    })();
    return () => { active = false; scanner?.destroy(); document.removeEventListener('visibilitychange', hide); };
  });
</script>
<div class="scanner">
  <h3>Find your slice.</h3><p>Point your camera at the cake QR on a trusted device.</p>
  <video bind:this={video} muted playsinline aria-label="Invitation camera preview"></video>
  {#if !ready && !error}<p role="status">Waiting for camera permission…</p>{/if}
  {#if error}<p role="alert">{error}</p>{/if}
  <button type="button" class="button secondary" onclick={onclose}>Close camera</button>
</div>
<style>.scanner{padding:16px 0}.scanner video{width:100%;max-height:360px;background:#211b19;border-radius:12px;object-fit:cover}.scanner p{margin:12px 0}</style>
