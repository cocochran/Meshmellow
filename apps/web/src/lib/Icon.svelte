<script lang="ts">
  let { name, size = 20 }: { name: string; size?: number } = $props();
  const paths: Record<string, string> = {
    grid: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
    devices: 'M3 4h14v11H3z M1 19h14 M18 9h5v12h-5z',
    model: 'm12 3 9 5-9 5-9-5 9-5z M3 12l9 5 9-5 M3 16l9 5 9-5',
    activity: 'M2 12h5l3-8 4 16 3-8h5',
    settings: 'M4 6h16 M4 12h16 M4 18h16 M8 3v6 M16 9v6 M10 15v6',
    arrow: 'M5 12h14 M14 7l5 5-5 5',
    plus: 'M12 5v14 M5 12h14',
    close: 'm6 6 12 12 M6 18 18 6',
    laptop: 'M5 4h14v12H5z M2 20h20l-3-4H5l-3 4z',
    phone: 'M7 2h10v20H7z M10 18h4',
    tablet: 'M4 2h16v20H4z M10 18h4',
    server: 'M4 3h16v8H4z M4 13h16v8H4z M7 7h.01 M7 17h.01 M11 7h6 M11 17h6',
    chevron: 'm9 5 7 7-7 7',
    copy: 'M9 9h12v12H9z M15 9V3H3v12h6',
    check: 'm5 12 4 4L19 6',
    refresh: 'M20 7v5h-5 M4 17v-5h5 M6 6a8 8 0 0 1 13 2 M18 18A8 8 0 0 1 5 16',
    leaf: 'M20 3C6 2 2 8 5 15s16 4 15-12z M5 20 15 9',
    info: 'M12 11v6 M12 7h.01 M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0',
    link: 'm9 15 6-6 M8 17l-1 1a4 4 0 0 1-6-6l4-4a4 4 0 0 1 6 0 M16 7l1-1a4 4 0 0 1 6 6l-4 4a4 4 0 0 1-6 0'
  };
</script>
<svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d={paths[name] || paths.devices} /></svg>
