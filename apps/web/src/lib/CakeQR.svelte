<script lang="ts">
  import QRCode from 'qrcode';
  let { ticket }: { ticket: string } = $props();
  const qr = $derived(QRCode.create(ticket, { errorCorrectionLevel: 'M' }));
  const path = $derived.by(() => {
    let path = '';
    for (let y = 0; y < qr.modules.size; y++) for (let x = 0; x < qr.modules.size; x++) {
      if (qr.modules.get(y, x)) path += `M${x+4} ${y+4}h1v1h-1z`;
    }
    return path;
  });
</script>
<div class="cake-qr">
  <div class="cake-top" aria-hidden="true"><span></span><span></span><img src="/meshmellow-logo.png" alt="" width="48" height="48"/><span></span><span></span></div>
  <svg viewBox={`0 0 ${qr.modules.size + 8} ${qr.modules.size + 8}`} role="img" aria-label="Scan this invitation to join the mesh" shape-rendering="crispEdges"><rect width="100%" height="100%" fill="white"/><path d={path} fill="#35221d"/></svg>
  <div class="cake-layers" aria-hidden="true"></div>
  <p>A slice of your mesh.</p>
</div>
<style>
.cake-qr{width:min(100%,340px);margin:24px auto;text-align:center;filter:drop-shadow(6px 8px 0 #412b2014)}
.cake-top{height:58px;background:#fff;display:flex;justify-content:space-around;align-items:center;border:4px solid #e8d8c4;border-bottom:0}.cake-top span{width:12px;height:12px;background:#b83f49;box-shadow:5px -5px 0 #dc6971}.cake-top img{image-rendering:pixelated}
svg{display:block;width:100%;height:auto;background:white;border-left:4px solid #e8d8c4;border-right:4px solid #e8d8c4;box-sizing:border-box}
.cake-layers{height:46px;border:4px solid #805347;border-top:0;background:linear-gradient(#fff 0 14%,#bb8058 14% 43%,#f6d5b8 43% 55%,#b14a53 55% 68%,#bb8058 68%);clip-path:polygon(0 0,100% 0,100% 85%,96% 85%,96% 100%,4% 100%,4% 85%,0 85%)}
p{font-family:Georgia,serif;font-size:21px;color:var(--text,#35221d);margin:14px 0}
</style>
