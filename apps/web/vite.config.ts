import { sveltekit } from '@sveltejs/kit/vite';
import adapter from '@sveltejs/adapter-static';
import tailwindcss from '@tailwindcss/vite';
import { defineConfig, type ProxyOptions } from 'vite';

export default defineConfig({
  plugins: [tailwindcss(), sveltekit({ adapter: adapter({ fallback: "index.html" }) })],
  server: {
    host: '127.0.0.1', port: 5173, strictPort: true,
    proxy: Object.fromEntries(['/api', '/v1'].map(path => [path, {
      target: 'http://127.0.0.1:3210', changeOrigin: true,
      configure(proxy) {
        proxy.on('proxyReq', (outgoing, incoming) => {
          if (incoming.headers.origin === 'http://127.0.0.1:5173') outgoing.setHeader('Origin', 'http://127.0.0.1:3210');
        });
      }
    } satisfies ProxyOptions]))
  }
});
