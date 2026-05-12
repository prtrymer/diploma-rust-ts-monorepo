import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:3000',
        changeOrigin: true,
        ws: true, // proxy WebSocket upgrades too
        configure: (proxy, _options) => {
          proxy.on('error', (err: any, _req, _res) => {
            if (err.code === 'EPIPE' || err.code === 'ECONNRESET') {
              // Ignore typical dev-server hot-reload WS disconnects
              return;
            }
            console.log('proxy error', err);
          });
        },
      },
    },
  },
  build: {
    outDir: '../static/dist',
    emptyOutDir: true,
    minify: false,
  },
})
