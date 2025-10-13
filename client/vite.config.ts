import { defineConfig } from "vite";

export default defineConfig({
  plugins: [],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: true,
        // optionally remove "/api" before forwarding:
        // rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
