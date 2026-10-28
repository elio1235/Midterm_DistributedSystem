import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// "npm run dev": the browser talks to http://localhost:5173 and /api/... is forwarded to the gateway.
// In Docker the same job is done by nginx (see nginx.conf).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.GATEWAY_URL || "http://127.0.0.1:8080",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
  preview: {
    port: 5173,
    proxy: {
      "/api": {
        target: process.env.GATEWAY_URL || "http://127.0.0.1:8080",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
