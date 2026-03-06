import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/health": "http://localhost:8000",
      "/ingest": "http://localhost:8000",
      "/buildings": "http://localhost:8000",
      "/devices": "http://localhost:8000",
      "/timeseries": "http://localhost:8000"
    }
  }
});

