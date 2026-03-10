import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/buildings": "http://localhost:8000",
      "/devices": "http://localhost:8000",
      "/timeseries": "http://localhost:8000",
      "/events": "http://localhost:8000",
      "/health": "http://localhost:8000",
      "/ingest": "http://localhost:8000",
    },
  },
});

