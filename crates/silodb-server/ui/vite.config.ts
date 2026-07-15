import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Served by silodb-server at /admin (assets embedded via rust-embed).
export default defineConfig({
  base: "/admin/",
  plugins: [react(), tailwindcss()],
  server: {
    // `npm run dev` against a locally running silodb-server
    proxy: {
      "/admin/api": "http://localhost:8080",
      "/sql": "http://localhost:8080",
      "/health": "http://localhost:8080",
    },
  },
});
