import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The bundle is embedded in the don binary and served from its root, so
// assets must be referenced relatively and land in a predictable place.
// `dist/` is committed — see web/README.md for why.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Deterministic file names keep the committed bundle from churning on
    // every rebuild, so `git diff web/dist` stays a signal rather than noise.
    rollupOptions: {
      output: {
        entryFileNames: "assets/app.js",
        chunkFileNames: "assets/[name].js",
        assetFileNames: "assets/app.[ext]",
      },
    },
  },
  server: {
    // `npm run dev` proxies the API to a locally running don, so the UI can
    // be iterated on with hot reload against a real stack.
    proxy: {
      "/api": {
        target: process.env.DON_UI_TARGET ?? "http://127.0.0.1:3666",
        changeOrigin: false,
      },
    },
  },
});
