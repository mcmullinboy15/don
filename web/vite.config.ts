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
    // Bind IPv4 explicitly. Left alone, Vite listens on `[::1]` only, which
    // a Don proxy forwarding to 127.0.0.1 can't reach — and neither can a
    // `ready.tcp` check.
    host: "127.0.0.1",
    // Under `don start`, Don assigns the port and forwards a stable public
    // one to it; standalone `npm run dev` keeps Vite's usual 5173.
    port: process.env.PORT ? Number(process.env.PORT) : 5173,
    // Never silently drift to another port: Don's proxy is pointed at this
    // one, so moving would break the forward rather than just relocating it.
    strictPort: true,
    // `npm run dev` proxies the API to a locally running don, so the UI can
    // be iterated on with hot reload against a real stack.
    proxy: {
      "/api": {
        target: process.env.DON_UI_TARGET ?? "http://127.0.0.1:3666",
        // Present the target's host. Not strictly required — don's origin
        // guard checks the hostname, not the port — but it's what a reverse
        // proxy conventionally does, and it keeps this working if the guard
        // ever tightens.
        changeOrigin: true,
      },
    },
  },
});
