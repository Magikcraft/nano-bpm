import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import App from "./App";
import { ThemeProvider } from "./theme/ThemeProvider";
import { installChunkReloadBackstop } from "./lib/lazyWithReload";
// Self-hosted JetBrains Mono (variable) — bundled so the code editor uses it
// offline, without relying on the font being installed on the user's machine.
import "@fontsource-variable/jetbrains-mono";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { refetchOnWindowFocus: false, retry: 1 },
  },
});

// Self-heal the "stale chunk after redeploy" failure globally: a module-preload
// that 404s to the SPA index.html (text/html) triggers a one-shot reload to
// fetch the fresh module graph. See lib/chunkReload.ts / lib/lazyWithReload.ts.
installChunkReloadBackstop();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {/* The gateway serves the SPA under /console, so the router shares that basename. */}
    <BrowserRouter basename="/console">
      <QueryClientProvider client={queryClient}>
        <ThemeProvider>
          <App />
        </ThemeProvider>
      </QueryClientProvider>
    </BrowserRouter>
  </React.StrictMode>,
);
