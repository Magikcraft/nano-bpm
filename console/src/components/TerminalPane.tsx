import { useCallback, useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import {
  getTerminalConfig,
  setTerminalConfig,
  type TerminalConfig,
} from "../lib/api";

/**
 * Integrated terminal (issues #496, #500): an xterm.js front end wired to the
 * server's PTY WebSocket at `/console/api/projects/{name}/pty`.
 *
 * The terminal is a console-managed, persisted setting (server-enforced). This
 * pane first reads `/console/api/config/terminal` and renders the appropriate
 * state — an actionable "turn it on" panel when it is off, a lock notice when
 * `NANO_CONSOLE_TERMINAL` disabled it, or the live shell — rather than always
 * dialing a socket and collapsing every failure into a bare "shell exited".
 *
 * Wire protocol (mirrors `server/src/console/pty.rs`):
 *   - Binary frames both ways carry raw terminal bytes.
 *   - Text frames client→server are JSON control messages; the only one is
 *     `{"type":"resize","cols":<n>,"rows":<n>}`.
 */
export default function TerminalPane({
  name,
  active,
}: {
  name: string;
  active: boolean;
}) {
  const [cfg, setCfg] = useState<TerminalConfig | null>(null);
  const [cfgErr, setCfgErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(() => {
    setCfgErr(null);
    getTerminalConfig()
      .then(setCfg)
      .catch((e: unknown) =>
        setCfgErr(e instanceof Error ? e.message : String(e)),
      );
  }, []);

  useEffect(() => {
    if (active) refresh();
  }, [active, refresh]);

  const enable = useCallback(() => {
    setBusy(true);
    setCfgErr(null);
    setTerminalConfig(true)
      .then(setCfg)
      .catch((e: unknown) =>
        setCfgErr(e instanceof Error ? e.message : String(e)),
      )
      .finally(() => setBusy(false));
  }, []);

  if (!active) return <Frame />;

  if (cfgErr) {
    return (
      <Frame>
        <Notice>
          <p>Couldn't read the terminal setting: {cfgErr}</p>
          <ActionButton onClick={refresh}>Retry</ActionButton>
        </Notice>
      </Frame>
    );
  }

  if (!cfg) {
    return (
      <Frame>
        <Notice>Checking terminal availability…</Notice>
      </Frame>
    );
  }

  if (cfg.locked) {
    return (
      <Frame>
        <Notice>
          The integrated terminal is disabled by{" "}
          <code>NANO_CONSOLE_TERMINAL</code> and can't be enabled from the
          console.
        </Notice>
      </Frame>
    );
  }

  if (!cfg.enabled) {
    return (
      <Frame>
        <Notice>
          <p>The integrated terminal is turned off.</p>
          {cfg.local ? (
            <ActionButton onClick={enable} disabled={busy}>
              {busy ? "Enabling…" : "Enable terminal"}
            </ActionButton>
          ) : (
            <p className="text-fg-faint">
              Enable it from the machine running Nano (it opens a shell there,
              so it's local-only).
            </p>
          )}
        </Notice>
      </Frame>
    );
  }

  if (!cfg.local) {
    return (
      <Frame>
        <Notice>
          The integrated terminal opens a shell on the machine hosting Nano, so
          it isn't available from this remote browser.
        </Notice>
      </Frame>
    );
  }

  // Enabled and local → dial the PTY.
  return <LiveTerminal name={name} onDisabled={refresh} />;
}

/** The active xterm.js ↔ PTY connection. Mounted only when the terminal is on. */
function LiveTerminal({
  name,
  onDisabled,
}: {
  name: string;
  onDisabled: () => void;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [generation, setGeneration] = useState(0);
  const [status, setStatus] = useState<
    "connecting" | "open" | "exited" | "failed"
  >("connecting");

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    setStatus("connecting");
    // Distinguishes a failed connect (closed before ever opening — e.g. the
    // server disabled the terminal) from a real shell exit (closed after open).
    let everOpened = false;

    const term = new Terminal({
      cursorBlink: true,
      fontFamily:
        '"JetBrains Mono Variable", ui-monospace, SFMono-Regular, monospace',
      fontSize: 13,
      theme: { background: "#00000000" },
      allowProposedApi: true,
    });
    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(host);
    fit.fit();
    term.focus();

    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(
      `${proto}//${location.host}/console/api/projects/${encodeURIComponent(
        name,
      )}/pty`,
    );
    ws.binaryType = "arraybuffer";
    const enc = new TextEncoder();

    const sendResize = () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(
          JSON.stringify({ type: "resize", cols: term.cols, rows: term.rows }),
        );
      }
    };

    ws.onopen = () => {
      everOpened = true;
      setStatus("open");
      sendResize();
    };
    ws.onclose = () => {
      setStatus(everOpened ? "exited" : "failed");
      // A connect that never opened usually means the server-side setting
      // changed under us; re-check so the parent can show the right panel.
      if (!everOpened) onDisabled();
    };
    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") term.write(ev.data);
      else term.write(new Uint8Array(ev.data));
    };

    const dataSub = term.onData((d) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(enc.encode(d));
    });
    const resizeSub = term.onResize(() => sendResize());

    const ro = new ResizeObserver(() => {
      try {
        fit.fit();
      } catch {
        // xterm throws if the element is detached mid-teardown; ignore.
      }
    });
    ro.observe(host);

    return () => {
      ro.disconnect();
      dataSub.dispose();
      resizeSub.dispose();
      // Detach handlers before closing so a late open/close event can't call
      // setStatus after unmount (React would warn on the detached component).
      ws.onopen = null;
      ws.onclose = null;
      ws.onmessage = null;
      ws.close();
      term.dispose();
    };
  }, [name, generation, onDisabled]);

  return (
    <Frame>
      <div ref={hostRef} className="absolute inset-0 px-2 py-1" />
      {status !== "open" && (
        <div className="pointer-events-none absolute right-2 top-1 flex items-center gap-2 text-xs text-fg-faint">
          {status === "connecting"
            ? "connecting…"
            : status === "failed"
              ? "couldn't connect"
              : "shell exited"}
          {status !== "connecting" && (
            <button
              className="pointer-events-auto rounded px-1.5 py-0.5 uppercase tracking-wider hover:bg-hover hover:text-fg-muted"
              onClick={() => setGeneration((g) => g + 1)}
            >
              Restart
            </button>
          )}
        </div>
      )}
    </Frame>
  );
}

function Frame({ children }: { children?: React.ReactNode }) {
  return <div className="relative min-h-0 flex-1">{children}</div>;
}

function Notice({ children }: { children: React.ReactNode }) {
  return (
    <div className="absolute inset-0 flex flex-col items-center justify-center gap-2 px-4 text-center text-sm text-fg-muted">
      {children}
    </div>
  );
}

function ActionButton({
  children,
  onClick,
  disabled,
}: {
  children: React.ReactNode;
  onClick: () => void;
  disabled?: boolean;
}) {
  return (
    <button
      className="rounded border border-border px-2 py-1 text-xs uppercase tracking-wider hover:bg-hover hover:text-fg disabled:opacity-50"
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  );
}
