import { useCallback, useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import {
  getTerminalConfig,
  setTerminalConfig,
  type TerminalConfig,
} from "../lib/api";
import {
  terminalPaneState,
  terminalTheme,
  type TerminalThemeColors,
} from "../lib/terminalPane";

/**
 * Integrated terminal (issues #496, #500, #504): an xterm.js front end wired to
 * the server's PTY WebSocket at `/console/api/projects/{name}/pty`.
 *
 * The terminal is a console-managed, persisted setting (server-enforced). This
 * pane first reads `/console/api/config/terminal` and renders the appropriate
 * state — an actionable "turn it on" panel when it is off, a lock notice when
 * `NANO_CONSOLE_TERMINAL` disabled it, or the live shell — rather than always
 * dialing a socket and collapsing every failure into a bare "shell exited".
 *
 * The render decision lives in `../lib/terminalPane` so its two #504 invariants
 * are unit-tested: (a) the pane stays mounted across tab switches so the shell
 * session survives, and (b) the xterm foreground is derived from the app theme
 * (legible in light mode, not xterm's default white).
 *
 * Because the parent keeps this component mounted on every bottom-panel tab, the
 * pane must contribute NO layout when its tab isn't selected — otherwise it
 * would split the height with the visible output/debug pane. It renders
 * `display:none` when inactive, which keeps the live terminal mounted (session
 * alive) while taking zero space.
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

  const state = terminalPaneState(active, cfg, cfgErr);

  const body = (() => {
    switch (state.kind) {
      case "idle":
        return <Frame />;
      case "error":
        return (
          <Frame>
            <Notice>
              <p>Couldn't read the terminal setting: {cfgErr}</p>
              <ActionButton onClick={refresh}>Retry</ActionButton>
            </Notice>
          </Frame>
        );
      case "loading":
        return (
          <Frame>
            <Notice>Checking terminal availability…</Notice>
          </Frame>
        );
      case "locked":
        return (
          <Frame>
            <Notice>
              The integrated terminal is disabled by{" "}
              <code>NANO_CONSOLE_TERMINAL</code> and can't be enabled from the
              console.
            </Notice>
          </Frame>
        );
      case "off-local":
        return (
          <Frame>
            <Notice>
              <p>The integrated terminal is turned off.</p>
              <ActionButton onClick={enable} disabled={busy}>
                {busy ? "Enabling…" : "Enable terminal"}
              </ActionButton>
            </Notice>
          </Frame>
        );
      case "off-remote":
        return (
          <Frame>
            <Notice>
              <p>The integrated terminal is turned off.</p>
              <p className="text-fg-faint">
                Enable it from the machine running Nano (it opens a shell there,
                so it's local-only).
              </p>
            </Notice>
          </Frame>
        );
      case "remote":
        return (
          <Frame>
            <Notice>
              The integrated terminal opens a shell on the machine hosting Nano,
              so it isn't available from this remote browser.
            </Notice>
          </Frame>
        );
      case "live":
        return (
          <LiveTerminal
            name={name}
            visible={!state.hidden}
            onDisabled={refresh}
          />
        );
    }
  })();

  return (
    // Zero-footprint when the terminal tab isn't selected, but still mounted so
    // a live shell keeps running. Never unmount on tab switch (#504).
    <div className={active ? "flex min-h-0 flex-1 flex-col" : "hidden"}>
      {body}
    </div>
  );
}

/** The active xterm.js ↔ PTY connection. Mounted only when the terminal is on. */
function LiveTerminal({
  name,
  visible,
  onDisabled,
}: {
  name: string;
  visible: boolean;
  onDisabled: () => void;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const termRef = useRef<Terminal | null>(null);
  const fitRef = useRef<FitAddon | null>(null);
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

    const readVar = (n: string): string =>
      getComputedStyle(document.documentElement).getPropertyValue(n);

    const term = new Terminal({
      cursorBlink: true,
      fontFamily:
        '"JetBrains Mono Variable", ui-monospace, SFMono-Regular, monospace',
      fontSize: 13,
      theme: toXtermTheme(terminalTheme(readVar)),
      allowProposedApi: true,
    });
    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(host);
    fit.fit();
    term.focus();
    termRef.current = term;
    fitRef.current = fit;

    // Keep the foreground legible when the user flips the console appearance
    // (`:root[data-appearance="light|dark"]`) while the terminal is open (#504).
    const themeObserver = new MutationObserver(() => {
      term.options.theme = toXtermTheme(terminalTheme(readVar));
    });
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-appearance"],
    });

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
      themeObserver.disconnect();
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
      termRef.current = null;
      fitRef.current = null;
    };
  }, [name, generation, onDisabled]);

  // While hidden (`display:none` on the parent) xterm can't measure the host,
  // so re-fit and restore focus when the tab becomes visible again (#504).
  useEffect(() => {
    if (!visible) return;
    try {
      fitRef.current?.fit();
    } catch {
      // element not laid out yet; the ResizeObserver will fit shortly.
    }
    termRef.current?.focus();
  }, [visible]);

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

/** Adapt our theme tokens to xterm's `ITheme` shape (drops undefined keys). */
function toXtermTheme(c: TerminalThemeColors) {
  return {
    background: c.background,
    foreground: c.foreground,
    cursor: c.cursor,
    ...(c.cursorAccent ? { cursorAccent: c.cursorAccent } : {}),
  };
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
