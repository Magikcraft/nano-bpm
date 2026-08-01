import { useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";

/**
 * Integrated terminal (issue #496): an xterm.js front end wired to the server's
 * PTY WebSocket at `/console/api/projects/{name}/pty`.
 *
 * Wire protocol (mirrors `server/src/console/pty.rs`):
 *   - Binary frames both ways carry raw terminal bytes.
 *   - Text frames client→server are JSON control messages; the only one is
 *     `{"type":"resize","cols":<n>,"rows":<n>}`.
 *
 * The terminal is only mounted while its tab is `active`, so an unopened tab
 * neither holds a shell nor a socket. `generation` bumps on the New-shell
 * button to tear down and respawn.
 */
export default function TerminalPane({
  name,
  active,
}: {
  name: string;
  active: boolean;
}) {
  const hostRef = useRef<HTMLDivElement>(null);
  const [generation, setGeneration] = useState(0);
  const [status, setStatus] = useState<"connecting" | "open" | "closed">(
    "connecting",
  );

  useEffect(() => {
    if (!active) return;
    const host = hostRef.current;
    if (!host) return;

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
      setStatus("open");
      sendResize();
    };
    ws.onclose = () => setStatus("closed");
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
      ws.close();
      term.dispose();
    };
  }, [name, active, generation]);

  return (
    <div className="relative min-h-0 flex-1">
      <div ref={hostRef} className="absolute inset-0 px-2 py-1" />
      {status !== "open" && (
        <div className="pointer-events-none absolute right-2 top-1 flex items-center gap-2 text-xs text-fg-faint">
          {status === "connecting" ? "connecting…" : "shell exited"}
          {status === "closed" && (
            <button
              className="pointer-events-auto rounded px-1.5 py-0.5 uppercase tracking-wider hover:bg-hover hover:text-fg-muted"
              onClick={() => {
                setStatus("connecting");
                setGeneration((g) => g + 1);
              }}
            >
              Restart
            </button>
          )}
        </div>
      )}
    </div>
  );
}
