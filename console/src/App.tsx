import { lazy, Suspense, useEffect, useRef, useState, type ReactNode } from "react";
import {
  NavLink,
  Navigate,
  Route,
  Routes,
  useLocation,
} from "react-router-dom";
import Topology from "./views/Topology";
import { useTheme } from "./theme/ThemeProvider";
import { api, projectsApi } from "./lib/api";
import { registerFileTypes } from "./lib/editorLang";

// Route views are code-split so heavy editors (bpmn-js modeler + properties
// panel, monaco) stay out of the initial bundle and load on navigation.
const Projects = lazy(() => import("./views/Projects"));
const ProjectWorkspace = lazy(() => import("./views/ProjectWorkspace"));
const Explorer = lazy(() => import("./views/Explorer"));
const Workers = lazy(() => import("./views/Workers"));
const Metrics = lazy(() => import("./views/Metrics"));
const Traces = lazy(() => import("./views/Traces"));
const Extensions = lazy(() => import("./views/Extensions"));
const Config = lazy(() => import("./views/Config"));
const Credits = lazy(() => import("./views/Credits"));

// 24×24 stroke icons, drawn to lucide-style metrics so the rail reads as one
// family.
function Icon({ d, children }: { d?: string; children?: ReactNode }) {
  return (
    <svg
      className="h-4 w-4 shrink-0"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {d ? <path d={d} /> : children}
    </svg>
  );
}

const icons = {
  projects: (
    <Icon d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z" />
  ),
  extensions: (
    <Icon d="M14 4a2 2 0 1 1 4 0v2h2a2 2 0 0 1 2 2v3h-2a2.5 2.5 0 0 0 0 5h2v3a2 2 0 0 1-2 2h-3v-2a2.5 2.5 0 0 0-5 0v2H9a2 2 0 0 1-2-2v-3H5a2.5 2.5 0 0 1 0-5h2V8a2 2 0 0 1 2-2h5z" />
  ),
  topology: (
    <Icon>
      <circle cx="12" cy="5" r="2.5" />
      <circle cx="5" cy="19" r="2.5" />
      <circle cx="19" cy="19" r="2.5" />
      <path d="M12 7.5 6.3 17M12 7.5l5.7 9.5M7.5 19h9" />
    </Icon>
  ),
  metrics: <Icon d="M3 21h18M7 16v-5M12 16V8M17 16v-8" />,
  explorer: (
    <Icon>
      <circle cx="11" cy="11" r="7" />
      <path d="m21 21-4.3-4.3" />
    </Icon>
  ),
  traces: <Icon d="M22 12h-4l-3 8L9 4l-3 8H2" />,
  workers: (
    <Icon>
      <rect x="4" y="4" width="16" height="16" rx="2" />
      <rect x="9" y="9" width="6" height="6" />
      <path d="M9 1v3M15 1v3M9 20v3M15 20v3M1 9h3M1 15h3M20 9h3M20 15h3" />
    </Icon>
  ),
  docs: (
    <Icon>
      <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
    </Icon>
  ),
  whitepaper: (
    <Icon>
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <path d="M14 2v6h6M16 13H8M16 17H8M10 9H8" />
    </Icon>
  ),
  credits: (
    <Icon>
      <rect x="2" y="4" width="20" height="16" rx="2" />
      <path d="M7 4v16M17 4v16M2 8h5M2 12h5M2 16h5M17 8h5M17 12h5M17 16h5" />
    </Icon>
  ),
  config: (
    <Icon>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </Icon>
  ),
  sun: (
    <Icon>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </Icon>
  ),
  moon: <Icon d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z" />,
  system: (
    <Icon>
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <path d="M8 21h8M12 17v4" />
    </Icon>
  ),
} as const;

const navItems: { to: string; label: string; icon: ReactNode }[] = [
  { to: "/projects", label: "Projects", icon: icons.projects },
  { to: "/extensions", label: "Extensions", icon: icons.extensions },
  { to: "/topology", label: "Topology", icon: icons.topology },
  { to: "/metrics", label: "Metrics", icon: icons.metrics },
  { to: "/explorer", label: "Explorer", icon: icons.explorer },
  { to: "/traces", label: "Traces", icon: icons.traces },
  { to: "/workers", label: "Workers", icon: icons.workers },
];

function railItemClass(active: boolean): string {
  return `relative flex items-center gap-2.5 rounded-md px-3 py-2 text-sm no-underline transition-colors ${
    active
      ? "bg-accent/10 font-medium text-accent-strong"
      : "text-fg-muted hover:bg-hover hover:text-fg"
  }`;
}

/** Accent bar marking the active rail item. */
function ActiveBar({ show }: { show: boolean }) {
  if (!show) return null;
  return (
    <span className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-gradient-to-b from-accent to-accent-2" />
  );
}

/** Sidebar segmented control cycling the appearance: light / dark / system.
 * Theme packs and imports are picked in Config → Appearance. */
function ThemeToggle() {
  const { selection, select } = useTheme();
  const modes = [
    { mode: "light", icon: icons.sun, title: "Light" },
    { mode: "dark", icon: icons.moon, title: "Dark" },
    { mode: "system", icon: icons.system, title: "Follow system" },
  ] as const;
  return (
    <div className="mx-3 mb-3 flex rounded-lg border border-edge bg-inset p-0.5">
      {modes.map((m) => {
        const active = selection.mode === m.mode;
        return (
          <button
            key={m.mode}
            title={
              selection.mode === "theme"
                ? `${m.title} (a theme pack is active — this switches back)`
                : m.title
            }
            onClick={() => select({ mode: m.mode })}
            className={`flex flex-1 items-center justify-center rounded-md py-1.5 transition-colors ${
              active ? "bg-raised text-accent-strong shadow-sm" : "text-fg-faint hover:text-fg"
            }`}
          >
            {m.icon}
          </button>
        );
      })}
    </div>
  );
}

export default function App() {
  const location = useLocation();
  // Remember the last place the user was within the Projects section (the
  // project list or a specific workspace) so the rail's "Projects" item returns
  // them there after a detour through Metrics/Traces/etc. — instead of always
  // dropping back at the root list.
  const projectsRoute = useRef(
    localStorage.getItem("nano.projectsRoute") || "/projects",
  );
  useEffect(() => {
    if (location.pathname.startsWith("/projects")) {
      projectsRoute.current = location.pathname;
      localStorage.setItem("nano.projectsRoute", location.pathname);
    }
  }, [location.pathname]);

  // The running gateway's version, shown in the sidebar chrome so it's visible
  // on every page — useful when bouncing between dev builds and staged releases
  // to confirm which binary is actually serving the console. Reads
  // `/console/api/topology`'s `gateway_version` (the same field surfaced on the
  // Topology page); silently absent if the probe fails.
  const [serverVersion, setServerVersion] = useState<string | null>(null);
  useEffect(() => {
    let cancelled = false;
    api
      .topology()
      .then((t) => {
        if (!cancelled) setServerVersion(t.gateway_version);
      })
      .catch(() => {
        /* leave hidden — sidebar is not the place to surface a probe error */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Register every installed lang pack's `fileTypes[]` with the Monaco
  // ext→language map at boot. Without this, a `.java` file (contributed by
  // the `lang-java` pack) falls through to the "typescript" default and
  // Monaco paints Java source with TS highlighting. Done once at App mount
  // so every code editor mounted afterwards sees the right language.
  useEffect(() => {
    projectsApi
      .extensions()
      .then((ov) => {
        for (const e of ov.extensions) {
          if (e.fileTypes?.length) registerFileTypes(e.fileTypes);
        }
      })
      .catch(() => {
        /* ignore — the static fallback table still covers the common cases */
      });
  }, []);

  return (
    <div className="flex h-full bg-app text-fg">
      <aside className="flex w-56 shrink-0 flex-col border-r border-edge bg-panel">
        <div className="px-5 py-4">
          <a href="/" className="block no-underline">
            <div className="bg-gradient-to-r from-accent to-accent-2 bg-clip-text text-lg font-bold tracking-tight text-transparent">
              nano BPM
            </div>
            <div className="text-xs text-fg-faint">single-node console</div>
            {serverVersion && (
              <div
                className="mt-1 font-mono text-[10px] text-fg-faint"
                title="Version of the running gateway (from /console/api/topology)"
              >
                gateway v{serverVersion}
              </div>
            )}
            <div className="mt-2 inline-block rounded-full border border-accent/40 bg-accent/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-accent-strong">
              Advanced Research Prototype
            </div>
          </a>
        </div>
        <nav className="flex flex-col gap-1 px-3">
          {navItems.map((item) => {
            // The Projects item is special: it links back to wherever the user
            // last was in that section and stays highlighted across all
            // /projects/* routes.
            const isProjects = item.to === "/projects";
            const to = isProjects ? projectsRoute.current : item.to;
            const active = isProjects
              ? location.pathname.startsWith("/projects")
              : location.pathname === item.to;
            return (
              <NavLink key={item.to} to={to} className={railItemClass(active)}>
                <ActiveBar show={active} />
                {item.icon}
                {item.label}
              </NavLink>
            );
          })}
        </nav>

        <a href="/docs" className={`mt-auto mx-3 ${railItemClass(false)}`} title="Documentation">
          {icons.docs}
          Documentation
        </a>

        <a href="/whitepaper" className={`mx-3 ${railItemClass(false)}`} title="Whitepaper">
          {icons.whitepaper}
          Whitepaper
        </a>

        <NavLink
          to="/credits"
          className={`mx-3 ${railItemClass(location.pathname.startsWith("/credits"))}`}
          title="Credits"
        >
          <ActiveBar show={location.pathname.startsWith("/credits")} />
          {icons.credits}
          Credits
        </NavLink>

        <NavLink
          to="/config"
          className={`mx-3 mb-3 ${railItemClass(location.pathname.startsWith("/config"))}`}
          title="Configuration"
        >
          <ActiveBar show={location.pathname.startsWith("/config")} />
          {icons.config}
          Config
        </NavLink>

        <ThemeToggle />
      </aside>

      <main className="min-w-0 flex-1 overflow-auto">
        <Suspense
          fallback={
            <div className="flex h-full items-center justify-center text-sm text-fg-faint">
              Loading…
            </div>
          }
        >
          <Routes>
            <Route path="/" element={<Navigate to="/projects" replace />} />
            <Route path="/projects" element={<Projects />} />
            <Route path="/projects/:name" element={<ProjectWorkspace />} />
            <Route path="/extensions" element={<Extensions />} />
            <Route path="/config" element={<Config />} />
            <Route path="/credits" element={<Credits />} />
            <Route path="/topology" element={<Topology />} />
            <Route path="/metrics" element={<Metrics />} />
            <Route path="/modeler" element={<Navigate to="/projects" replace />} />
            <Route path="/explorer" element={<Explorer />} />
            <Route path="/traces" element={<Traces />} />
            <Route path="/workers" element={<Workers />} />
            <Route path="*" element={<Navigate to="/projects" replace />} />
          </Routes>
        </Suspense>
      </main>
    </div>
  );
}
