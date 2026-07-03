import { lazy, Suspense, useEffect, useRef } from "react";
import {
  NavLink,
  Navigate,
  Route,
  Routes,
  useLocation,
} from "react-router-dom";
import Topology from "./views/Topology";

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

const navItems = [
  { to: "/projects", label: "Projects" },
  { to: "/extensions", label: "Extensions" },
  { to: "/topology", label: "Topology" },
  { to: "/metrics", label: "Metrics" },
  { to: "/explorer", label: "Explorer" },
  { to: "/traces", label: "Traces" },
  { to: "/workers", label: "Workers" },
];

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

  return (
    <div className="flex h-full bg-zinc-950 text-zinc-100">
      <aside className="flex w-56 shrink-0 flex-col border-r border-zinc-800 bg-zinc-900">
        <div className="px-5 py-4">
          <a href="/" className="block no-underline">
            <div className="text-lg font-semibold tracking-tight text-zinc-100">nano BPM</div>
            <div className="text-xs text-zinc-500">single-node console</div>
            <div className="mt-2 inline-block rounded-full border border-violet-400/40 bg-violet-400/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-violet-300">
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
              <NavLink
                key={item.to}
                to={to}
                className={`rounded-md px-3 py-2 text-sm transition-colors ${
                  active
                    ? "bg-zinc-800 text-white"
                    : "text-zinc-400 hover:bg-zinc-800/50 hover:text-zinc-200"
                }`}
              >
                {item.label}
              </NavLink>
            );
          })}
        </nav>

        <NavLink
          to="/credits"
          className={`mt-auto mx-3 flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors ${
            location.pathname.startsWith("/credits")
              ? "bg-zinc-800 text-white"
              : "text-zinc-400 hover:bg-zinc-800/50 hover:text-zinc-200"
          }`}
          title="Credits"
        >
          <svg
            className="h-4 w-4"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <rect x="2" y="4" width="20" height="16" rx="2" />
            <path d="M7 4v16M17 4v16M2 8h5M2 12h5M2 16h5M17 8h5M17 12h5M17 16h5" />
          </svg>
          Credits
        </NavLink>

        <NavLink
          to="/config"
          className={`mb-3 mx-3 flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors ${
            location.pathname.startsWith("/config")
              ? "bg-zinc-800 text-white"
              : "text-zinc-400 hover:bg-zinc-800/50 hover:text-zinc-200"
          }`}
          title="Configuration"
        >
          <svg
            className="h-4 w-4"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <circle cx="12" cy="12" r="3" />
            <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
          </svg>
          Config
        </NavLink>
      </aside>

      <main className="min-w-0 flex-1 overflow-auto">
        <Suspense
          fallback={
            <div className="flex h-full items-center justify-center text-sm text-zinc-500">
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
