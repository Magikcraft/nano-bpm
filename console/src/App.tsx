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
