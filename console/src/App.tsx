import { lazy, Suspense } from "react";
import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import Topology from "./views/Topology";

// Route views are code-split so heavy editors (bpmn-js modeler + properties
// panel, monaco) stay out of the initial bundle and load on navigation.
const Modeler = lazy(() => import("./views/Modeler"));
const Explorer = lazy(() => import("./views/Explorer"));
const Workers = lazy(() => import("./views/Workers"));
const Metrics = lazy(() => import("./views/Metrics"));
const Traces = lazy(() => import("./views/Traces"));

const navItems = [
  { to: "/topology", label: "Topology" },
  { to: "/metrics", label: "Metrics" },
  { to: "/modeler", label: "Modeler" },
  { to: "/explorer", label: "Explorer" },
  { to: "/traces", label: "Traces" },
  { to: "/workers", label: "Workers" },
];

export default function App() {
  return (
    <div className="flex h-full bg-zinc-950 text-zinc-100">
      <aside className="flex w-56 shrink-0 flex-col border-r border-zinc-800 bg-zinc-900">
        <div className="px-5 py-4">
          <a href="/" className="block no-underline">
            <div className="text-lg font-semibold tracking-tight text-zinc-100">nano BPM</div>
            <div className="text-xs text-zinc-500">single-node console</div>
          </a>
        </div>
        <nav className="flex flex-col gap-1 px-3">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) =>
                `rounded-md px-3 py-2 text-sm transition-colors ${
                  isActive
                    ? "bg-zinc-800 text-white"
                    : "text-zinc-400 hover:bg-zinc-800/50 hover:text-zinc-200"
                }`
              }
            >
              {item.label}
            </NavLink>
          ))}
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
            <Route path="/" element={<Navigate to="/topology" replace />} />
            <Route path="/topology" element={<Topology />} />
            <Route path="/metrics" element={<Metrics />} />
            <Route path="/modeler" element={<Modeler />} />
            <Route path="/explorer" element={<Explorer />} />
            <Route path="/traces" element={<Traces />} />
            <Route path="/workers" element={<Workers />} />
            <Route path="*" element={<Navigate to="/topology" replace />} />
          </Routes>
        </Suspense>
      </main>
    </div>
  );
}
