import { NavLink, Navigate, Route, Routes } from "react-router-dom";
import Topology from "./views/Topology";
import Modeler from "./views/Modeler";
import Explorer from "./views/Explorer";
import Workers from "./views/Workers";
import Metrics from "./views/Metrics";
import Traces from "./views/Traces";

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
          <div className="text-lg font-semibold tracking-tight">nano BPM</div>
          <div className="text-xs text-zinc-500">single-node console</div>
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
      </main>
    </div>
  );
}
