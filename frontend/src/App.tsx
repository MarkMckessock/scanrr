import { NavLink, Route, Routes } from "react-router-dom";
import { useLiveEvents } from "./useEvents";
import Dashboard from "./pages/Dashboard";
import Jobs from "./pages/Jobs";
import RunDetail from "./pages/RunDetail";
import Detections from "./pages/Detections";
import Replacements from "./pages/Replacements";
import Integrations from "./pages/Integrations";
import Settings from "./pages/Settings";

const NAV = [
  { to: "/", label: "Dashboard", end: true },
  { to: "/jobs", label: "Jobs", end: false },
  { to: "/detections", label: "Corrupt files", end: false },
  { to: "/replacements", label: "Replacements", end: false },
  { to: "/integrations", label: "Integrations", end: false },
  { to: "/settings", label: "Settings", end: false },
];

export default function App() {
  useLiveEvents();
  return (
    <div className="flex min-h-screen">
      <aside className="w-56 shrink-0 border-r border-slate-800 p-4">
        <div className="mb-6 flex items-center gap-2 px-2">
          <span className="text-lg font-bold tracking-tight text-sky-400">scanrr</span>
        </div>
        <nav className="space-y-1">
          {NAV.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              end={n.end}
              className={({ isActive }) =>
                `block rounded-lg px-3 py-2 text-sm font-medium transition ${
                  isActive ? "bg-slate-800 text-white" : "text-slate-400 hover:bg-slate-800/50 hover:text-slate-200"
                }`
              }
            >
              {n.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="flex-1 overflow-auto p-8">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/jobs" element={<Jobs />} />
          <Route path="/runs/:id" element={<RunDetail />} />
          <Route path="/detections" element={<Detections />} />
          <Route path="/replacements" element={<Replacements />} />
          <Route path="/integrations" element={<Integrations />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </main>
    </div>
  );
}
