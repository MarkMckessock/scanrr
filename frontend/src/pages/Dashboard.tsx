import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../api";
import { Badge, Card, StatCard } from "../ui";

export default function Dashboard() {
  const { data: stats } = useQuery({ queryKey: ["stats"], queryFn: api.stats });
  const { data: runs } = useQuery({ queryKey: ["runs"], queryFn: api.runs });

  const ok = stats?.files_ok ?? 0;
  const corrupt = stats?.files_corrupt ?? 0;
  const total = Math.max(ok + corrupt, 1);

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Dashboard</h1>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <StatCard label="Jobs" value={stats?.jobs ?? "—"} />
        <StatCard label="Active runs" value={stats?.active_runs ?? "—"} tone="text-sky-300" />
        <StatCard
          label="Open detections"
          value={stats?.open_detections ?? "—"}
          tone={corrupt ? "text-rose-300" : ""}
        />
        <StatCard label="Files tracked" value={stats?.files_tracked ?? "—"} />
      </div>

      <Card>
        <div className="mb-2 flex items-center justify-between text-sm">
          <span className="font-medium">Library health</span>
          <span className="text-slate-400">
            {ok} clean · {corrupt} corrupt
          </span>
        </div>
        <div className="flex h-3 overflow-hidden rounded-full bg-slate-800">
          <div className="bg-emerald-500" style={{ width: `${(ok / total) * 100}%` }} />
          <div className="bg-rose-500" style={{ width: `${(corrupt / total) * 100}%` }} />
        </div>
      </Card>

      <Card>
        <div className="mb-3 font-medium">Recent runs</div>
        <div className="space-y-1">
          {(runs ?? []).slice(0, 10).map((r) => (
            <Link
              key={r.id}
              to={`/runs/${r.id}`}
              className="flex items-center justify-between rounded-lg px-2 py-2 text-sm hover:bg-slate-800/60"
            >
              <span className="flex items-center gap-3">
                <Badge value={r.status} />
                <span className="text-slate-300">run #{r.id}</span>
                <span className="text-slate-500">{r.trigger}</span>
              </span>
              <span className="text-slate-400">
                {r.files_scanned} scanned · {r.files_corrupt} corrupt · {r.files_skipped} skipped
              </span>
            </Link>
          ))}
          {runs && runs.length === 0 && <div className="text-sm text-slate-500">No runs yet.</div>}
        </div>
      </Card>
    </div>
  );
}
