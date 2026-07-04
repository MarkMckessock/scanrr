import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../api";
import { Badge, Card, StatCard } from "../ui";

function fmtDur(s?: number | null): string {
  if (s == null) return "—";
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = Math.floor(s % 60);
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${sec}s`;
  return `${sec}s`;
}

function fmtBytes(n?: number | null): string {
  if (n == null) return "—";
  const gb = n / 1e9;
  return gb >= 1 ? `${gb.toFixed(1)} GB` : `${(n / 1e6).toFixed(0)} MB`;
}

const basename = (p: string) => p.split("/").pop() || p;
const elapsedSince = (iso: string | null) =>
  iso ? (Date.now() - Date.parse(iso)) / 1000 : null;

export default function Dashboard() {
  const { data: stats } = useQuery({ queryKey: ["stats"], queryFn: api.stats });
  const { data: runs } = useQuery({ queryKey: ["runs"], queryFn: api.runs });
  const { data: activity } = useQuery({
    queryKey: ["activity"],
    queryFn: api.activity,
    refetchInterval: 2000,
  });

  const ok = stats?.files_ok ?? 0;
  const corrupt = stats?.files_corrupt ?? 0;
  const total = Math.max(ok + corrupt, 1);
  const activeRuns = activity?.runs ?? [];
  const activeTasks = activity?.tasks ?? [];

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Dashboard</h1>

      {activeRuns.length > 0 && (
        <Card>
          <div className="mb-3 font-medium">Active jobs</div>
          <div className="space-y-4">
            {activeRuns.map((r) => (
              <div key={r.run_id}>
                <div className="mb-1 flex items-center justify-between text-sm">
                  <span className="text-slate-200">{r.job_name}</span>
                  <span className="text-slate-400">
                    {r.files_done}/{r.files_total} · {(r.progress * 100).toFixed(1)}%
                    {r.files_corrupt > 0 && (
                      <span className="text-rose-300"> · {r.files_corrupt} corrupt</span>
                    )}
                  </span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-slate-800">
                  <div
                    className="h-full bg-sky-500 transition-all"
                    style={{ width: `${Math.min(r.progress * 100, 100)}%` }}
                  />
                </div>
                <div className="mt-1 flex justify-between text-xs text-slate-500">
                  <span>elapsed {fmtDur(r.elapsed_seconds)}</span>
                  <span>{r.eta_seconds != null ? `~${fmtDur(r.eta_seconds)} left` : "estimating…"}</span>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {activeTasks.length > 0 && (
        <Card>
          <div className="mb-3 font-medium">
            Currently processing <span className="text-slate-500">({activeTasks.length})</span>
          </div>
          <div className="space-y-3">
            {activeTasks.map((t) => (
              <div key={t.task_id}>
                <div className="mb-1 flex items-center justify-between text-sm">
                  <span className="truncate pr-3 font-mono text-xs text-slate-300" title={t.path}>
                    {basename(t.path)}
                  </span>
                  <span className="shrink-0 text-xs text-slate-400">
                    {fmtBytes(t.size_bytes)} · {t.pct != null ? `${(t.pct * 100).toFixed(0)}%` : "preparing…"}
                  </span>
                </div>
                <div className="h-1.5 overflow-hidden rounded-full bg-slate-800">
                  <div
                    className="h-full bg-emerald-500 transition-all"
                    style={{ width: `${t.pct != null ? Math.min(t.pct * 100, 100) : 3}%` }}
                  />
                </div>
                <div className="mt-1 flex justify-between text-xs text-slate-500">
                  <span>
                    {t.position_s != null && t.duration_s
                      ? `${fmtDur(t.position_s)} / ${fmtDur(t.duration_s)} · ${t.frames ?? 0} frames`
                      : "hashing / opening…"}
                  </span>
                  <span>elapsed {fmtDur(elapsedSince(t.started_at))}</span>
                </div>
              </div>
            ))}
          </div>
        </Card>
      )}

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
