import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { api } from "../api";
import { Badge, Button, Card, StatCard } from "../ui";

export default function RunDetail() {
  const { id } = useParams();
  const runId = Number(id);
  const qc = useQueryClient();
  const { data: run } = useQuery({ queryKey: ["run", runId], queryFn: () => api.run(runId) });
  const { data: files } = useQuery({ queryKey: ["run-files", runId], queryFn: () => api.runFiles(runId) });
  const cancel = useMutation({
    mutationFn: () => api.cancelRun(runId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["run", runId] }),
  });

  const active = run?.status === "running" || run?.status === "cancelling";

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="flex items-center gap-3 text-2xl font-semibold">
          Run #{runId} {run && <Badge value={run.status} />}
        </h1>
        {active && (
          <Button variant="danger" onClick={() => cancel.mutate()}>
            Cancel
          </Button>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
        <StatCard label="Discovered" value={run?.files_discovered ?? "—"} />
        <StatCard label="Scanned" value={run?.files_scanned ?? "—"} />
        <StatCard label="Skipped" value={run?.files_skipped ?? "—"} />
        <StatCard label="Corrupt" value={run?.files_corrupt ?? "—"} tone="text-rose-300" />
        <StatCard label="Unreadable" value={run?.files_unreadable ?? "—"} tone="text-amber-300" />
      </div>

      <Card className="p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-800 text-left text-slate-400">
            <tr>
              <th className="px-4 py-2 font-medium">File</th>
              <th className="px-4 py-2 font-medium">Disposition</th>
              <th className="px-4 py-2 font-medium">Outcome</th>
            </tr>
          </thead>
          <tbody>
            {(files ?? []).map((f) => (
              <tr key={f.path} className="border-b border-slate-800/60">
                <td className="px-4 py-2 font-mono text-xs text-slate-300">{f.path}</td>
                <td className="px-4 py-2 text-slate-400">{f.disposition}</td>
                <td className="px-4 py-2">
                  {f.outcome ? <Badge value={f.outcome} /> : <span className="text-slate-600">…</span>}
                </td>
              </tr>
            ))}
            {files && files.length === 0 && (
              <tr>
                <td className="px-4 py-6 text-slate-500" colSpan={3}>
                  No files in this run.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
