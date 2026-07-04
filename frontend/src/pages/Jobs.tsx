import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { Badge, Button, Card } from "../ui";

export default function Jobs() {
  const qc = useQueryClient();
  const { data: jobs } = useQuery({ queryKey: ["jobs"], queryFn: api.jobs });
  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ["jobs"] });
    qc.invalidateQueries({ queryKey: ["runs"] });
  };

  const [name, setName] = useState("");
  const [rootPath, setRootPath] = useState("");
  const [ttl, setTtl] = useState(30);
  const [cron, setCron] = useState("");

  const create = useMutation({
    mutationFn: () =>
      api.createJob({ name, root_path: rootPath, ttl_days: ttl, schedule_cron: cron || null }),
    onSuccess: () => {
      setName("");
      setRootPath("");
      setCron("");
      invalidate();
    },
  });
  const run = useMutation({ mutationFn: (id: number) => api.runJob(id), onSuccess: invalidate });
  const remove = useMutation({ mutationFn: (id: number) => api.deleteJob(id), onSuccess: invalidate });
  const toggle = useMutation({
    mutationFn: (j: { id: number; enabled: boolean }) => api.updateJob(j.id, { enabled: !j.enabled }),
    onSuccess: invalidate,
  });

  const input = "rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-sm";

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Jobs</h1>

      <Card>
        <div className="mb-3 font-medium">New path job</div>
        <div className="flex flex-wrap items-end gap-3">
          <input className={input} placeholder="name" value={name} onChange={(e) => setName(e.target.value)} />
          <input
            className={`${input} min-w-64 flex-1`}
            placeholder="/mnt/media/movies"
            value={rootPath}
            onChange={(e) => setRootPath(e.target.value)}
          />
          <label className="text-sm text-slate-400">
            TTL days{" "}
            <input
              className={`${input} w-20`}
              type="number"
              value={ttl}
              onChange={(e) => setTtl(Number(e.target.value))}
            />
          </label>
          <input
            className={`${input} w-40`}
            placeholder="cron (optional)"
            value={cron}
            onChange={(e) => setCron(e.target.value)}
          />
          <Button variant="primary" disabled={!name || !rootPath} onClick={() => create.mutate()}>
            Create
          </Button>
        </div>
      </Card>

      <Card className="p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-800 text-left text-slate-400">
            <tr>
              <th className="px-4 py-2 font-medium">Name</th>
              <th className="px-4 py-2 font-medium">Path</th>
              <th className="px-4 py-2 font-medium">Schedule</th>
              <th className="px-4 py-2 font-medium">Last run</th>
              <th className="px-4 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {(jobs ?? []).map((j) => (
              <tr key={j.id} className="border-b border-slate-800/60">
                <td className="px-4 py-2">
                  <span className="flex items-center gap-2">
                    {!j.enabled && <span className="text-slate-600">⏸</span>}
                    {j.name}
                  </span>
                </td>
                <td className="px-4 py-2 font-mono text-xs text-slate-400">{j.root_path}</td>
                <td className="px-4 py-2 text-slate-400">{j.schedule_cron ?? "manual"}</td>
                <td className="px-4 py-2">{j.last_run ? <Badge value={j.last_run.status} /> : "—"}</td>
                <td className="px-4 py-2">
                  <div className="flex gap-2">
                    <Button variant="primary" onClick={() => run.mutate(j.id)}>
                      Run
                    </Button>
                    <Button onClick={() => toggle.mutate({ id: j.id, enabled: j.enabled })}>
                      {j.enabled ? "Disable" : "Enable"}
                    </Button>
                    <Button variant="danger" onClick={() => remove.mutate(j.id)}>
                      Delete
                    </Button>
                  </div>
                </td>
              </tr>
            ))}
            {jobs && jobs.length === 0 && (
              <tr>
                <td className="px-4 py-6 text-slate-500" colSpan={5}>
                  No jobs yet — create one above.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
