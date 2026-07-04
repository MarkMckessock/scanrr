import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { Badge, Button, Card } from "../ui";

function buildYaml(name: string, rootPath: string, ttl: number, cron: string): string {
  const lines = [
    "jobs:",
    `  - name: ${name || "My Job"}`,
    "    type: path",
    `    root_path: ${rootPath || "/mnt/media"}`,
    `    ttl_days: ${ttl}`,
  ];
  if (cron) lines.push(`    schedule_cron: "${cron}"`);
  return lines.join("\n") + "\n";
}

export default function Jobs() {
  const qc = useQueryClient();
  const { data: jobs } = useQuery({ queryKey: ["jobs"], queryFn: api.jobs });

  const [name, setName] = useState("");
  const [rootPath, setRootPath] = useState("");
  const [ttl, setTtl] = useState(30);
  const [cron, setCron] = useState("");
  const [yamlPreview, setYamlPreview] = useState<string | null>(null);

  const run = useMutation({
    mutationFn: (slug: string) => api.runJob(slug),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["jobs"] });
      qc.invalidateQueries({ queryKey: ["runs"] });
    },
  });

  const input = "rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-sm";

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Jobs</h1>
      <p className="text-sm text-slate-400">
        Jobs are defined in the YAML config (infrastructure-as-code) and are read-only here. Use the
        generator below to author a stanza for your ConfigMap.
      </p>

      <Card>
        <div className="mb-3 font-medium">Generate job YAML</div>
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
          <Button
            variant="primary"
            disabled={!name || !rootPath}
            onClick={() => setYamlPreview(buildYaml(name, rootPath, ttl, cron))}
          >
            Generate YAML
          </Button>
        </div>
      </Card>

      <Card className="p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-800 text-left text-slate-400">
            <tr>
              <th className="px-4 py-2 font-medium">Name</th>
              <th className="px-4 py-2 font-medium">Source</th>
              <th className="px-4 py-2 font-medium">Schedule</th>
              <th className="px-4 py-2 font-medium">Last run</th>
              <th className="px-4 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {(jobs ?? []).map((j) => (
              <tr key={j.slug} className="border-b border-slate-800/60">
                <td className="px-4 py-2">
                  <span className="flex items-center gap-2">
                    {!j.enabled && <span className="text-slate-600">⏸</span>}
                    {j.name}
                  </span>
                  <span className="font-mono text-xs text-slate-500">
                    {j.root_path ?? (j.arr_instance ? `arr: ${j.arr_instance}` : "")}
                  </span>
                </td>
                <td className="px-4 py-2">
                  <Badge value="yaml" />
                </td>
                <td className="px-4 py-2 text-slate-400">{j.schedule_cron ?? "manual"}</td>
                <td className="px-4 py-2">{j.last_run ? <Badge value={j.last_run.status} /> : "—"}</td>
                <td className="px-4 py-2">
                  <Button variant="primary" onClick={() => run.mutate(j.slug)}>
                    Run
                  </Button>
                </td>
              </tr>
            ))}
            {jobs && jobs.length === 0 && (
              <tr>
                <td className="px-4 py-6 text-slate-500" colSpan={5}>
                  No jobs — define them in the YAML config and restart.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>

      {yamlPreview !== null && (
        <div
          className="fixed inset-0 z-10 flex items-center justify-center bg-black/60 p-4"
          onClick={() => setYamlPreview(null)}
        >
          <Card className="w-full max-w-2xl">
            <div onClick={(e) => e.stopPropagation()}>
              <div className="mb-3 flex items-center justify-between">
                <span className="font-medium">Job config (YAML)</span>
                <span className="flex gap-2">
                  <Button onClick={() => navigator.clipboard.writeText(yamlPreview)}>Copy</Button>
                  <Button onClick={() => setYamlPreview(null)}>Close</Button>
                </span>
              </div>
              <pre className="overflow-auto rounded-lg bg-slate-950 p-4 text-xs text-slate-300">
                {yamlPreview}
              </pre>
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}
