import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { Badge, Button, Card } from "../ui";

export default function Integrations() {
  const { data: instances } = useQuery({ queryKey: ["arr-instances"], queryFn: api.arrInstances });
  const [tested, setTested] = useState<Record<string, string>>({});

  const test = useMutation({
    mutationFn: (name: string) => api.testArrInstance(name),
    onSuccess: (r, name) =>
      setTested((t) => ({ ...t, [name]: r.ok ? `ok ${r.version ?? ""}` : "failed" })),
    onError: (_e, name) => setTested((t) => ({ ...t, [name]: "failed" })),
  });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Integrations</h1>
      <p className="text-sm text-slate-400">
        Sonarr / Radarr instances are defined in the YAML config (infrastructure-as-code) and are
        read-only here. Jobs reference an instance by name.
      </p>

      {(instances ?? []).map((inst) => (
        <Card key={inst.name}>
          <div className="mb-3 flex items-center justify-between">
            <span className="flex items-center gap-3">
              <Badge value={inst.type} />
              <span className="font-medium text-slate-200">{inst.name}</span>
              <span className="font-mono text-xs text-slate-500">{inst.url}</span>
              {tested[inst.name] && <span className="text-xs text-slate-400">{tested[inst.name]}</span>}
            </span>
            <Button onClick={() => test.mutate(inst.name)}>Test</Button>
          </div>
          <div className="text-xs text-slate-400">
            <div className="mb-1 font-medium text-slate-500">Path mappings</div>
            {inst.mappings.length === 0 && <div className="text-slate-600">none</div>}
            {inst.mappings.map((m, i) => (
              <div key={i} className="font-mono">
                {m.from} <span className="text-slate-600">→</span> {m.to}
              </div>
            ))}
          </div>
        </Card>
      ))}

      {instances && instances.length === 0 && (
        <Card>
          <div className="text-sm text-slate-500">
            No arr instances configured. Add a <code className="text-slate-400">sonarr:</code> or{" "}
            <code className="text-slate-400">radarr:</code> stanza to your YAML config.
          </div>
        </Card>
      )}
    </div>
  );
}
