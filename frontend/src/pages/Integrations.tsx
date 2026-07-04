import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { Badge, Button, Card } from "../ui";

const input = "rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-sm";

function ArrInstances() {
  const qc = useQueryClient();
  const { data: instances } = useQuery({ queryKey: ["arr-instances"], queryFn: api.arrInstances });
  const invalidate = () => qc.invalidateQueries({ queryKey: ["arr-instances"] });

  const [type, setType] = useState("sonarr");
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [key, setKey] = useState("");
  const [tested, setTested] = useState<Record<number, string>>({});

  const create = useMutation({
    mutationFn: () => api.createArrInstance({ type, name, base_url: url, api_key: key }),
    onSuccess: () => {
      setName("");
      setUrl("");
      setKey("");
      invalidate();
    },
  });
  const remove = useMutation({ mutationFn: (id: number) => api.deleteArrInstance(id), onSuccess: invalidate });
  const test = useMutation({
    mutationFn: (id: number) => api.testArrInstance(id),
    onSuccess: (r, id) => setTested((t) => ({ ...t, [id]: r.ok ? `ok ${r.version ?? ""}` : "failed" })),
    onError: (_e, id) => setTested((t) => ({ ...t, [id]: "failed" })),
  });

  return (
    <Card>
      <div className="mb-3 font-medium">Sonarr / Radarr instances</div>
      <div className="mb-4 flex flex-wrap items-end gap-3">
        <select className={input} value={type} onChange={(e) => setType(e.target.value)}>
          <option value="sonarr">sonarr</option>
          <option value="radarr">radarr</option>
        </select>
        <input className={input} placeholder="name" value={name} onChange={(e) => setName(e.target.value)} />
        <input
          className={`${input} min-w-56 flex-1`}
          placeholder="http://sonarr:8989"
          value={url}
          onChange={(e) => setUrl(e.target.value)}
        />
        <input
          className={input}
          placeholder="api key"
          type="password"
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
        <Button variant="primary" disabled={!name || !url || !key} onClick={() => create.mutate()}>
          Add
        </Button>
      </div>
      <div className="space-y-1">
        {(instances ?? []).map((i) => (
          <div key={i.id} className="flex items-center justify-between rounded-lg px-2 py-2 text-sm hover:bg-slate-800/50">
            <span className="flex items-center gap-3">
              <Badge value={i.type} />
              <span className="text-slate-200">{i.name}</span>
              <span className="font-mono text-xs text-slate-500">{i.base_url}</span>
              {tested[i.id] && <span className="text-xs text-slate-400">{tested[i.id]}</span>}
            </span>
            <span className="flex gap-2">
              <Button onClick={() => test.mutate(i.id)}>Test</Button>
              <Button variant="danger" onClick={() => remove.mutate(i.id)}>
                Delete
              </Button>
            </span>
          </div>
        ))}
        {instances && instances.length === 0 && (
          <div className="text-sm text-slate-500">No instances configured.</div>
        )}
      </div>
    </Card>
  );
}

function PathMappings() {
  const qc = useQueryClient();
  const { data: mappings } = useQuery({ queryKey: ["path-mappings"], queryFn: api.pathMappings });
  const { data: instances } = useQuery({ queryKey: ["arr-instances"], queryFn: api.arrInstances });
  const invalidate = () => qc.invalidateQueries({ queryKey: ["path-mappings"] });

  const [instanceId, setInstanceId] = useState<number | "">("");
  const [remote, setRemote] = useState("");
  const [local, setLocal] = useState("");

  const create = useMutation({
    mutationFn: () =>
      api.createPathMapping({ arr_instance_id: Number(instanceId), remote_path: remote, local_path: local }),
    onSuccess: () => {
      setRemote("");
      setLocal("");
      invalidate();
    },
  });
  const remove = useMutation({ mutationFn: (id: number) => api.deletePathMapping(id), onSuccess: invalidate });

  return (
    <Card>
      <div className="mb-1 font-medium">Path mappings</div>
      <p className="mb-3 text-xs text-slate-400">Translate an arr-namespace path to scanrr&apos;s local mount.</p>
      <div className="mb-4 flex flex-wrap items-end gap-3">
        <select className={input} value={instanceId} onChange={(e) => setInstanceId(Number(e.target.value))}>
          <option value="">instance…</option>
          {(instances ?? []).map((i) => (
            <option key={i.id} value={i.id}>
              {i.name}
            </option>
          ))}
        </select>
        <input className={input} placeholder="/data/media/tv" value={remote} onChange={(e) => setRemote(e.target.value)} />
        <span className="text-slate-500">→</span>
        <input className={input} placeholder="/mnt/tv" value={local} onChange={(e) => setLocal(e.target.value)} />
        <Button variant="primary" disabled={!instanceId || !remote || !local} onClick={() => create.mutate()}>
          Add
        </Button>
      </div>
      <div className="space-y-1">
        {(mappings ?? []).map((m) => (
          <div key={m.id} className="flex items-center justify-between rounded-lg px-2 py-2 text-sm hover:bg-slate-800/50">
            <span className="font-mono text-xs text-slate-300">
              {m.remote_path} <span className="text-slate-600">→</span> {m.local_path}
            </span>
            <Button variant="danger" onClick={() => remove.mutate(m.id)}>
              Delete
            </Button>
          </div>
        ))}
        {mappings && mappings.length === 0 && <div className="text-sm text-slate-500">No mappings.</div>}
      </div>
    </Card>
  );
}

export default function Integrations() {
  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Integrations</h1>
      <ArrInstances />
      <PathMappings />
    </div>
  );
}
