import { Fragment, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { Badge, Button, Card } from "../ui";

export default function Detections() {
  const qc = useQueryClient();
  const [showAll, setShowAll] = useState(false);
  const [expanded, setExpanded] = useState<number | null>(null);
  const { data: detections } = useQuery({
    queryKey: ["detections", showAll ? "all" : "open"],
    queryFn: () => api.detections(showAll ? undefined : "open"),
  });
  const triage = useMutation({
    mutationFn: (v: { id: number; action: string }) => api.triage(v.id, v.action),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["detections"] }),
  });
  const replace = useMutation({
    mutationFn: (id: number) => api.replaceDetection(id),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["replacements"] }),
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Corrupt files</h1>
        <Button onClick={() => setShowAll((v) => !v)}>{showAll ? "Show open only" : "Show all"}</Button>
      </div>

      <Card className="p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-800 text-left text-slate-400">
            <tr>
              <th className="px-4 py-2 font-medium">File</th>
              <th className="px-4 py-2 font-medium">Detected</th>
              <th className="px-4 py-2 font-medium">Status</th>
              <th className="px-4 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {(detections ?? []).map((d) => (
              <Fragment key={d.id}>
                <tr className="border-b border-slate-800/60">
                  <td className="px-4 py-2 font-mono text-xs text-slate-300">
                    <button
                      className="cursor-pointer text-left hover:text-white"
                      onClick={() => setExpanded(expanded === d.id ? null : d.id)}
                    >
                      {d.path}
                    </button>
                  </td>
                  <td className="px-4 py-2 text-slate-400">{d.detected_at}</td>
                  <td className="px-4 py-2">
                    <Badge value={d.status} />
                  </td>
                  <td className="px-4 py-2">
                    <div className="flex gap-2">
                      <Button variant="primary" onClick={() => replace.mutate(d.id)}>
                        Replace
                      </Button>
                      <Button onClick={() => triage.mutate({ id: d.id, action: "acknowledge" })}>Ack</Button>
                      <Button onClick={() => triage.mutate({ id: d.id, action: "resolve" })}>Resolve</Button>
                      <Button onClick={() => triage.mutate({ id: d.id, action: "ignore" })}>Ignore</Button>
                    </div>
                  </td>
                </tr>
                {expanded === d.id && d.error_log && (
                  <tr className="border-b border-slate-800/60 bg-slate-950/50">
                    <td className="px-4 py-2" colSpan={4}>
                      <pre className="max-h-48 overflow-auto whitespace-pre-wrap text-xs text-slate-400">
                        {d.error_log}
                      </pre>
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {detections && detections.length === 0 && (
              <tr>
                <td className="px-4 py-6 text-slate-500" colSpan={4}>
                  Nothing corrupt. 🎉
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
