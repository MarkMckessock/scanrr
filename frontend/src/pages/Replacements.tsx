import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { Badge, Button, Card } from "../ui";

export default function Replacements() {
  const qc = useQueryClient();
  const { data: replacements } = useQuery({ queryKey: ["replacements"], queryFn: api.replacements });
  const invalidate = () => qc.invalidateQueries({ queryKey: ["replacements"] });

  const approve = useMutation({ mutationFn: (id: number) => api.approveReplacement(id), onSuccess: invalidate });
  const reject = useMutation({ mutationFn: (id: number) => api.rejectReplacement(id), onSuccess: invalidate });
  const approveAll = useMutation({ mutationFn: () => api.approveAllReplacements(), onSuccess: invalidate });

  const pending = (replacements ?? []).filter((r) => r.status === "pending_approval");

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold">Replacements</h1>
        {pending.length > 0 && (
          <Button variant="primary" onClick={() => approveAll.mutate()}>
            Approve all ({pending.length})
          </Button>
        )}
      </div>
      <p className="text-sm text-slate-400">
        Corrupt arr-linked files proposed for re-request. Deletions require approval unless a job
        sets <code className="text-slate-300">auto_approve</code>.
      </p>

      <Card className="p-0">
        <table className="w-full text-sm">
          <thead className="border-b border-slate-800 text-left text-slate-400">
            <tr>
              <th className="px-4 py-2 font-medium">#</th>
              <th className="px-4 py-2 font-medium">Instance</th>
              <th className="px-4 py-2 font-medium">Attempt</th>
              <th className="px-4 py-2 font-medium">Status</th>
              <th className="px-4 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody>
            {(replacements ?? []).map((r) => (
              <tr key={r.id} className="border-b border-slate-800/60">
                <td className="px-4 py-2 text-slate-400">{r.id}</td>
                <td className="px-4 py-2 font-mono text-xs text-slate-400">
                  {r.arr_instance ?? "—"} {r.media_type ? `(${r.media_type})` : ""}
                </td>
                <td className="px-4 py-2 text-slate-400">{r.attempt}</td>
                <td className="px-4 py-2">
                  <Badge value={r.status} />
                  {r.approved_by && <span className="ml-2 text-xs text-slate-500">by {r.approved_by}</span>}
                </td>
                <td className="px-4 py-2">
                  {r.status === "pending_approval" ? (
                    <div className="flex gap-2">
                      <Button variant="primary" onClick={() => approve.mutate(r.id)}>
                        Approve
                      </Button>
                      <Button variant="danger" onClick={() => reject.mutate(r.id)}>
                        Reject
                      </Button>
                    </div>
                  ) : (
                    <span className="text-xs text-slate-500">{r.notes ?? "—"}</span>
                  )}
                </td>
              </tr>
            ))}
            {replacements && replacements.length === 0 && (
              <tr>
                <td className="px-4 py-6 text-slate-500" colSpan={5}>
                  No replacements.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
