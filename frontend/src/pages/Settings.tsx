import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { Card } from "../ui";

export default function Settings() {
  const { data: settings } = useQuery({ queryKey: ["settings"], queryFn: api.settings });

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Settings</h1>
      <p className="text-sm text-slate-400">
        Current runtime configuration (§13). Editing lands in a later iteration.
      </p>
      <Card className="p-0">
        <table className="w-full text-sm">
          <tbody>
            {Object.entries(settings ?? {}).map(([key, value]) => (
              <tr key={key} className="border-b border-slate-800/60">
                <td className="px-4 py-2 font-medium text-slate-300">{key}</td>
                <td className="px-4 py-2 font-mono text-xs text-slate-400">
                  {Array.isArray(value) ? value.join(", ") : String(value)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
