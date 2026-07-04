import type { ReactNode } from "react";

const STATUS_COLORS: Record<string, string> = {
  ok: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30",
  completed: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30",
  corrupt: "bg-rose-500/15 text-rose-300 ring-rose-500/30",
  open: "bg-rose-500/15 text-rose-300 ring-rose-500/30",
  unreadable: "bg-amber-500/15 text-amber-300 ring-amber-500/30",
  running: "bg-sky-500/15 text-sky-300 ring-sky-500/30",
  queued: "bg-slate-500/15 text-slate-300 ring-slate-500/30",
  cancelled: "bg-slate-500/15 text-slate-400 ring-slate-500/30",
  resolved: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30",
  acknowledged: "bg-slate-500/15 text-slate-300 ring-slate-500/30",
};

export function Badge({ value }: { value: string }) {
  const cls = STATUS_COLORS[value] ?? "bg-slate-500/15 text-slate-300 ring-slate-500/30";
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ${cls}`}>
      {value}
    </span>
  );
}

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div className={`rounded-xl border border-slate-800 bg-slate-900/50 p-4 ${className}`}>
      {children}
    </div>
  );
}

export function StatCard({ label, value, tone = "" }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <Card>
      <div className="text-sm text-slate-400">{label}</div>
      <div className={`mt-1 text-2xl font-semibold ${tone}`}>{value}</div>
    </Card>
  );
}

export function Button({
  children,
  onClick,
  variant = "default",
  disabled,
}: {
  children: ReactNode;
  onClick?: () => void;
  variant?: "default" | "primary" | "danger";
  disabled?: boolean;
}) {
  const styles = {
    default: "border-slate-700 bg-slate-800 hover:bg-slate-700 text-slate-200",
    primary: "border-sky-600 bg-sky-600 hover:bg-sky-500 text-white",
    danger: "border-rose-700 bg-rose-900/40 hover:bg-rose-900/70 text-rose-200",
  }[variant];
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`rounded-lg border px-3 py-1.5 text-sm font-medium transition disabled:opacity-40 ${styles}`}
    >
      {children}
    </button>
  );
}
