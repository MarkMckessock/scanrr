export interface Stats {
  jobs: number;
  active_runs: number;
  open_detections: number;
  files_ok: number;
  files_corrupt: number;
  files_tracked: number;
}

export interface RunSummary {
  id: number | null;
  status: string;
  finished_at: string | null;
}

export interface Job {
  id: number;
  name: string;
  type: string;
  enabled: boolean;
  ttl_seconds: number;
  schedule_cron: string | null;
  root_path: string | null;
  auto_replace: boolean;
  last_run: RunSummary | null;
}

export interface Run {
  id: number;
  job_id: number;
  status: string;
  trigger: string;
  files_discovered: number;
  files_scanned: number;
  files_skipped: number;
  files_corrupt: number;
  files_unreadable: number;
  started_at: string | null;
  finished_at: string | null;
}

export interface RunFile {
  path: string;
  disposition: string;
  outcome: string | null;
}

export interface Detection {
  id: number;
  path: string;
  hash: string;
  status: string;
  detected_at: string;
  resolved_at: string | null;
  error_log: string | null;
}

const BASE = "/api";

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.status === 204 ? (undefined as T) : ((await res.json()) as T);
}

const post = <T>(path: string, body?: unknown) =>
  req<T>(path, { method: "POST", body: body ? JSON.stringify(body) : undefined });

export const api = {
  stats: () => req<Stats>("/stats"),
  jobs: () => req<Job[]>("/jobs"),
  createJob: (body: { name: string; root_path: string; ttl_days: number; schedule_cron: string | null }) =>
    post<Job>("/jobs", body),
  updateJob: (id: number, body: Partial<{ enabled: boolean; name: string; schedule_cron: string | null }>) =>
    req<Job>(`/jobs/${id}`, { method: "PUT", body: JSON.stringify(body) }),
  deleteJob: (id: number) => req<{ deleted: number }>(`/jobs/${id}`, { method: "DELETE" }),
  runJob: (id: number) => post<{ run_id: number }>(`/jobs/${id}/run`),
  runs: () => req<Run[]>("/runs"),
  run: (id: number) => req<Run>(`/runs/${id}`),
  runFiles: (id: number) => req<RunFile[]>(`/runs/${id}/files`),
  cancelRun: (id: number) => post<Run>(`/runs/${id}/cancel`),
  detections: (status?: string) =>
    req<Detection[]>(`/detections${status ? `?status=${status}` : ""}`),
  triage: (id: number, action: string) => post<{ id: number; status: string }>(`/detections/${id}/${action}`),
  settings: () => req<Record<string, unknown>>("/settings"),
};
