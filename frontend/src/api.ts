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
  slug: string;
  name: string;
  type: string;
  enabled: boolean;
  ttl_seconds: number;
  schedule_cron: string | null;
  root_path: string | null;
  arr_instance_id: number | null;
  auto_replace: boolean;
  last_run: RunSummary | null;
}

export interface Run {
  id: number;
  job_slug: string;
  job_name: string;
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

export interface ArrInstance {
  id: number;
  type: string;
  name: string;
  base_url: string;
  enabled: boolean;
}

export interface PathMapping {
  id: number;
  arr_instance_id: number;
  remote_path: string;
  local_path: string;
}

export interface Replacement {
  id: number;
  detection_id: number;
  attempt: number;
  status: string;
  media_type: string | null;
  requested_at: string | null;
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
  runJob: (slug: string) => post<{ run_id: number }>(`/jobs/${slug}/run`),
  runs: () => req<Run[]>("/runs"),
  run: (id: number) => req<Run>(`/runs/${id}`),
  runFiles: (id: number) => req<RunFile[]>(`/runs/${id}/files`),
  cancelRun: (id: number) => post<Run>(`/runs/${id}/cancel`),
  detections: (status?: string) =>
    req<Detection[]>(`/detections${status ? `?status=${status}` : ""}`),
  triage: (id: number, action: string) => post<{ id: number; status: string }>(`/detections/${id}/${action}`),
  replaceDetection: (id: number) => post<Replacement>(`/detections/${id}/replace`),
  settings: () => req<Record<string, unknown>>("/settings"),
  arrInstances: () => req<ArrInstance[]>("/arr-instances"),
  createArrInstance: (body: { type: string; name: string; base_url: string; api_key: string }) =>
    post<ArrInstance>("/arr-instances", body),
  deleteArrInstance: (id: number) => req<{ deleted: number }>(`/arr-instances/${id}`, { method: "DELETE" }),
  testArrInstance: (id: number) => post<{ ok: boolean; version?: string }>(`/arr-instances/${id}/test`),
  pathMappings: () => req<PathMapping[]>("/path-mappings"),
  createPathMapping: (body: { arr_instance_id: number; remote_path: string; local_path: string }) =>
    post<PathMapping>("/path-mappings", body),
  deletePathMapping: (id: number) => req<{ deleted: number }>(`/path-mappings/${id}`, { method: "DELETE" }),
  replacements: () => req<Replacement[]>("/replacements"),
};
