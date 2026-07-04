# Deploying scanrr (kube-saturn)

scanrr runs in the `media` namespace of the Saturn cluster, built from the bjw-s
`app-template` chart, exposed at `https://scanrr.markmckessock.com` behind
Cloudflare Zero Trust (admin-only — it can trigger arr deletions).

## Artifacts

| Where | What |
|---|---|
| `deploy/Dockerfile` | Multi-stage image: Vite SPA build → Python 3.12 + ffmpeg runtime (uvicorn on :8000). |
| `.github/workflows/docker.yml` | Builds + pushes `ghcr.io/markmckessock/scanrr` on push to `main` / tags. |
| `kube-saturn/kubernetes/apps/media/scanrr/` | Flux `ks.yaml` (volsync PVC) + `app/` (HelmRelease, OCIRepository, ExternalSecret). |
| `kube-saturn/terraform/cloudflare/zero_trust.tf` | `scanrr` Access application (admin policy). |

## Config & secrets

The whole `scanrr.yaml` (settings/jobs/arr/pushover) is **rendered by the
`scanrr-config` ExternalSecret** and mounted read-only at `/config/scanrr.yaml`.
Structure is literal in the template (versioned in git); only the secret values are
pulled from 1Password, per field, from the **existing** items:

| local key | 1Password item | field |
|---|---|---|
| `radarr_api_key` | `radarr` | `RADARR_API_KEY` |
| `radarr_4k_api_key` | `radarr-4k` | `RADARR_API_KEY` |
| `sonarr_api_key` | `sonarr` | `SONARR_API_KEY` |
| `sonarr_4k_api_key` | `sonarr-4k` | `SONARR_API_KEY` |
| `pushover_user` | `pushover` | `PUSHOVER_USER_KEY` |
| `pushover_token` | `pushover` | `ALERTMANAGER_PUSHOVER_TOKEN` |

Jobs are **arr-type**, one per instance (radarr, radarr-4k, sonarr, sonarr-4k).
Media (granite + basalt Synology) is mounted **read-only**; `auto_replace` is **off**
on every job for initial testing, so nothing is ever deleted. Path mappings translate
arr paths to the scanrr mounts (`/media → /granite/media`, `/basalt/media` identity).

> The Pushover `api_token` reuses the existing alertmanager application token. Add a
> dedicated field + repoint `pushover_token` if you want scanrr pushes under their
> own app.

## Deploy sequence

1. **Image first.** Push scanrr to `main` (or tag) → the Docker workflow publishes
   `ghcr.io/markmckessock/scanrr:latest`. Ensure the GHCR package is pullable by the
   cluster (public, or covered by the existing pull secret — same as
   `splitflap_webhook`).
2. **Manifests.** Commit the kube-saturn changes and let Flux reconcile
   `kubernetes/apps/media/scanrr` (volsync bootstraps the 2Gi PVC; the ExternalSecret
   renders the config; the pod pulls the image).
3. **Zero Trust.** Reconcile the `terraform/cloudflare/` workspace to create the
   admin-only Access application.
4. **Verify.** Pod healthy (`/api/health`), `https://scanrr.markmckessock.com` prompts
   Cloudflare Access, the four arr jobs list in the UI, and a manual run discovers +
   scans. Only then consider flipping `auto_replace` on a job.
