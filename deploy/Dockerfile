# syntax=docker/dockerfile:1

# --- stage 1: build the frontend SPA -------------------------------------- #
FROM node:20-slim AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# --- stage 2: python + ffmpeg runtime ------------------------------------- #
FROM python:3.12-slim AS runtime

# ffmpeg CLI (subprocess backend) + libs PyAV links against.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the backend. Editable keeps the source layout so app.py can locate
# ../../frontend/dist (FRONTEND_DIST = parents[3] of app.py = /app).
COPY backend /app/backend
RUN pip install --no-cache-dir -e /app/backend

# Built SPA at the path FastAPI serves it from.
COPY --from=frontend /app/frontend/dist /app/frontend/dist

ENV SCANRR_DATABASE_URL=sqlite:////data/scanrr.db \
    PYTHONUNBUFFERED=1

EXPOSE 8000
USER 1000
CMD ["python", "-m", "uvicorn", "scanrr.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
