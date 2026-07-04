import { defineConfig, devices } from "@playwright/test";

/**
 * Runs against a scanrr instance serving the built SPA + API.
 * Start one first (from backend/, with the frontend built):
 *   SCANRR_DATABASE_URL=sqlite:///dev.db python -m uvicorn scanrr.api.app:app --port 8000
 * then seed a job/run, or point PLAYWRIGHT_BASE_URL at any running instance.
 */
export default defineConfig({
  testDir: "./e2e",
  outputDir: "./e2e/.output",
  fullyParallel: true,
  reporter: [["list"]],
  use: {
    baseURL: process.env.PLAYWRIGHT_BASE_URL ?? "http://127.0.0.1:8000",
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
