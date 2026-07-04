import { test, expect } from "@playwright/test";

const shot = (name: string) => `screens/${name}.png`;

test("dashboard renders stats, health bar and recent runs", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Dashboard" })).toBeVisible();
  await expect(page.getByText("Library health")).toBeVisible();
  await expect(page.getByText("Open detections")).toBeVisible();
  await expect(page.getByText(/run #\d+/)).toBeVisible();
  await page.screenshot({ path: shot("dashboard"), fullPage: true });
});

test("jobs page lists the YAML job read-only", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
  await expect(page.getByText("Movies")).toBeVisible();
  await expect(page.getByText("yaml").first()).toBeVisible();
  await expect(page.getByRole("button", { name: "Run" }).first()).toBeVisible();
  await page.screenshot({ path: shot("jobs"), fullPage: true });
});

test("detections page shows the corrupt file with triage actions", async ({ page }) => {
  await page.goto("/detections");
  await expect(page.getByRole("heading", { name: "Corrupt files" })).toBeVisible();
  await expect(page.getByText(/Interstellar/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Ack" }).first()).toBeVisible();
  await page.screenshot({ path: shot("detections"), fullPage: true });
});

test("run detail shows per-file outcomes", async ({ page }) => {
  await page.goto("/runs/1");
  await expect(page.getByRole("heading", { name: /Run #1/ })).toBeVisible();
  await expect(page.getByText("Discovered")).toBeVisible();
  await expect(page.getByText(/Interstellar/)).toBeVisible();
  await page.screenshot({ path: shot("run-detail"), fullPage: true });
});

test("integrations page renders arr instance + path mapping forms", async ({ page }) => {
  await page.goto("/integrations");
  await expect(page.getByRole("heading", { name: "Integrations" })).toBeVisible();
  await expect(page.getByText("Sonarr / Radarr instances")).toBeVisible();
  await expect(page.getByText("Path mappings")).toBeVisible();
  await expect(page.getByPlaceholder("http://sonarr:8989")).toBeVisible();
  await page.screenshot({ path: "screens/integrations.png", fullPage: true });
});

test("settings page renders the config table", async ({ page }) => {
  await page.goto("/settings");
  await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();
  await expect(page.getByText("detector_backend")).toBeVisible();
  await page.screenshot({ path: shot("settings"), fullPage: true });
});

test("sidebar navigation works via client routing", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Corrupt files" }).click();
  await expect(page.getByRole("heading", { name: "Corrupt files" })).toBeVisible();
  await page.getByRole("link", { name: "Jobs" }).click();
  await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
});

test("generate job YAML modal renders a stanza", async ({ page }) => {
  await page.goto("/jobs");
  await page.getByPlaceholder("name").fill("My Movies");
  await page.getByPlaceholder("/mnt/media/movies").fill("/mnt/movies");
  await page.getByRole("button", { name: "Generate YAML" }).click();
  await expect(page.getByText("Job config (YAML)")).toBeVisible();
  await expect(page.getByText(/name: My Movies/)).toBeVisible();
});
