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

test("jobs page lists the seeded job and its actions", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
  await expect(page.getByText("Movies")).toBeVisible();
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

test("creating a job through the form adds it to the table", async ({ page }) => {
  await page.goto("/jobs");
  await page.getByPlaceholder("name").fill("E2E Job");
  await page.getByPlaceholder("/mnt/media/movies").fill("/tmp/e2e-library");
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByText("E2E Job")).toBeVisible();
});
