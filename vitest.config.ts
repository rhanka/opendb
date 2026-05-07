import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    include: ["tests/**/*.test.ts"],
    // Task 1 lands before test files exist; later tasks replace this with real suites.
    passWithNoTests: true,
    pool: "forks",
    testTimeout: 30_000
  }
});
