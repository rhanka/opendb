import { lstatSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";

const root = process.cwd();
const ignored = new Set([
  ".git",
  ".opendb-git",
  ".worktrees",
  "target",
  "node_modules",
  ".superpowers",
  ".playwright-mcp"
]);
const forbiddenExtensions = new Set([".py", ".pyi", ".pyw"]);
const forbiddenFiles: string[] = [];

function walk(dir: string): void {
  for (const entry of readdirSync(dir)) {
    if (ignored.has(entry)) continue;
    const path = join(dir, entry);
    const stat = lstatSync(path);
    if (stat.isSymbolicLink()) continue;
    if (stat.isDirectory()) {
      walk(path);
      continue;
    }
    for (const ext of forbiddenExtensions) {
      if (entry.endsWith(ext)) {
        forbiddenFiles.push(relative(root, path));
      }
    }
  }
}

walk(root);

if (forbiddenFiles.length > 0) {
  console.error("Python files are not allowed in OpenDB:");
  for (const file of forbiddenFiles) console.error(`- ${file}`);
  process.exit(1);
}

console.log("No Python files found.");
