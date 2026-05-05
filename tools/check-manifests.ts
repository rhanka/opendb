import { existsSync } from "node:fs";

const baseDir = "deploy/k8s/base";

if (!existsSync(baseDir)) {
  console.log("Kubernetes manifests are not present yet; manifest validation is deferred to Task 9.");
  process.exit(0);
}

console.log("Kubernetes manifest validation placeholder passed. Task 9 will replace this with static checks.");
