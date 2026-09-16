#!/usr/bin/env node
// pnpm/npm 的 .bin/eggshell 走这里，参数原样转给内核。
import { spawnSync } from "node:child_process";

import { kernel } from "./index.js";

if (kernel === null) {
  console.error("eggshell-kernel: No binaries available yet; run `node install.js`.");
  process.exit(1);
}
const run = spawnSync(kernel, process.argv.slice(2), { stdio: "inherit", windowsHide: true });
process.exit(run.status ?? 1);