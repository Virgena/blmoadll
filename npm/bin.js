#!/usr/bin/env node
// pnpm/npm's .bin/eggshell lands here; arguments are passed through to the kernel.
import { spawnSync } from "node:child_process";

import { kernel } from "./index.js";

if (kernel === null) {
  console.error("eggshell-kernel: No binaries available yet; run `node install.js`.");
  process.exit(1);
}
const run = spawnSync(kernel, process.argv.slice(2), { stdio: "inherit", windowsHide: true });
process.exit(run.status ?? 1);