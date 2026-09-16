#!/usr/bin/env node
// .bin/eggshell-fixture: 替身插件，参数原样转给内核那侧的 fixture。
import { spawnSync } from "node:child_process";

import { fixture } from "./index.js";

if (fixture === null) {
  console.error("eggshell-kernel: Without eggshell-fixture, build using `cargo build -p eggshell-kernel --features fixture`.");
  process.exit(1);
}
const run = spawnSync(fixture, process.argv.slice(2), { stdio: "inherit", windowsHide: true });
process.exit(run.status ?? 1);