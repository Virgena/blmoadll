#!/usr/bin/env node
// .bin/blmoadll-fixture: the test double; arguments are passed through to the fixture in the kernel crate.
import { spawnSync } from "node:child_process";

import { fixture } from "./index.js";

if (fixture === null) {
  console.error("@virgena/blmoadll: Without blmoadll-fixture, build using `cargo build -p kernel --features fixture`.");
  process.exit(1);
}
const run = spawnSync(fixture, process.argv.slice(2), { stdio: "inherit", windowsHide: true });
process.exit(run.status ?? 1);