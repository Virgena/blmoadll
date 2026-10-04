// Put the kernel binaries into this package's bin/ so a source checkout works without a registry.
// Published Windows installs use @virgena/blmoadll-win32-x64 instead.
import { chmodSync, copyFileSync, existsSync, linkSync, mkdirSync, rmSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = here;
const stageIndex = process.argv.indexOf("--stage");
const stage = stageIndex >= 0;
if (stage && !process.argv[stageIndex + 1]) {
  console.error("@virgena/blmoadll: --stage needs an output directory.");
  process.exit(2);
}
const stageDir = stage ? resolve(root, process.argv[stageIndex + 1]) : null;
const binDir = stageDir ?? join(root, "bin");
const exe = process.platform === "win32" ? ".exe" : "";
const profile = process.env.BLMOADLL_PROFILE === "debug" ? "debug" : "release";
const require = createRequire(import.meta.url);
const BINS = [
  { name: "blmoadll", required: true },
  { name: "blmoadll-fixture", required: true },
];

const has = (dir, name) => existsSync(join(dir, name + exe));

function fromInstalled() {
  return BINS.every((bin) => has(binDir, bin.name)) ? binDir : null;
}

function fromPlatform() {
  if (stage || process.platform !== "win32" || process.arch !== "x64") return null;
  try {
    const manifest = require.resolve("@virgena/blmoadll-win32-x64/package.json");
    const dir = join(dirname(manifest), "bin");
    return BINS.every((bin) => has(dir, bin.name)) ? dir : null;
  } catch {
    return null;
  }
}

function fromEnv() {
  const hint = process.env.BLMOADLL_BIN;
  if (!hint || !existsSync(hint)) return null;
  const path = resolve(hint);
  const dir = statSync(path).isDirectory() ? path : dirname(path);
  return BINS.every((bin) => has(dir, bin.name)) ? dir : null;
}

function fromTarget() {
  const order = profile === "debug" ? ["debug", "release"] : ["release", "debug"];
  for (const name of order) {
    const dir = join(root, "target", name);
    if (has(dir, "blmoadll")) return dir;
  }
  return null;
}

function fromCargo() {
  if (!existsSync(join(root, "Cargo.toml"))) {
    console.error("@virgena/blmoadll: I couldn't find a pre-built binary, and there is no Cargo workspace alongside this package.");
    console.error("  Either set BLMOADLL_BIN to point to a blmoadll executable, or run from the blmoadll checkout.");
    process.exit(1);
  }
  const args = ["build", "-p", "kernel", "--bins", "--features", "fixture,host"];
  if (profile === "release") args.push("--release");
  console.error(`@virgena/blmoadll: no pre-built binary; running cargo ${args.join(" ")} (the first build takes a few minutes)`);
  const run = spawnSync("cargo", args, { cwd: root, stdio: "inherit", windowsHide: true });
  if (run.error || run.status !== 0) {
    console.error(`@virgena/blmoadll: cargo failed: ${run.error?.message ?? `exit ${run.status}`}`);
    process.exit(1);
  }
  return join(root, "target", profile);
}

/** Hard links first: no second copy on the same disk (a debug kernel is 68 MiB). */
function place(source, name) {
  const target = join(binDir, name + exe);
  rmSync(target, { force: true });
  try {
    linkSync(join(source, name + exe), target);
  } catch {
    copyFileSync(join(source, name + exe), target);
    if (exe === "") chmodSync(target, 0o755);
  }
  return target;
}

/** A bare blmoadll prints usage and exits 2; if it runs at all, the binary is executable. */
function sanity(kernelPath) {
  const run = spawnSync(kernelPath, [], { encoding: "utf8", windowsHide: true });
  if (run.error) {
    console.error(`@virgena/blmoadll: ${kernelPath} Can't get up to speed: ${run.error.message}`);
    process.exit(1);
  }
  if (run.status !== 2 && run.status !== 0) {
    console.error(`@virgena/blmoadll: ${kernelPath} Exit code ${run.status} (Expected 2 = usage)`);
    console.error((run.stderr ?? "").trim().split("\n").slice(-3).join("\n"));
    process.exit(1);
  }
}

if (!stage && fromInstalled() && process.env.BLMOADLL_FORCE !== "1") {
  console.error(`@virgena/blmoadll: ${binDir} Already exists (BLMOADLL_FORCE=1 to reinstall)`);
  process.exit(0);
}

if (!stage && fromPlatform()) {
  process.exit(0);
}

const source = stage ? fromCargo() : fromEnv() ?? fromTarget() ?? fromCargo();
mkdirSync(binDir, { recursive: true });
for (const bin of BINS) {
  if (!has(source, bin.name)) {
    console.error(`@virgena/blmoadll: ${source} There isn't any inside. ${bin.name}${exe}`);
    process.exit(1);
  }
  place(source, bin.name);
}
sanity(join(binDir, "blmoadll" + exe));
console.error(`@virgena/blmoadll: It's installed ${join(binDir, "blmoadll" + exe)} (From ${source})`);
