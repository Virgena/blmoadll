// Put the kernel binaries into this package's bin/ so a host that ran pnpm install has
// eggshell available. Lookup order:
//   1) already in this package's bin/ (installing twice is a no-op; EGGSHELL_FORCE=1 reinstalls)
//   2) the ready-made binary EGGSHELL_BIN points at (file or directory)
//   3) an already built target/<profile>/ in the repository
//   4) build one with cargo now (needs a Rust toolchain)
//
// EGGSHELL_PROFILE=debug changes the preferred and built profile (release by default).
import { chmodSync, copyFileSync, existsSync, linkSync, mkdirSync, rmSync, statSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
/** One level above npm/ is the cargo workspace root (present when installing from this repository's checkout). */
const root = resolve(here, "..");
const binDir = join(here, "bin");
const exe = process.platform === "win32" ? ".exe" : "";
const profile = process.env.EGGSHELL_PROFILE === "debug" ? "debug" : "release";
const BINS = [
  { name: "eggshell", required: true },
  { name: "eggshell-fixture", required: false },
];

const has = (dir, name) => existsSync(join(dir, name + exe));

function fromInstalled() {
  return has(binDir, "eggshell") ? binDir : null;
}

function fromEnv() {
  const hint = process.env.EGGSHELL_BIN;
  if (!hint || !existsSync(hint)) return null;
  const path = resolve(hint);
  const dir = statSync(path).isDirectory() ? path : dirname(path);
  return has(dir, "eggshell") ? dir : null;
}

function fromTarget() {
  const order = profile === "debug" ? ["debug", "release"] : ["release", "debug"];
  for (const name of order) {
    const dir = join(root, "target", name);
    if (has(dir, "eggshell")) return dir;
  }
  return null;
}

function fromCargo() {
  if (!existsSync(join(root, "Cargo.toml"))) {
    console.error("eggshell-kernel: I couldn't find a pre-built binary, and there is no Cargo workspace alongside this package.");
    console.error("  Either set EGGSHELL_BIN to point to an eggshell executable, or install it from the eggshellmod checkout.");
    process.exit(1);
  }
  const args = ["build", "-p", "eggshell-kernel", "--features", "fixture,host"];
  if (profile === "release") args.push("--release");
  console.error(`eggshell-kernel: no pre-built binary; running cargo ${args.join(" ")} (the first build takes a few minutes)`);
  const run = spawnSync("cargo", args, { cwd: root, stdio: "inherit", windowsHide: true });
  if (run.error || run.status !== 0) {
    console.error(`eggshell-kernel: cargo failed: ${run.error?.message ?? `exit ${run.status}`}`);
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

/** A bare eggshell prints usage and exits 2; if it runs at all, the binary is executable. */
function sanity(kernelPath) {
  const run = spawnSync(kernelPath, [], { encoding: "utf8", windowsHide: true });
  if (run.error) {
    console.error(`eggshell-kernel: ${kernelPath} Can't get up to speed: ${run.error.message}`);
    process.exit(1);
  }
  if (run.status !== 2 && run.status !== 0) {
    console.error(`eggshell-kernel: ${kernelPath} Exit code ${run.status} (Expected 2 = usage)`);
    console.error((run.stderr ?? "").trim().split("\n").slice(-3).join("\n"));
    process.exit(1);
  }
}

if (fromInstalled() && process.env.EGGSHELL_FORCE !== "1") {
  console.log(`eggshell-kernel: ${binDir} Already exists (EGGSHELL_FORCE=1 to reinstall)`);
  process.exit(0);
}

const source = fromEnv() ?? fromTarget() ?? fromCargo();
mkdirSync(binDir, { recursive: true });
for (const bin of BINS) {
  if (!has(source, bin.name)) {
    if (bin.required) {
      console.error(`eggshell-kernel: ${source} There isn't any inside. ${bin.name}${exe}`);
      process.exit(1);
    }
    console.error(`eggshell-kernel: Not found ${bin.name}${exe}. The host can run; the fixture plugin needs to be built manually ("--features fixture").`);
    continue;
  }
  place(source, bin.name);
}
sanity(join(binDir, "eggshell" + exe));
console.log(`eggshell-kernel: It's installed ${join(binDir, "eggshell" + exe)} (From ${source})`);