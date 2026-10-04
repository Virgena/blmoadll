import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const npm = process.execPath;
const npmCli = process.env.npm_execpath ?? join(dirname(process.execPath), "node_modules", "npm", "bin", "npm-cli.js");
const temp = mkdtempSync(join(tmpdir(), "blmoadll-pack-"));
const npmrc = join(temp, "npmrc");
writeFileSync(npmrc, "");
const npmEnv = { ...process.env, npm_config_cache: join(temp, "cache"), npm_config_userconfig: npmrc };
delete npmEnv.npm_config_allow_scripts;

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { encoding: "utf8", ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`${command} ${args.join(" ")} exited ${result.status}`);
  }
  return result;
}

function pack(target) {
  const args = target === undefined
    ? ["pack", "--json", "--pack-destination", temp]
    : ["pack", target, "--json", "--pack-destination", temp];
  const result = run(npm, [npmCli, ...args], { cwd: root, env: npmEnv });
  const entries = JSON.parse(result.stdout);
  if (!Array.isArray(entries) || entries.length !== 1 || !entries[0].filename) {
    throw new Error(`unexpected npm pack output for ${target ?? "root"}`);
  }
  return { ...entries[0], tarball: join(temp, entries[0].filename) };
}

try {
  const main = pack();
  const platform = pack("./platforms/win32-x64");
  const mainFiles = new Set(main.files.map((file) => file.path));
  const platformFiles = new Set(platform.files.map((file) => file.path));

  for (const required of ["index.js", "index.d.ts", "bin.js", "bin-fixture.js", "install.js"]) {
    if (!mainFiles.has(required)) throw new Error(`main tarball is missing ${required}`);
  }
  for (const forbidden of ["crates/kernel/src/bin/blmoadll.rs", "Cargo.toml", "platforms/win32-x64/package.json"]) {
    if (mainFiles.has(forbidden)) throw new Error(`main tarball unexpectedly contains ${forbidden}`);
  }
  for (const required of ["bin/blmoadll.exe", "bin/blmoadll-fixture.exe"]) {
    if (!platformFiles.has(required)) throw new Error(`platform tarball is missing ${required}`);
  }

  const consumer = join(temp, "consumer");
  mkdirSync(consumer);
  writeFileSync(join(consumer, "package.json"), '{"private":true,"type":"module","allowScripts":{}}\n');
  run(npm, [npmCli, "install", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", platform.tarball, main.tarball], {
    cwd: consumer,
    env: npmEnv,
  });

  const script = `
    import { spawnSync } from "node:child_process";
    import { kernel, fixture, requireKernel } from "@virgena/blmoadll";
    if (!kernel || !fixture) throw new Error("binary exports are missing");
    const result = spawnSync(requireKernel(), ["--help"], { encoding: "utf8", windowsHide: true });
    if (result.status !== 0) throw new Error(result.stderr || "blmoadll --help failed");
    if (!result.stdout.includes("usage: blmoadll")) throw new Error("unexpected blmoadll usage text");
  `;
  run(process.execPath, ["--input-type=module", "-e", script], { cwd: consumer });

  const mainJson = JSON.parse(readFileSync(join(consumer, "node_modules", "@virgena", "blmoadll", "package.json"), "utf8"));
  if (mainJson.name !== "@virgena/blmoadll") throw new Error("installed package name is wrong");
  const platformJson = JSON.parse(readFileSync(join(consumer, "node_modules", "@virgena", "blmoadll-win32-x64", "package.json"), "utf8"));
  if (platformJson.name !== "@virgena/blmoadll-win32-x64") throw new Error("installed platform package name is wrong");

  console.log("npm pack verification passed");
} finally {
  rmSync(temp, { recursive: true, force: true });
}