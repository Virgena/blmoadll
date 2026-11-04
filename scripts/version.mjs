// One version, one file: [workspace.package] in Cargo.toml. Every npm manifest is derived from it.
//   node scripts/version.mjs check [--tag v0.2.0]
//   node scripts/version.mjs set 0.2.0
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cargoFile = join(root, "Cargo.toml");
const rootPackageFile = join(root, "package.json");
const platformRoot = join(root, "platforms");
const PLATFORM_DEP = /^@virgena\/blmoadll-/;
const SEMVER = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

const rel = (file) => relative(root, file).split("\\").join("/");
const read = (file) => readFileSync(file, "utf8");
const readJson = (file) => JSON.parse(read(file));
const writeJson = (file, value) => writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
const eol = (text) => (text.includes("\r\n") ? "\r\n" : "\n");

function usage() {
  console.error("usage: node scripts/version.mjs check [--tag v0.2.0]");
  console.error("       node scripts/version.mjs set 0.2.0");
  process.exit(2);
}

function cargoVersion(text) {
  let section = "";
  for (const line of text.split(/\r?\n/)) {
    const header = /^\s*\[([^\]]+)\]\s*$/.exec(line);
    if (header) {
      section = header[1].trim();
      continue;
    }
    if (section !== "workspace.package") continue;
    const entry = /^\s*version\s*=\s*"([^"]*)"/.exec(line);
    if (entry) return entry[1];
  }
  throw new Error("Cargo.toml has no version under [workspace.package]");
}

function setCargoVersion(text, version) {
  const lines = text.split(/\r?\n/);
  let section = "";
  let replaced = false;
  for (let index = 0; index < lines.length; index += 1) {
    const header = /^\s*\[([^\]]+)\]\s*$/.exec(lines[index]);
    if (header) {
      section = header[1].trim();
      continue;
    }
    if (section !== "workspace.package" || replaced) continue;
    if (/^\s*version\s*=/.test(lines[index])) {
      lines[index] = lines[index].replace(/^(\s*version\s*=\s*")[^"]*(".*)$/, `$1${version}$2`);
      replaced = true;
    }
  }
  if (!replaced) throw new Error("Cargo.toml has no version under [workspace.package]");
  return lines.join(eol(text));
}

/** Every npm manifest that must carry the same version, root first. */
function manifests() {
  const platforms = existsSync(platformRoot)
    ? readdirSync(platformRoot, { withFileTypes: true })
        .filter((entry) => entry.isDirectory())
        .map((entry) => join(platformRoot, entry.name, "package.json"))
        .filter((file) => existsSync(file))
    : [];
  return [rootPackageFile, ...platforms];
}

/** Workspace members carry no source in Cargo.lock; they must all be the workspace version. */
function lockedMembers(text) {
  const members = [];
  for (const block of text.split(/^\[\[package\]\]\s*$/m).slice(1)) {
    if (/^\s*source\s*=/m.test(block)) continue;
    const name = /^\s*name\s*=\s*"([^"]+)"/m.exec(block)?.[1];
    const version = /^\s*version\s*=\s*"([^"]+)"/m.exec(block)?.[1];
    if (name && version) members.push({ name, version });
  }
  return members;
}

function check(tag) {
  const expected = cargoVersion(read(cargoFile));
  const problems = [];
  const files = manifests();

  for (const file of files) {
    const actual = readJson(file).version;
    if (actual !== expected) {
      problems.push(`${rel(file)}: version "${actual}", expected "${expected}" from Cargo.toml`);
    }
  }

  const optional = readJson(rootPackageFile).optionalDependencies ?? {};
  for (const [name, range] of Object.entries(optional)) {
    if (PLATFORM_DEP.test(name) && range !== expected) {
      problems.push(`${rel(rootPackageFile)}: optionalDependencies["${name}"] is "${range}", expected "${expected}"`);
    }
  }

  const lockFile = join(root, "Cargo.lock");
  if (existsSync(lockFile)) {
    for (const member of lockedMembers(read(lockFile))) {
      if (member.version !== expected) {
        problems.push(`${rel(lockFile)}: ${member.name} is ${member.version}, expected ${expected} (needs \`cargo metadata\`)`);
      }
    }
  }

  if (tag !== null && tag !== `v${expected}`) {
    problems.push(`git tag: "${tag}", expected "v${expected}"`);
  }

  if (problems.length > 0) {
    for (const problem of problems) console.error(`version: ${problem}`);
    console.error("version: fix with `npm run version:set -- <version>`; Cargo.toml is the only file to edit by hand.");
    process.exit(1);
  }
  console.log(`version: ${expected} is consistent in ${files.length} npm manifests${tag === null ? "" : ` and tag ${tag}`}`);
}

function set(version) {
  if (!SEMVER.test(version)) {
    console.error(`version: "${version}" is not a semver version (0.2.0 or 0.2.0-dev.1).`);
    process.exit(2);
  }

  writeFileSync(cargoFile, setCargoVersion(read(cargoFile), version));

  const rootPackage = readJson(rootPackageFile);
  rootPackage.version = version;
  for (const name of Object.keys(rootPackage.optionalDependencies ?? {})) {
    if (PLATFORM_DEP.test(name)) rootPackage.optionalDependencies[name] = version;
  }
  writeJson(rootPackageFile, rootPackage);

  const files = manifests().slice(1);
  for (const file of files) {
    const manifest = readJson(file);
    manifest.version = version;
    writeJson(file, manifest);
  }

  // Rewrite Cargo.lock for the new workspace version without refreshing other pins.
  const metadata = (extra) =>
    spawnSync("cargo", ["metadata", "--format-version", "1", ...extra], {
      cwd: root,
      stdio: ["ignore", "ignore", "inherit"],
      windowsHide: true,
    });
  let cargo = metadata([]);
  if (cargo.status !== 0) cargo = metadata(["--offline"]);
  if (cargo.error || cargo.status !== 0) {
    console.error("version: Cargo.lock was not refreshed (cargo metadata failed); run it by hand before committing.");
  }

  console.log(`version: ${version} written to ${[cargoFile, ...manifests()].map(rel).join(", ")}`);
  console.log(`version: next: git commit -am "v${version}" && git tag v${version} && git push --follow-tags`);
}

const [command, ...rest] = process.argv.slice(2);
if (command === "check") {
  const index = rest.indexOf("--tag");
  if (index >= 0 && !rest[index + 1]) usage();
  check(index >= 0 ? rest[index + 1] : null);
} else if (command === "set" && rest[0]) {
  set(rest[0]);
} else {
  usage();
}
