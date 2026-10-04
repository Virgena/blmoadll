import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const exe = process.platform === "win32" ? ".exe" : "";

function platformBin() {
  if (process.platform !== "win32" || process.arch !== "x64") return null;
  try {
    const manifest = require.resolve("@virgena/blmoadll-win32-x64/package.json");
    return join(dirname(manifest), "bin");
  } catch {
    return null;
  }
}

function installed(name) {
  const local = join(here, "bin", name + exe);
  if (existsSync(local)) return local;
  const platform = platformBin();
  if (platform === null) return null;
  const bundled = join(platform, name + exe);
  return existsSync(bundled) ? bundled : null;
}

/** Absolute path of the kernel (the blmoadll host); null when it is not installed. */
export const kernel = installed("blmoadll");

/** Absolute path of the test double (cargo --features fixture); null when it is not built. */
export const fixture = installed("blmoadll-fixture");

/** The kernel path, or a throw when it is not installed: friendlier than an ENOENT out of spawn. */
export function requireKernel() {
  if (kernel === null) {
    throw new Error(`@virgena/blmoadll: No binaries yet; runs on Node "${join(here, "install.js")}"`);
  }
  return kernel;
}