// Hosts take the kernel path from here:
//   import { kernel, fixture } from "eggshell-kernel";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const exe = process.platform === "win32" ? ".exe" : "";

const installed = (name) => {
  const path = join(here, "bin", name + exe);
  return existsSync(path) ? path : null;
};

/** Absolute path of the kernel (the eggshell host); null when it is not installed. */
export const kernel = installed("eggshell");

/** Absolute path of the test double (cargo --features fixture); null when it is not built. */
export const fixture = installed("eggshell-fixture");

/** The kernel path, or a throw when it is not installed: friendlier than an ENOENT out of spawn. */
export function requireKernel() {
  if (kernel === null) {
    throw new Error(`eggshell-kernel: No binaries yet; runs on Node "${join(here, "install.js")}"`);
  }
  return kernel;
}