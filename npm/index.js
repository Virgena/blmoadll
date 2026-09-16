// 宿主从这儿拿内核路径:
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

/** 内核（eggshell 宿主）的绝对路径；没装好是 null。 */
export const kernel = installed("eggshell");

/** 替身插件（cargo --features fixture）的绝对路径；没构建就是 null。 */
export const fixture = installed("eggshell-fixture");

/** 拿内核路径，没装好就抛 —— 比在 spawn 里吃一个 ENOENT 好读。 */
export function requireKernel() {
  if (kernel === null) {
    throw new Error(`eggshell-kernel: No binaries yet; runs on Node "${join(here, "install.js")}"`);
  }
  return kernel;
}