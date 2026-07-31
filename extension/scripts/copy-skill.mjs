import { cp, mkdir, rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const extensionRoot = join(scriptDirectory, "..");
const source = join(extensionRoot, "..", "skill", "spark-perf-optimization");
const destination = join(extensionRoot, "skill", "spark-perf-optimization");

await rm(join(extensionRoot, "skill"), { recursive: true, force: true });
await mkdir(dirname(destination), { recursive: true });
await cp(source, destination, { recursive: true });
console.log(`Bundled skill: ${destination}`);