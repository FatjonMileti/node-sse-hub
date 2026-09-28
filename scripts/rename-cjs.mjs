// Post-process the CommonJS build (tsc -p tsconfig.cjs.json → dist/cjs):
//  1. rename every .js file to .cjs so the ESM-first package
//     ("type": "module") can expose a require() entry point, and
//  2. rewrite relative require("./X.js") calls to require("./X.cjs").
//     Only relative paths are rewritten; bare specifiers and node: imports
//     are left untouched.
import { readdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const dir = fileURLToPath(new URL("../dist/cjs/", import.meta.url));

const entries = await readdir(dir, { withFileTypes: true });
const jsFiles = entries
  .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
  .map((entry) => entry.name);

for (const name of jsFiles) {
  const base = name.slice(0, -".js".length);
  await rename(join(dir, name), join(dir, `${base}.cjs`));
}

for (const name of jsFiles) {
  const base = name.slice(0, -".js".length);
  const path = join(dir, `${base}.cjs`);
  const content = await readFile(path, "utf8");
  const rewritten = content.replace(
    /require\("(\.[^"]*)\.js"\)/g,
    'require("$1.cjs")',
  );
  if (rewritten !== content) {
    await writeFile(path, rewritten);
  }
}
console.log("Renamed CJS output to .cjs and rewrote relative requires");
