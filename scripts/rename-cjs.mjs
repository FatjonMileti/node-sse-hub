// Post-process the CommonJS build (tsc -p tsconfig.cjs.json → dist/cjs):
//  1. rename every .js file to .cjs so the ESM-first package
//     ("type": "module") can expose a require() entry point, and
//  2. rewrite relative require("./X.js") calls to require("./X.cjs").
//     Only relative paths are rewritten; bare specifiers and node: imports
//     are left untouched.
//  3. rename every .d.ts file to .d.cts (typed by CJS consumers via the
//     "require" types condition) and rewrite relative
//     from "./X.js" specifiers to from "./X.cjs", which resolves to the
//     sibling .d.cts under node16 resolution.
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

const dtsFiles = entries
  .filter((entry) => entry.isFile() && entry.name.endsWith(".d.ts"))
  .map((entry) => entry.name);

for (const name of dtsFiles) {
  const base = name.slice(0, -".d.ts".length);
  await rename(join(dir, name), join(dir, `${base}.d.cts`));
}

for (const name of dtsFiles) {
  const base = name.slice(0, -".d.ts".length);
  const path = join(dir, `${base}.d.cts`);
  const content = await readFile(path, "utf8");
  const rewritten = content.replace(
    /from "(\.[^"]*)\.js"/g,
    'from "$1.cjs"',
  );
  if (rewritten !== content) {
    await writeFile(path, rewritten);
  }
}
console.log("Renamed CJS output to .cjs/.d.cts and rewrote relative paths");
