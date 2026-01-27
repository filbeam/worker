import fs from "fs";

function fail(msg) {
  console.error("❌", msg);
  process.exit(1);
}

function read(file) {
  return fs.readFileSync(file, "utf8");
}

// Root files (script runs inside worker/)
const rootPkg = JSON.parse(read("../package.json"));
const vitest = read("../vitest.workspace.js");
const tsconfig = read("../tsconfig.json");
const ci = read("../.github/workflows/ci.yml");

// 1. Get workers from workspaces
const workspaces = rootPkg.workspaces || [];
const workers = workspaces.map(w => w.replace("./", ""));

if (!workers.length) {
  fail("No workspaces found in root package.json");
}

console.log("Found workers:", workers.join(", "));

// 2. Validate each worker is registered everywhere
for (const worker of workers) {
  if (!vitest.includes(worker)) {
    fail(`${worker} missing from vitest.workspace.js`);
  }

  if (!tsconfig.includes(worker)) {
    fail(`${worker} missing from tsconfig.json project references (run npm run update-ts-project)`);
  }

  if (!ci.includes(worker)) {
    fail(`${worker} missing from ci.yml deploy job`);
  }
}

console.log("✅ All workers are registered correctly.");
