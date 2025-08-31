#!/usr/bin/env tsx
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { getRepoUrl } from "./utils/get-repo.js";

const version = process.argv[2];
if (!version) {
  console.error("Usage: pnpm tsx scripts/update-changelog.ts <version>");
  process.exit(1);
}

const date = new Date().toISOString().split("T")[0];
const tag = `v${version}`;
const changelogPath = join(process.cwd(), "CHANGELOG.md");

const repoUrl = getRepoUrl(); // 🔥 Auto-detect from package.json or git remote

let changelog = readFileSync(changelogPath, "utf8");

const unreleasedRegex = /(## \[Unreleased\]\s*)([\s\S]*?)(?=##|$)/i;
const match = unreleasedRegex.exec(changelog);

if (!match) {
  console.error("❌ No [Unreleased] section found in CHANGELOG.md");
  process.exit(1);
}

const unreleasedContent = match[2].trim();
if (!unreleasedContent) {
  console.error("❌ [Unreleased] section is empty");
  process.exit(1);
}

const newEntry = `## [${tag}] - ${date}\n${unreleasedContent}\n\n`;
changelog = changelog.replace(unreleasedRegex, `## [Unreleased]\n\n${newEntry}`);

// --- build correct URLs depending on host ---
let linkLine: string;
let unreleasedLine: string;

if (repoUrl.includes("github.com")) {
  linkLine = `[${tag}]: ${repoUrl}/releases/tag/${tag}`;
  unreleasedLine = `[Unreleased]: ${repoUrl}/compare/${tag}...HEAD`;
} else if (repoUrl.includes("gitlab")) {
  linkLine = `[${tag}]: ${repoUrl}/-/tags/${tag}`;
  unreleasedLine = `[Unreleased]: ${repoUrl}/-/compare/${tag}...HEAD`;
} else {
  // fallback generic remote
  linkLine = `[${tag}]: ${repoUrl}`;
  unreleasedLine = `[Unreleased]: ${repoUrl}`;
}

// inject or append links
if (changelog.includes("[Unreleased]:")) {
  changelog = changelog.replace(/\[Unreleased\]:.*\n/, `${unreleasedLine}\n${linkLine}\n`);
} else {
  changelog += `\n${unreleasedLine}\n${linkLine}\n`;
}

writeFileSync(changelogPath, changelog, "utf8");
console.log(`✅ CHANGELOG.md updated with version ${tag} (${repoUrl})`);