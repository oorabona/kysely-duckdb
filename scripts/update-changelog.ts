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

// Use a single multiline regex to capture the Unreleased section and its
// content until the next boundary:
//  - next H2 heading (## ...), regardless of version
//  - a thematic break (---) line
//  - end of file
// We allow custom prefixes before headings (e.g., "#sym:").
const unreleasedBlock = /^(?<prefix>[^\n]*?##\s*\[?Unreleased\]?[^\n]*\n)(?<content>[\s\S]*?)(?=^##\s|^\s*---\s*$|$(?![\s\S]))/im;
const m = changelog.match(unreleasedBlock);

if (!m || !m.groups) {
  console.error("❌ No [Unreleased] section found in CHANGELOG.md");
  process.exit(1);
}

const unreleasedContent = m.groups.content.trim();

// Enforce non-empty Unreleased section
if (!unreleasedContent) {
  console.error("❌ [Unreleased] section is empty");
  process.exit(1);
}

// Build the new version entry.
const newEntryLines: string[] = [];
newEntryLines.push(`## [${tag}] - ${date}`);
newEntryLines.push(""); // blank line before content
newEntryLines.push(unreleasedContent);
newEntryLines.push(""); // trailing blank line after the entry

// Rebuild the changelog by replacing the captured block with the preserved
// Unreleased heading, a blank line, then the new entry.
changelog = changelog.replace(
  unreleasedBlock,
  `${m.groups.prefix}\n${newEntryLines.join("\n")}\n`,
);

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

// Inject or update footer links. We accept that link reference definitions may
// appear anywhere in the file; we normalize them by ensuring they exist and
// updating their values. We'll update the first occurrence if present.
const lines2 = changelog.split(/\r?\n/);
let foundUnreleasedLink = false;
let insertedTagLink = false;

for (let i = 0; i < lines2.length; i++) {
  const ln = lines2[i];
  if (/^\[Unreleased\]:/i.test(ln)) {
    lines2[i] = unreleasedLine;
    foundUnreleasedLink = true;
  }
  if (new RegExp(`^\\[${tag.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}]:`, 'i').test(ln)) {
    lines2[i] = linkLine;
    insertedTagLink = true;
  }
}

if (!foundUnreleasedLink) {
  lines2.push(unreleasedLine);
}
if (!insertedTagLink) {
  lines2.push(linkLine);
}

changelog = lines2.join("\n");

writeFileSync(changelogPath, changelog, "utf8");
console.log(`✅ CHANGELOG.md updated with version ${tag} (${repoUrl})`);