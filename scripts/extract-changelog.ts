#!/usr/bin/env tsx
import { readFileSync } from "node:fs";
import { join } from "node:path";

const version = process.argv[2];
if (!version) {
  console.error("Usage: pnpm tsx scripts/extract-changelog.ts <version>");
  process.exit(1);
}

const tag = `v${version}`;
const changelogPath = join(process.cwd(), "CHANGELOG.md");
const changelog = readFileSync(changelogPath, "utf8");

// Match the section for this version.
// Build a robust multiline regex that captures from the version heading
//   "## [vX.Y.Z] ..." up to (but not including) the next H2 (line starting with "## "),
//   a thematic break (---) line, or end of file. We intentionally do not stop
//   at H3 (### ...) so that subsections are included.
// NOTE: When constructing a RegExp from a string, backslashes must be escaped (\\s, \\n, etc.).
function escapeRe(input: string) {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const tagRe = escapeRe(tag);
const versionBlock = new RegExp(
  `^(?<prefix>[^\n]*?##\\s*\\[?${tagRe}\\]?[^\n]*\r?\n)(?<content>[\\s\\S]*?)(?=^##\\s|^\\s*---\\s*$|$(?![\\s\\S]))`,
  'm',
);

const m = changelog.match(versionBlock);

if (!m || !m.groups) {
  console.error(`❌ No [${tag}] section found in CHANGELOG.md`);
  process.exit(1);
}

const versionContent = m.groups.content.trim();

// Enforce non-empty Unreleased section
if (!versionContent) {
  console.error(`❌ No changelog entry found for ${tag}`);
  process.exit(1);
}


const entry = m[0].trim();

// Print in a nicer release notes format
console.log(`# Release ${tag}\n\n${entry}`);