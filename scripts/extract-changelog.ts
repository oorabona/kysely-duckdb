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

// Match the section for this version
const entryRegex = new RegExp(`## \\[${tag}\\][\\s\\S]*?(?=##|$)`);
const match = changelog.match(entryRegex);

if (!match) {
  console.error(`❌ No changelog entry found for ${tag}`);
  process.exit(1);
}

const entry = match[0].trim();

// Print in a nicer release notes format
console.log(`# Release ${tag}\n\n${entry}`);