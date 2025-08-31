#!/usr/bin/env tsx
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { execSync } from "node:child_process";

export function getRepoUrl(): string {
  const pkgPath = join(process.cwd(), "package.json");

  // 1. Try package.json.repository
  try {
    const pkg = JSON.parse(readFileSync(pkgPath, "utf8")) as {
      repository?: { url?: string } | string;
    };

    if (pkg.repository) {
      const repoUrl = typeof pkg.repository === "string"
        ? pkg.repository
        : pkg.repository.url;

      if (repoUrl) return normalizeGitUrl(repoUrl);
    }
  } catch {
    // ignore
  }

  // 2. Fallback: git remote
  try {
    const remoteUrl = execSync("git remote get-url origin", { encoding: "utf8" }).trim();
    return normalizeGitUrl(remoteUrl);
  } catch {
    // ignore
  }

  throw new Error("❌ Could not determine repository URL (package.json.repository or git remote)");
}

function normalizeGitUrl(url: string): string {
  if (url.startsWith("git@")) {
    // e.g. git@gitlab.com:org/repo.git
    return url.replace(/^git@/, "https://").replace(":", "/").replace(/\.git$/, "");
  }
  if (url.startsWith("http")) {
    return url.replace(/\.git$/, "");
  }
  return url;
}