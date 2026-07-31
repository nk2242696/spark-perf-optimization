import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import * as vscode from "vscode";

const SKILL_NAME = "spark-perf-optimization";
const OWNERSHIP_FILE = ".spark-perf-extension.json";

export function installedSkillPath(): string {
  return path.join(os.homedir(), ".copilot", "skills", SKILL_NAME);
}

export async function installSkill(
  context: vscode.ExtensionContext,
  interactive = true,
): Promise<string | undefined> {
  const source = path.join(context.extensionUri.fsPath, "skill", SKILL_NAME);
  const target = installedSkillPath();
  const targetParent = path.dirname(target);
  const temporary = `${target}.installing`;
  const backup = `${target}.backup`;

  try {
    await fs.access(path.join(source, "SKILL.md"));
  } catch {
    void vscode.window.showErrorMessage(
      "The extension package does not contain the Spark performance skill bundle.",
    );
    return undefined;
  }

  const targetExists = await exists(target);
  if (targetExists && interactive) {
    const choice = await vscode.window.showWarningMessage(
      "A Spark performance skill is already installed. Replace it with the bundled version?",
      { modal: true },
      "Replace",
    );
    if (choice !== "Replace") {
      return undefined;
    }
  }

  await fs.mkdir(targetParent, { recursive: true });
  await fs.rm(temporary, { recursive: true, force: true });
  await fs.rm(backup, { recursive: true, force: true });
  await fs.cp(source, temporary, { recursive: true });
  await validateSkill(temporary);
  await fs.writeFile(
    path.join(temporary, OWNERSHIP_FILE),
    JSON.stringify({ extension: context.extension.id, version: context.extension.packageJSON.version }, null, 2),
    "utf8",
  );

  try {
    if (targetExists) {
      await fs.rename(target, backup);
    }
    await fs.rename(temporary, target);
    await fs.rm(backup, { recursive: true, force: true });
  } catch (error) {
    await fs.rm(temporary, { recursive: true, force: true });
    if (!(await exists(target)) && (await exists(backup))) {
      await fs.rename(backup, target);
    }
    throw error;
  }

  if (interactive) {
    void vscode.window.showInformationMessage(
      "Spark Performance Optimization skill installed. Reload VS Code to refresh skill discovery.",
      "Reload",
    ).then((choice) => {
      if (choice === "Reload") {
        void vscode.commands.executeCommand("workbench.action.reloadWindow");
      }
    });
  }
  return target;
}

export async function uninstallSkill(context: vscode.ExtensionContext): Promise<void> {
  const target = installedSkillPath();
  if (!(await exists(path.join(target, OWNERSHIP_FILE)))) {
    void vscode.window.showWarningMessage(
      "No extension-owned Spark performance skill installation was found.",
    );
    return;
  }

  const choice = await vscode.window.showWarningMessage(
    `Remove ${target}?`,
    { modal: true },
    "Uninstall",
  );
  if (choice !== "Uninstall") {
    return;
  }

  const ownership = JSON.parse(
    await fs.readFile(path.join(target, OWNERSHIP_FILE), "utf8"),
  ) as { extension?: string };
  if (ownership.extension !== context.extension.id) {
    throw new Error("The installed skill is not owned by this extension.");
  }
  await fs.rm(target, { recursive: true, force: true });
  void vscode.window.showInformationMessage("Spark Performance Optimization skill uninstalled.");
}

async function validateSkill(root: string): Promise<void> {
  const required = [
    "SKILL.md",
    path.join("scripts", "spark_eventlog_analyze.py"),
    path.join("references", "analyzer-usage.md"),
    path.join("assets", "pr-description-template.md"),
  ];
  await Promise.all(required.map((relativePath) => fs.access(path.join(root, relativePath))));
  const skill = await fs.readFile(path.join(root, "SKILL.md"), "utf8");
  if (!skill.startsWith("---\n") || !skill.includes(`name: ${SKILL_NAME}`)) {
    throw new Error("Invalid SKILL.md frontmatter or skill name.");
  }
}

async function exists(target: string): Promise<boolean> {
  try {
    await fs.access(target);
    return true;
  } catch {
    return false;
  }
}