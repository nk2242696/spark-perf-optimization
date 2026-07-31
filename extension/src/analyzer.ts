import { spawn } from "node:child_process";
import * as path from "node:path";
import * as vscode from "vscode";
import { installSkill, installedSkillPath } from "./skillInstaller";

export async function analyzeEventLog(
  context: vscode.ExtensionContext,
  output: vscode.OutputChannel,
): Promise<void> {
  const selected = await vscode.window.showOpenDialog({
    canSelectFiles: true,
    canSelectFolders: false,
    canSelectMany: false,
    openLabel: "Analyze Event Log",
    filters: {
      "Spark event logs": ["zip", "gz", "json", "eventlog"],
      "All files": ["*"],
    },
  });
  if (!selected?.[0]) {
    return;
  }

  const skillRoot = await ensureSkill(context);
  if (!skillRoot) {
    return;
  }
  const python = configuredPython();
  const analyzer = path.join(skillRoot, "scripts", "spark_eventlog_analyze.py");
  output.clear();
  output.show(true);

  await vscode.window.withProgress(
    {
      location: vscode.ProgressLocation.Notification,
      title: "Analyzing Spark event log",
      cancellable: true,
    },
    (_progress, token) => runAnalyzer(python, analyzer, selected[0].fsPath, output, token),
  );
}

export async function openCliTerminal(context: vscode.ExtensionContext): Promise<void> {
  const skillRoot = await ensureSkill(context);
  if (!skillRoot) {
    return;
  }
  const terminal = vscode.window.createTerminal({
    name: "Spark Perf CLI",
    cwd: vscode.Uri.file(skillRoot),
  });
  const analyzer = path.join("scripts", "spark_eventlog_analyze.py");
  terminal.show();
  const command = `${quote(configuredPython())} ${quote(analyzer)} --help`;
  terminal.sendText(process.platform === "win32" ? `& ${command}` : command);
}

async function ensureSkill(context: vscode.ExtensionContext): Promise<string | undefined> {
  try {
    await vscode.workspace.fs.stat(vscode.Uri.file(path.join(installedSkillPath(), "SKILL.md")));
    return installedSkillPath();
  } catch {
    return installSkill(context, false);
  }
}

function configuredPython(): string {
  const configured = vscode.workspace.getConfiguration("sparkPerf").get<string>("pythonPath", "").trim();
  if (configured) {
    return configured;
  }
  return process.platform === "win32" ? "python" : "python3";
}

function runAnalyzer(
  python: string,
  analyzer: string,
  eventLog: string,
  output: vscode.OutputChannel,
  token: vscode.CancellationToken,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(python, [analyzer, "all", eventLog], { windowsHide: true });
    token.onCancellationRequested(() => child.kill());
    child.stdout.on("data", (data: Buffer) => output.append(data.toString()));
    child.stderr.on("data", (data: Buffer) => output.append(data.toString()));
    child.on("error", (error) => {
      void vscode.window.showErrorMessage(`Unable to start Python: ${error.message}`);
      reject(error);
    });
    child.on("close", (code) => {
      if (token.isCancellationRequested) {
        output.appendLine("\nAnalysis cancelled.");
        resolve();
      } else if (code === 0) {
        resolve();
      } else {
        const error = new Error(`Analyzer exited with code ${code ?? "unknown"}.`);
        void vscode.window.showErrorMessage(error.message);
        reject(error);
      }
    });
  });
}

function quote(value: string): string {
  return `"${value.replaceAll('"', '\\"')}"`;
}