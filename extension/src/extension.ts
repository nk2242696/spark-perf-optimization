import * as vscode from "vscode";
import { analyzeEventLog, openCliTerminal } from "./analyzer";
import { installSkill, uninstallSkill } from "./skillInstaller";

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel("Spark Performance Optimization");
  context.subscriptions.push(
    output,
    vscode.commands.registerCommand("sparkPerf.installSkill", () => installSkill(context)),
    vscode.commands.registerCommand("sparkPerf.uninstallSkill", () => uninstallSkill(context)),
    vscode.commands.registerCommand("sparkPerf.analyzeEventLog", () => analyzeEventLog(context, output)),
    vscode.commands.registerCommand("sparkPerf.openCliTerminal", () => openCliTerminal(context)),
  );
}

export function deactivate(): void {
  // Resources registered in the extension context are disposed automatically.
}