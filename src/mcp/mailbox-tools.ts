/**
 * C2C Mailbox MCP tools.
 *
 * These tools allow ChatGPT Web to:
 *   - Read pending messages (PLAN_REQUEST, EXECUTION_REPORT)
 *   - Submit PLAN_RESPONSE and REVIEW_RESPONSE
 *
 * Control-plane ONLY. No source write, no shell, no DB.
 *
 * Authorization: mailbox.read (list/get) | mailbox.write (submit)
 *
 * Tool names are namespaced under "mailbox_" to avoid collisions with
 * existing and future upstream tools.
 */

import { z } from "zod";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import type { AuthInfo } from "@modelcontextprotocol/sdk/server/auth/types.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { getMailboxStore, mailboxFile } from "../mailbox/store.js";
import { getStateDir } from "../config/paths.js";
import type { Logger } from "../logger/index.js";

// ---------------------------------------------------------------------------
// Helpers â€” ToolResult and adapter
// ---------------------------------------------------------------------------

type ToolResult = {
  content: { type: "text"; text: string }[];
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
};

type HandlerExtra = { authInfo?: AuthInfo | undefined };

export interface DispatchBackendInput {
  workspaceRoot: string;
  workspaceId: string;
  request_id: string;
  round: number;
  workspaceRegistryFile?: string;
}
export type DispatchBackendInvoker = (input: DispatchBackendInput) => Record<string, unknown>;

export interface ExecutionAgentsBackendInput { workspaceRoot: string; workspaceId: string; workspaceRegistryFile?: string }
export type ExecutionAgentsBackendInvoker = (input: ExecutionAgentsBackendInput) => Record<string, unknown>;

export interface V2TaskCreationBackendInput {
  workspaceRoot: string;
  workspaceId: string;
  request_id: string;
  task: string;
  agent_id?: string;
  requested_model?: string;
  workspaceRegistryFile?: string;
}
export type V2TaskCreationBackendDiagnostic = Record<string, string | number | boolean | null | undefined>;
export type V2TaskCreationBackendInvoker = (
  input: V2TaskCreationBackendInput,
  reportDiagnostic?: (diagnostic: V2TaskCreationBackendDiagnostic) => void
) => Record<string, unknown> | Promise<Record<string, unknown>>;

export type V2LifecycleBackendInput = {
  workspaceRoot: string;
  workspaceId: string;
  request_id: string;
  round: number;
  workspaceRegistryFile?: string;
} & ({ operation: "critique"; agent_id: string; critique: string; requested_model?: string }
  | { operation: "decision"; decision: "APPROVE" | "REVISE" | "BLOCK"; plan?: string;
      agent_id?: string; requested_model?: string });
export type V2LifecycleBackendDiagnostic = Record<string, string | number | boolean | null | undefined>;
export type V2LifecycleBackendInvoker = (
  input: V2LifecycleBackendInput,
  reportDiagnostic?: (diagnostic: V2LifecycleBackendDiagnostic) => void
) => Record<string, unknown> | Promise<Record<string, unknown>>;

export type V2ExecutionControlInput = { workspaceRoot: string; workspaceId: string; request_id: string; round: number;
  workspaceRegistryFile?: string;
  operation: "review" | "reroute"; verdict?: "APPROVED" | "FIX_REQUIRED" | "BLOCKED";
  summary?: string; agent_id?: string; requested_model?: string };
export type V2ExecutionControlInvoker = (input: V2ExecutionControlInput) => Record<string, unknown> | Promise<Record<string, unknown>>;
export type V2ExecutionWakeInput = { workspaceRoot: string; workspaceId: string; request_id: string; round: number;
  workspaceRegistryFile?: string };
export type V2ExecutionWakeInvoker = (input: V2ExecutionWakeInput) => Record<string, unknown> | Promise<Record<string, unknown>>;
export type RegisteredRequestWorkspaceLookup = (requestId: string, selectedWorkspaceId: string) => string | null;

function validExecutionAgentsResult(value: unknown, workspaceId: string): value is Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const result = value as Record<string, unknown>;
  if (result.workspace_id !== workspaceId || !Array.isArray(result.agents) ||
      result.agents.length < 2 || result.agents.length > 5) return false;
  const agents = result.agents as unknown[];
  const ids = new Set<string>();
  for (const value of agents) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const agent = value as Record<string, unknown>;
    if (!["codex", "cursor", "opencode", "copilot", "vscode"].includes(String(agent.id)) || ids.has(String(agent.id)) ||
        typeof agent.display_name !== "string" || typeof agent.installed !== "boolean" ||
        typeof agent.available !== "boolean" || typeof agent.execution_supported !== "boolean" ||
        typeof agent.model_selection_supported !== "boolean" || !Array.isArray(agent.models) ||
        !agent.models.every((model) => typeof model === "string") ||
        !(agent.default_model === null || typeof agent.default_model === "string") ||
        typeof agent.model_discovery_supported !== "boolean" ||
        !(agent.limitations === null || typeof agent.limitations === "string")) return false;
    ids.add(String(agent.id));
  }
  return ids.has("codex") && ids.has("cursor");
}

const DISPATCH_MAX_OUTPUT = 64 * 1024;
const DISPATCH_TIMEOUT_MS = 30000;
const EXECUTION_AGENTS_MAX_OUTPUT = 512 * 1024;
const EXECUTION_AGENTS_TIMEOUT_MS = 30000;
const V2_TASK_MAX_OUTPUT = 64 * 1024;
// Task creation performs canonical agent and model discovery before it can
// accept the task. Allow that cold-start path to finish while keeping a hard
// upper bound on the Python child.
const V2_TASK_TIMEOUT_MS = 60000;
const PRIMARY_C2C_WORKSPACE_ID = "b1c422b1eab0";
function canonicalC2CRoot(registryFile?: string): string | null {
  if (!registryFile) return null;
  try {
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8").replace(/^\uFEFF/, "")) as Record<string, unknown>;
    const root = registry[PRIMARY_C2C_WORKSPACE_ID];
    return typeof root === "string" && path.isAbsolute(root) ? path.resolve(root) : null;
  } catch { return null; }
}

function pythonBackendEnv(registryFile?: string): NodeJS.ProcessEnv {
  const canonicalRoot = canonicalC2CRoot(registryFile);
  return { ...process.env,
    ...(!process.env.PYTHONPATH && canonicalRoot ? { PYTHONPATH: canonicalRoot } : {}) };
}
/** Resolve the shared machine config for V2 host operations. */
function autonomousConfigPath(workspaceRoot: string, registryFile?: string): string {
  const configured = process.env.C2C_AUTONOMOUS_CONFIG?.trim();
  if (configured && path.isAbsolute(configured)) return path.resolve(configured);

  const canonicalConfig = process.env.C2C_CONFIG_LOCAL?.trim();
  if (canonicalConfig && path.isAbsolute(canonicalConfig)) {
    const shared = path.join(path.dirname(path.resolve(canonicalConfig)), "autonomous.local.json");
    return shared;
  }

  const pythonRoot = (process.env.PYTHONPATH ?? "")
    .split(path.delimiter)
    .map((entry) => entry.trim())
    .find((entry) => entry && path.isAbsolute(entry));
  if (pythonRoot) {
    const shared = path.join(path.resolve(pythonRoot), "config", "autonomous.local.json");
    return shared;
  }

  // A registered project is not required to carry a private C2C config copy.
  // Resolve the shared backend config from the canonical registry when a
  // bridge was launched without the launcher's explicit environment.
  return path.join(canonicalC2CRoot(registryFile) ?? workspaceRoot, "config", "autonomous.local.json");
}

interface V2TaskChildResult {
  stdout: string;
  stderr: string;
  status: number | null;
  signal: NodeJS.Signals | null;
  error?: NodeJS.ErrnoException;
  timedOut?: boolean;
  outputLimit?: boolean;
}

function terminateV2TaskChild(child: ChildProcess): void {
  if (process.platform === "win32" && child.pid) {
    const killer = spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
      windowsHide: true, stdio: "ignore", shell: false,
    });
    killer.once("error", () => { try { child.kill(); } catch { /* already exited */ } });
    killer.once("close", (code) => {
      if (code !== 0) { try { child.kill(); } catch { /* already exited */ } }
    });
    return;
  }
  try { child.kill("SIGKILL"); } catch { /* already exited */ }
}

function runV2TaskChild(command: { executable: string; args: string[]; cwd: string; workspaceRegistryFile?: string }, timeoutMs = V2_TASK_TIMEOUT_MS): Promise<V2TaskChildResult> {
  return new Promise((resolve) => {
    const child = spawn(command.executable, command.args, {
      cwd: command.cwd, windowsHide: true, shell: false, stdio: ["ignore", "pipe", "pipe"],
      env: pythonBackendEnv(command.workspaceRegistryFile),
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let error: NodeJS.ErrnoException | undefined;
    let timedOut = false;
    let outputLimit = false;
    let settled = false;
    const stopChild = (reason: "timeout" | "output_limit") => {
      if (error) return;
      error = Object.assign(new Error(reason === "timeout" ? "Python backend timed out" : "Python backend output limit exceeded"), {
        code: reason === "timeout" ? "ETIMEDOUT" : "ERR_CHILD_PROCESS_STDIO_MAXBUFFER",
      });
      timedOut = reason === "timeout";
      outputLimit = reason === "output_limit";
      terminateV2TaskChild(child);
    };
    const timer = setTimeout(() => stopChild("timeout"), timeoutMs);
    const collect = (target: Buffer[], chunk: Buffer, stream: "stdout" | "stderr") => {
      if (error) return;
      if (stream === "stdout") stdoutBytes += chunk.length;
      else stderrBytes += chunk.length;
      if ((stream === "stdout" ? stdoutBytes : stderrBytes) > V2_TASK_MAX_OUTPUT) {
        stopChild("output_limit");
        return;
      }
      target.push(chunk);
    };
    child.stdout?.on("data", (chunk: Buffer) => collect(stdout, chunk, "stdout"));
    child.stderr?.on("data", (chunk: Buffer) => collect(stderr, chunk, "stderr"));
    child.once("error", (spawnError: NodeJS.ErrnoException) => { error ??= spawnError; });
    child.once("close", (status, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ stdout: Buffer.concat(stdout).toString("utf8"), stderr: Buffer.concat(stderr).toString("utf8"),
        status, signal, ...(error ? { error } : {}), ...(timedOut ? { timedOut } : {}),
        ...(outputLimit ? { outputLimit } : {}) });
    });
  });
}

export function buildV2TaskCreationCommand(input: V2TaskCreationBackendInput, python: string) {
  const configPath = autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile);
  return {
    executable: python,
    cwd: input.workspaceRoot,
    workspaceRegistryFile: input.workspaceRegistryFile,
    configPath,
    args: ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
      "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config", configPath,
      "web-task", "--request-id", input.request_id, "--task", input.task,
      ...(input.agent_id ? ["--agent-id", input.agent_id] : []),
      ...(input.requested_model ? ["--requested-model", input.requested_model] : []),
      "--submit", "--json"],
  };
}

export function buildV2LifecycleCommand(input: V2LifecycleBackendInput, python: string) {
  const configPath = autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile);
  const args = ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
    "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config", configPath];
  if (input.operation === "critique") {
    args.push("v2-critique", "--request-id", input.request_id, "--round", String(input.round),
      "--agent-id", input.agent_id, "--critique", input.critique);
    if (input.requested_model) args.push("--requested-model", input.requested_model);
  } else {
    args.push("v2-decision", "--request-id", input.request_id, "--round", String(input.round),
      "--decision", input.decision.toLowerCase(), "--plan", input.plan ?? "");
    if (input.agent_id) args.push("--agent-id", input.agent_id);
    if (input.requested_model) args.push("--requested-model", input.requested_model);
  }
  args.push("--json");
  return { executable: python, cwd: input.workspaceRoot, configPath, args, workspaceRegistryFile: input.workspaceRegistryFile };
}

export function buildV2ExecutionControlCommand(input: V2ExecutionControlInput, python: string) {
  const configPath = autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile);
  const command = { review: "v2-review", reroute: "v2-reroute" }[input.operation];
  const args = ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
    "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config", configPath,
    command, "--request-id", input.request_id, "--round", String(input.round)];
  if (input.operation === "review") args.push("--verdict", input.verdict!, "--summary", input.summary!);
  if (input.agent_id !== undefined) args.push("--agent-id", input.agent_id);
  if (input.requested_model !== undefined) args.push("--requested-model", input.requested_model);
  args.push("--json");
  return { executable: python, cwd: input.workspaceRoot, configPath, args, workspaceRegistryFile: input.workspaceRegistryFile };
}

export function buildV2ExecutionWakeCommand(input: V2ExecutionWakeInput, python: string) {
  const configPath = autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile);
  return { executable: python, cwd: input.workspaceRoot, configPath, workspaceRegistryFile: input.workspaceRegistryFile,
    args: ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
      "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config", configPath,
      "v2-recover-wake", "--request-id", input.request_id, "--round", String(input.round), "--json"] };
}

const V2_WAKE_ERRORS = new Set(["V2_DISABLED", "REQUEST_NOT_FOUND", "REQUEST_ID_INVALID", "REQUEST_ID_MISMATCH",
  "WORKSPACE_MISMATCH", "ROUND_MISMATCH", "STATE_CONFLICT", "REQUEST_BUSY", "REQUEST_INCOMPATIBLE",
  "REPORT_NOT_DURABLE", "REPORT_MISMATCH", "BACKEND_UNAVAILABLE"]);
function safeWakeError(value: unknown): string {
  return typeof value === "string" && V2_WAKE_ERRORS.has(value) ? value : "BACKEND_UNAVAILABLE";
}

export async function invokeV2ExecutionWakeBackend(input: V2ExecutionWakeInput): Promise<Record<string, unknown>> {
  const command = buildV2ExecutionWakeCommand(input,
    process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python");
  if (!fs.existsSync(command.configPath)) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
  try {
    const result = await runV2TaskChild(command);
    if (result.error || result.timedOut || result.outputLimit) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
    const parsed: unknown = JSON.parse(result.stdout.trim());
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
    const output = parsed as Record<string, unknown>;
    if (output.ok === false) return { ok: false, error_code: safeWakeError(output.error_code) };
    if (result.status !== 0 || output.ok !== true || output.request_id !== input.request_id ||
        output.workspace_id !== input.workspaceId || output.round !== input.round || typeof output.state !== "string") {
      return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
    }
    return output;
  } catch { return { ok: false, error_code: "BACKEND_UNAVAILABLE" }; }
}

export async function invokeV2ExecutionControlBackend(input: V2ExecutionControlInput): Promise<Record<string, unknown>> {
  const command = buildV2ExecutionControlCommand(input,
    process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python");
  if (!fs.existsSync(command.configPath)) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
  try {
    const result = await runV2TaskChild(command, V2_TASK_TIMEOUT_MS);
    if (result.error) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
    const parsed: unknown = JSON.parse(result.stdout.trim());
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
    const output = parsed as Record<string, unknown>;
    if (output.ok === false) return { ok: false, error_code: safeExecutionControlError(output.error_code) };
    if (output.ok !== true || output.request_id !== input.request_id || output.workspace_id !== input.workspaceId ||
        !validExecutionControlState(input, output) || result.status !== 0) return { ok: false, error_code: "BACKEND_UNAVAILABLE" };
    return output;
  } catch { return { ok: false, error_code: "BACKEND_UNAVAILABLE" }; }
}

function safeExecutionControlError(value: unknown): string {
  const allowed = new Set(["V2_DISABLED", "REQUEST_NOT_FOUND", "REQUEST_ID_INVALID", "REQUEST_ID_MISMATCH",
    "WORKSPACE_MISMATCH", "ROUND_MISMATCH", "STATE_CONFLICT", "REQUEST_BUSY", "REQUEST_INCOMPATIBLE",
    "REVIEW_VERDICT_INVALID", "REVIEW_SUMMARY_REQUIRED", "REVIEW_ROUTING_CONFLICT",
    "REVIEW_RESPONSE_CONFLICT", "REVIEW_RESPONSE_INVALID", "REVIEW_RESPONSE_NOT_FOUND",
    "REVIEW_RESPONSE_MISMATCH", "REVIEW_RESPONSE_PROTOCOL_MISMATCH", "REVIEW_RESPONSE_AMBIGUOUS",
    "FIX_ASSIGNMENT_INVALID", "ASSIGNMENT_CONFLICT", "ASSIGNMENT_BACKEND_UNAVAILABLE",
    "AGENT_ID_REQUIRED", "AGENT_ID_AMBIGUOUS", "AGENT_NOT_FOUND", "AGENT_OFFLINE",
    "AGENT_WORKSPACE_MISMATCH", "AGENT_PROTOCOL_UNSUPPORTED", "AGENT_CAPABILITY_UNSUPPORTED",
    "MODEL_INVALID", "MODEL_UNSUPPORTED",
    "REPORT_NOT_DURABLE", "REPORT_MISMATCH",
    "ASSIGNMENT_NOT_FOUND", "ASSIGNMENT_NOT_RETRYABLE", "ASSIGNMENT_WORKER_STILL_RUNNING", "ASSIGNMENT_IDENTITY_MISMATCH",
    "ASSIGNMENT_ACTIVE_WRITER", "ASSIGNMENT_NOT_REROUTABLE", "BACKEND_UNAVAILABLE"]);
  return typeof value === "string" && allowed.has(value) ? value : "BACKEND_UNAVAILABLE";
}

function validExecutionControlState(input: V2ExecutionControlInput, output: Record<string, unknown>): boolean {
  if (input.operation === "review") {
    const expected = { APPROVED: "DONE", BLOCKED: "BLOCKED", FIX_REQUIRED: "EXECUTING_FIX" }[input.verdict!];
    return output.state === expected && output.round === input.round + (input.verdict === "FIX_REQUIRED" ? 1 : 0);
  }
  return output.round === input.round && ["EXECUTING_LOCAL", "EXECUTING_FIX"].includes(String(output.state));
}

function v2LifecycleArgvShape(args: string[]): string[] {
  const result = [...args];
  for (let i = 0; i < result.length; i++) {
    if (["--critique", "--plan"].includes(result[i] ?? "") && result[i + 1] !== undefined) {
      result[i + 1] = "<redacted>";
      i++;
    }
  }
  return result;
}

function v2LifecycleErrorCode(stderr: string): string | null {
  const last = stderr.trim().split(/\r?\n/).filter(Boolean).at(-1) ?? "";
  const match = last.match(/(?:orchestrator\.(?:assignments\.AssignmentError|state\.StateError)|ValueError):\s*([A-Z][A-Z0-9_]*)(?::|\s|$)/);
  const allowed = new Set([
    "ASSIGNMENT_NOT_FOUND", "ASSIGNMENT_WRONG_AGENT", "ASSIGNMENT_COMPLETED",
    "ASSIGNMENT_ALREADY_CLAIMED", "ASSIGNMENT_CLAIM_INVALID", "ASSIGNMENT_STORE_INVALID",
    "WORKSPACE_MISMATCH", "ROUND_MISMATCH", "REQUEST_NOT_FOUND", "REQUEST_INCOMPATIBLE",
    "REQUEST_BUSY", "STATE_CONFLICT", "MODEL_REQUEST_MISMATCH", "IDEMPOTENCY_CONFLICT",
    "AGENT_NOT_FOUND", "AGENT_ID_AMBIGUOUS", "AGENT_WORKSPACE_MISMATCH", "AGENT_OFFLINE",
    "AGENT_PROTOCOL_UNSUPPORTED", "AGENT_CAPABILITY_UNSUPPORTED", "MODEL_INVALID", "MODEL_UNSUPPORTED",
  ]);
  return match && allowed.has(match[1] ?? "") ? match[1] ?? null : null;
}

/** Delegate V2 critique/decision transitions to Python's canonical lifecycle CLI. */
export async function invokeV2LifecycleBackend(
  input: V2LifecycleBackendInput,
  reportDiagnostic: (diagnostic: V2LifecycleBackendDiagnostic) => void = () => undefined
): Promise<Record<string, unknown>> {
  const unavailable = () => ({ ok: false, error_code: "BACKEND_UNAVAILABLE",
    message: "V2 lifecycle backend is unavailable" });
  const command = buildV2LifecycleCommand(input,
    process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python");
  const diagnosticBase = {
    operation: input.operation, request_id: input.request_id, workspace_id: input.workspaceId,
    agent_id: "agent_id" in input ? input.agent_id ?? null : null,
    executable: command.executable, resolved_executable: resolveExecutableForDiagnostics(command.executable),
    argv_shape: JSON.stringify(v2LifecycleArgvShape(command.args)), cwd: command.cwd,
    config_path: command.configPath, config_exists: fs.existsSync(command.configPath),
    state_dir: path.join(input.workspaceRoot, "runtime", "state"), node_response: "BACKEND_UNAVAILABLE",
  };
  if (!diagnosticBase.config_exists) {
    reportDiagnostic({ failure: "CONFIG_NOT_FOUND", ...diagnosticBase });
    return unavailable();
  }
  try {
    const result = await runV2TaskChild(command);
    const stdoutBytes = Buffer.byteLength(result.stdout, "utf8");
    const stderrBytes = Buffer.byteLength(result.stderr, "utf8");
    const pythonErrorCode = !result.error && result.status !== 0 ? v2LifecycleErrorCode(result.stderr) : null;
    const base = { ...diagnosticBase, exit_code: result.status, signal: result.signal ?? null,
      stdout_type: typeof result.stdout, stdout_bytes: stdoutBytes, stderr_type: typeof result.stderr,
      stderr_bytes: stderrBytes, stderr_class: classifyPythonStderr(result.stderr),
      python_error_code: pythonErrorCode, node_response: pythonErrorCode ?? "BACKEND_UNAVAILABLE",
      spawn_error_code: result.error?.code ?? null, timed_out: result.timedOut ?? false,
      output_limit: result.outputLimit ?? false };
    if (result.error || result.timedOut || result.outputLimit || stdoutBytes > V2_TASK_MAX_OUTPUT) {
      reportDiagnostic({ failure: result.timedOut ? "TIMEOUT" : result.outputLimit ? "OUTPUT_LIMIT" : "PYTHON_FAILED", ...base });
      return unavailable();
    }
    let parsed: unknown;
    try { parsed = JSON.parse(result.stdout.trim()); }
    catch {
      reportDiagnostic({ failure: "INVALID_JSON", ...base });
      return result.status !== 0 && pythonErrorCode ? { ok: false, error_code: pythonErrorCode,
        message: "Canonical V2 lifecycle validation failed" } : unavailable();
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      reportDiagnostic({ failure: "INVALID_JSON_SHAPE", ...base });
      return unavailable();
    }
    const output = parsed as Record<string, unknown>;
    if (output.ok === false) {
      const errorCode = typeof output.error_code === "string" ? output.error_code : null;
      // JSON is the trusted canonical CLI contract; preserve future machine-safe StateError codes.
      const safeJsonErrorCode = errorCode && /^[A-Z][A-Z0-9_]{0,79}$/.test(errorCode) ? errorCode : null;
      if (result.status !== 0 && !safeJsonErrorCode) {
        reportDiagnostic({ failure: "PYTHON_FAILED", ...base });
        return pythonErrorCode ? { ok: false, error_code: pythonErrorCode,
          message: "Canonical V2 lifecycle validation failed" } : unavailable();
      }
      if (!safeJsonErrorCode) return unavailable();
      reportDiagnostic({ failure: "PYTHON_DOMAIN_ERROR", ...base, node_response: safeJsonErrorCode });
      return { ok: false, error_code: safeJsonErrorCode, message: "Canonical V2 lifecycle validation failed" };
    }
    if (result.status !== 0) {
      reportDiagnostic({ failure: "PYTHON_FAILED", ...base });
      return pythonErrorCode ? { ok: false, error_code: pythonErrorCode,
        message: "Canonical V2 lifecycle validation failed" } : unavailable();
    }
    const expectedCommand = input.operation === "critique" ? "v2-critique" : "v2-decision";
    const expectedState = input.operation === "critique" ? "AWAITING_WEB_PLAN_DECISION"
      : input.decision === "APPROVE" ? "PLAN_APPROVED"
      : input.decision === "REVISE" ? "AWAITING_AGENT_CRITIQUE" : "BLOCKED";
    const expectedRound = input.operation === "decision" && input.decision === "REVISE"
      ? input.round + 1 : input.round;
    if (output.ok !== true || output.command !== expectedCommand || output.request_id !== input.request_id ||
        output.workspace_id !== input.workspaceId || output.round !== expectedRound || output.state !== expectedState) {
      reportDiagnostic({ failure: "CORRELATION_OR_STATE_MISMATCH", ...base,
        python_command: typeof output.command === "string" ? output.command : null,
        python_status: typeof output.status === "string" ? output.status : null,
        python_request_id_matches: output.request_id === input.request_id,
        python_workspace_id_matches: output.workspace_id === input.workspaceId,
        python_round_matches: output.round === expectedRound,
        python_state: typeof output.state === "string" ? output.state : null,
        python_error_code: typeof output.error_code === "string" ? output.error_code : null });
      return unavailable();
    }
    return { ok: true, request_id: input.request_id, workspace_id: input.workspaceId,
      round: expectedRound, state: expectedState, status: output.status };
  } catch (error) {
    reportDiagnostic({ failure: "ADAPTER_EXCEPTION", ...diagnosticBase,
      error_class: error instanceof Error ? error.name : "UnknownError" });
    return unavailable();
  }
}

function v2TaskCreationArgvShape(input: V2TaskCreationBackendInput): string[] {
  return ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
    "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config",
    autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile), "web-task", "--request-id",
    input.request_id, "--task", "<redacted>",
    ...(input.agent_id ? ["--agent-id", input.agent_id] : []),
    ...(input.requested_model ? ["--requested-model", input.requested_model] : []),
    "--submit", "--json"];
}

function resolveExecutableForDiagnostics(executable: string): string {
  if (path.isAbsolute(executable)) return executable;
  const lookup = spawnSync(process.platform === "win32" ? "where.exe" : "which", [executable], {
    encoding: "utf8", windowsHide: true, timeout: 2000, maxBuffer: 8192, shell: false,
  });
  return lookup.status === 0 ? (lookup.stdout ?? "").split(/\r?\n/).find(Boolean) ?? executable : executable;
}

const dispatchStatusContractCache = new Map<string, ReadonlySet<string>>();

function canonicalDispatchStatuses(input: DispatchBackendInput, python: string, configPath: string): ReadonlySet<string> | null {
  const key = `${python}\u0000${input.workspaceRoot}\u0000${configPath}`;
  const cached = dispatchStatusContractCache.get(key);
  if (cached) return cached;
  const args = ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
    "--config", configPath, "dispatch-contract", "--json"];
  try {
    const result = spawnSync(python, args, { cwd: input.workspaceRoot, encoding: "utf8",
      windowsHide: true, timeout: DISPATCH_TIMEOUT_MS, maxBuffer: DISPATCH_MAX_OUTPUT, shell: false,
      env: pythonBackendEnv(input.workspaceRegistryFile) });
    if (result.error || result.status !== 0) return null;
    const parsed: unknown = JSON.parse((result.stdout ?? "").trim());
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    const output = parsed as Record<string, unknown>;
    const statuses = output.backend_statuses;
    if (output.command !== "dispatch-contract" || !Array.isArray(statuses) || statuses.length === 0 ||
        !statuses.every((status) => typeof status === "string" && /^[a-z][a-z0-9_]{0,63}$/.test(status))) return null;
    const contract = new Set(statuses as string[]);
    if (contract.size !== statuses.length || !contract.has("rejected")) return null;
    dispatchStatusContractCache.set(key, contract);
    return contract;
  } catch { return null; }
}

export function invokeDispatchBackend(input: DispatchBackendInput): Record<string, unknown> {
  const unavailable = () => ({ status: "rejected", request_id: input.request_id,
    workspace_id: input.workspaceId, round: input.round, error_code: "BACKEND_UNAVAILABLE",
    message: "Dispatch backend is unavailable" });
  const configPath = autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile);
  if (!fs.existsSync(configPath)) return unavailable();
  const python = process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python";
  const dispatchStatuses = canonicalDispatchStatuses(input, python, configPath);
  if (!dispatchStatuses) return unavailable();
  const args = ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
    "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config", configPath,
    "dispatch-backend", "--request-id", input.request_id, "--round", String(input.round),
    "--mailbox-file", mailboxFile(input.workspaceId)];
  try {
    const result = spawnSync(python, args, { cwd: input.workspaceRoot, encoding: "utf8",
      windowsHide: true, timeout: DISPATCH_TIMEOUT_MS, maxBuffer: DISPATCH_MAX_OUTPUT, shell: false,
      env: pythonBackendEnv(input.workspaceRegistryFile) });
    if (result.error || result.status !== 0 || Buffer.byteLength(result.stdout ?? "", "utf8") > DISPATCH_MAX_OUTPUT) return unavailable();
    const parsed: unknown = JSON.parse((result.stdout ?? "").trim());
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return unavailable();
    const output = parsed as Record<string, unknown>;
    if (typeof output.status !== "string" || !dispatchStatuses.has(output.status) ||
        output.request_id !== input.request_id || output.workspace_id !== input.workspaceId || output.round !== input.round) return unavailable();
    return output;
  } catch {
    return unavailable();
  }
}

export function invokeExecutionAgentsBackend(input: ExecutionAgentsBackendInput): Record<string, unknown> {
  const unavailable = () => ({ workspace_id: input.workspaceId, error: "BACKEND_UNAVAILABLE",
    message: "Execution capability backend is unavailable" });
  const configPath = autonomousConfigPath(input.workspaceRoot, input.workspaceRegistryFile);
  if (!fs.existsSync(configPath)) return unavailable();
  const python = process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python";
  const args = ["-m", "orchestrator.cli_public", "--workspace", input.workspaceRoot,
    "--state-dir", path.join(input.workspaceRoot, "runtime", "state"), "--config", configPath,
    "execution-agents", "--json"];
  try {
    const result = spawnSync(python, args, { cwd: input.workspaceRoot, encoding: "utf8",
      windowsHide: true, timeout: EXECUTION_AGENTS_TIMEOUT_MS,
      maxBuffer: EXECUTION_AGENTS_MAX_OUTPUT, shell: false,
      env: pythonBackendEnv(input.workspaceRegistryFile) });
    if (result.error || result.status !== 0 || Buffer.byteLength(result.stdout ?? "", "utf8") > EXECUTION_AGENTS_MAX_OUTPUT) return unavailable();
    const parsed: unknown = JSON.parse((result.stdout ?? "").trim());
    return validExecutionAgentsResult(parsed, input.workspaceId) ? parsed : unavailable();
  } catch {
    return unavailable();
  }
}

/** Delegate executable task creation to Python's canonical web-task lifecycle. */
export async function invokeV2TaskCreationBackend(
  input: V2TaskCreationBackendInput,
  reportDiagnostic: (diagnostic: V2TaskCreationBackendDiagnostic) => void = () => undefined
): Promise<Record<string, unknown>> {
  const unavailable = () => ({ ok: false, error_code: "BACKEND_UNAVAILABLE",
    message: "V2 task creation backend is unavailable" });
  const python = process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python";
  // Python's canonical V2 route bootstraps AgentRegistry from the same local
  // execution-capability probe used by plan decision and dispatch. Keep public
  // host aliases (such as "codex") intact until that canonical resolver runs.
  const command = buildV2TaskCreationCommand(input, python);
  const configPath = command.configPath;
  if (!fs.existsSync(configPath)) {
    reportDiagnostic({ failure: "CONFIG_NOT_FOUND", cwd: input.workspaceRoot, config_exists: false });
    return unavailable();
  }
  try {
    const result = await runV2TaskChild(command);
    const stdout = result.stdout;
    const stderr = result.stderr;
    if (result.error || Buffer.byteLength(stdout, "utf8") > V2_TASK_MAX_OUTPUT) {
      const spawnError = result.error;
      reportDiagnostic({ failure: result.timedOut ? "TIMEOUT" : result.outputLimit ? "OUTPUT_LIMIT" : "SPAWN_ERROR",
        executable: command.executable, resolved_executable: resolveExecutableForDiagnostics(command.executable),
        cwd: input.workspaceRoot, workspace_root: input.workspaceRoot, workspace_id: input.workspaceId,
        request_id: input.request_id, requested_agent_id: input.agent_id ?? null,
        resolved_agent_id: input.agent_id ?? null,
        argv_shape: JSON.stringify(v2TaskCreationArgvShape(input)),
        config_path: configPath, config_exists: true,
        exit_code: result.status, signal: result.signal ?? null,
        spawn_error_code: spawnError?.code ?? null, stdout_bytes: Buffer.byteLength(stdout, "utf8"),
        stdout_parse: "not_attempted", stderr_bytes: Buffer.byteLength(stderr, "utf8"),
        stderr_class: classifyPythonStderr(stderr) });
      return unavailable();
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(stdout.trim());
    } catch {
    reportDiagnostic({ failure: "INVALID_JSON", executable: command.executable, cwd: input.workspaceRoot,
        resolved_executable: resolveExecutableForDiagnostics(command.executable), workspace_root: input.workspaceRoot,
        workspace_id: input.workspaceId, request_id: input.request_id,
        ...safeRuntimeDiagnostic(input.workspaceId),
        requested_agent_id: input.agent_id ?? null, resolved_agent_id: input.agent_id ?? null,
        argv_shape: JSON.stringify(v2TaskCreationArgvShape(input)), config_path: configPath, config_exists: true,
        exit_code: result.status, stdout_bytes: Buffer.byteLength(stdout, "utf8"), stdout_parse: "invalid_json",
        stderr_bytes: Buffer.byteLength(stderr, "utf8"),
        stderr_class: classifyPythonStderr(stderr), stderr_detail: classifyPythonStderrDetail(stderr),
        stderr_evidence: JSON.stringify(safePythonStderrEvidence(stderr)) });
      return unavailable();
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      reportDiagnostic({ failure: "INVALID_JSON_SHAPE", executable: command.executable, cwd: input.workspaceRoot,
        resolved_executable: resolveExecutableForDiagnostics(command.executable), workspace_root: input.workspaceRoot,
        workspace_id: input.workspaceId, request_id: input.request_id,
        requested_agent_id: input.agent_id ?? null, resolved_agent_id: input.agent_id ?? null,
        argv_shape: JSON.stringify(v2TaskCreationArgvShape(input)), config_path: configPath, config_exists: true,
        exit_code: result.status, stdout_bytes: Buffer.byteLength(stdout, "utf8"), stdout_parse: "wrong_json_shape",
        stderr_bytes: Buffer.byteLength(stderr, "utf8"), stderr_class: classifyPythonStderr(stderr) });
      return unavailable();
    }
    const output = parsed as Record<string, unknown>;
    // Expected Python StateError failures arrive as JSON with a nonzero exit.
    // Interpret the rejection before applying the success-only exit contract.
    if (output.ok === false) {
      reportDiagnostic({ failure: "PYTHON_REJECTED", executable: command.executable, cwd: input.workspaceRoot,
        workspace_id: input.workspaceId, request_id: input.request_id,
        python_error_code: typeof output.error_code === "string" ? output.error_code : null,
        python_status: typeof output.status === "string" ? output.status : null,
        exit_code: result.status, stderr_bytes: Buffer.byteLength(stderr, "utf8"),
        stderr_class: classifyPythonStderr(stderr) });
      const errorCode = typeof output.error_code === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(output.error_code)
        ? output.error_code : "V2_TASK_CREATION_FAILED";
      return { ok: false, error_code: errorCode, message: "Python rejected V2 task creation" };
    }
    if (output.ok !== true) return unavailable();
    const details = output.details && typeof output.details === "object" && !Array.isArray(output.details)
      ? output.details as Record<string, unknown> : {};
    const receipt = details.receipt && typeof details.receipt === "object" && !Array.isArray(details.receipt)
      ? details.receipt as Record<string, unknown> : null;
    if (result.status !== 0 || output.command !== "web-task" || output.status !== "WEB_TASK_CREATED" ||
        output.request_id !== input.request_id || output.workspace_id !== input.workspaceId || output.round !== 1 ||
        typeof output.state !== "string" || !receipt || typeof receipt.message_id !== "string" ||
        receipt.request_id !== input.request_id || receipt.type !== "TASK_REQUEST" || receipt.round !== 1) {
      reportDiagnostic({ failure: "OUTPUT_CONTRACT_MISMATCH", executable: command.executable, cwd: input.workspaceRoot,
        workspace_id: input.workspaceId, request_id: input.request_id, exit_code: result.status,
        output_command: typeof output.command === "string" ? output.command : null,
        output_status: typeof output.status === "string" ? output.status : null,
        output_request_id: typeof output.request_id === "string" ? output.request_id : null,
        output_workspace_id: typeof output.workspace_id === "string" ? output.workspace_id : null,
        output_round: typeof output.round === "number" ? output.round : null,
        receipt_type: typeof receipt?.type === "string" ? receipt.type : null,
        receipt_round: typeof receipt?.round === "number" ? receipt.round : null });
      return unavailable();
    }
    return { ok: true, request_id: input.request_id, workspace_id: input.workspaceId,
      round: 1, state: output.state, message_id: receipt.message_id,
      already_existed: details.already_existed === true };
  } catch (error) {
    reportDiagnostic({ failure: "ADAPTER_EXCEPTION", cwd: input.workspaceRoot, workspace_id: input.workspaceId,
      request_id: input.request_id, error_class: error instanceof Error ? error.name : "UnknownError" });
    return unavailable();
  }
}

function classifyPythonStderr(stderr: string): string | null {
  const lines = stderr.trim().split(/\r?\n/).filter(Boolean);
  const last = lines.at(-1) ?? "";
  const error = last.match(/^([A-Za-z_][A-Za-z0-9_.]*(?:Error|Exception|Exit|Interrupt))(?::|$)/);
  if (error) return error[1];
  const first = lines[0] ?? "";
  if (/^Traceback \(most recent call last\):$/.test(first)) return "Traceback without terminal exception";
  return first ? "nonempty non-traceback" : null;
}

export function classifyPythonStderrDetail(stderr: string): string | null {
  const last = stderr.trim().split(/\r?\n/).filter(Boolean).at(-1) ?? "";
  const lower = last.toLowerCase();
  if (/The same task is already active as [A-Za-z0-9_-]{8,128};/.test(last)) return "duplicate_active_request";
  if (lower.includes("cannot discover bridge")) return "bridge_workspace_identity_missing";
  if (lower.includes("c2c bridge not reachable")) return "bridge_runtime_not_found";
  const status = last.match(/\b(401|403|404|408|429|500|502|503|504) from /);
  if (status) return `bridge_http_${status[1]}`;
  if (lower.includes("non-json response from")) return "bridge_non_json_response";
  if (lower.includes("connection refused")) return "connection_refused";
  if (lower.includes("timed out")) return "connection_timeout";
  if (/^[A-Za-z_][A-Za-z0-9_.]*BridgeError:/.test(last)) return "bridge_error_other";
  return null;
}

export function safePythonStderrEvidence(stderr: string): Record<string, string> | null {
  const last = stderr.trim().split(/\r?\n/).filter(Boolean).at(-1) ?? "";
  const duplicate = last.match(/The same task is already active as ([A-Za-z0-9_-]{8,128});/);
  if (duplicate) return { python_error_code: "DUPLICATE_ACTIVE_REQUEST", active_request_id: duplicate[1] };
  const match = last.match(/C2C bridge not reachable for workspace_id=[^ ]+\s+\(tried\s+([^,]+),\s+scanned\s+(.+)\)$/i);
  if (match) return { attempted_endpoint: match[1], scanned_runtime_dir: match[2] };
  const status = last.match(/\b(401|403|404|408|429|500|502|503|504) from (https?:\/\/[^\s?]+)/);
  if (status) return { http_status: status[1], endpoint: status[2] };
  return null;
}

function safeRuntimeDiagnostic(workspaceId: string) {
  const stateDir = getStateDir();
  const noProxy = (process.env.NO_PROXY ?? process.env.no_proxy ?? "")
    .split(/[;,\s]+/).filter(Boolean).map((entry) => entry.toLowerCase());
  return {
    node_state_dir: stateDir,
    node_runtime_record_exists: fs.existsSync(path.join(stateDir, "runtime", `${workspaceId}.json`)),
    node_local_app_data: process.env.LOCALAPPDATA ?? null,
    node_config_local: process.env.C2C_CONFIG_LOCAL ?? null,
    proxy_configured: Boolean(process.env.HTTP_PROXY || process.env.HTTPS_PROXY || process.env.http_proxy || process.env.https_proxy),
    no_proxy_bypasses_loopback: noProxy.includes("127.0.0.1") || noProxy.includes("localhost"),
  };
}

function ok(data: unknown): ToolResult {
  return { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] };
}

function okStructured<T extends Record<string, unknown>>(data: T): ToolResult {
  return { ...ok(data), structuredContent: data };
}

function fail(code: string, message: string): ToolResult {
  return {
    content: [{ type: "text", text: JSON.stringify({ error: code, message }) }],
    isError: true,
  };
}

function requireScope(authInfo: AuthInfo | undefined, scope: string): ToolResult | null {
  if (!authInfo) return null; // trusted in-process
  if (!authInfo.scopes.includes(scope)) {
    return fail("INSUFFICIENT_SCOPE", `Requires '${scope}' scope.`);
  }
  return null;
}

// ---------------------------------------------------------------------------
// Input schemas
// ---------------------------------------------------------------------------

const messageTypeSchema = z.enum([
  "PLAN_REQUEST",
  "PLAN_RESPONSE",
  "EXECUTION_REPORT",
  "REVIEW_RESPONSE",
  "TASK_REQUEST",
  "PLAN_CRITIQUE",
  "PLAN_DECISION",
  "ERROR",
]);

const verdictSchema = z.enum(["APPROVED", "FIX_REQUIRED", "BLOCKED"]);

function workspaceScopedInput<T extends Record<string, z.ZodTypeAny>>(shape: T): T & { workspace_id: z.ZodOptional<z.ZodString> } {
  return {
    workspace_id: z.string().optional().describe("Registered workspace ID for this request; omitted uses the endpoint's primary workspace"),
    ...shape,
  };
}

const listInputShape = {
  request_id: z
    .string()
    .optional()
    .describe("Filter by request_id (UUID returned by c2c mailbox submit)"),
  type: messageTypeSchema
    .optional()
    .describe("Filter by message type (PLAN_REQUEST, PLAN_RESPONSE, EXECUTION_REPORT, REVIEW_RESPONSE, ERROR)"),
  min_round: z.number().int().min(1).optional().describe("Minimum round number (inclusive)"),
  limit: z
    .number()
    .int()
    .min(1)
    .max(100)
    .default(20)
    .describe("Max results (1-100, default 20)"),
};

const getInputShape = {
  message_id: z
    .string()
    .min(8)
    .describe("The message_id returned by list/submit"),
};

const submitPlanResponseInputShape = {
  request_id: z.string().min(8).describe("Request ID this PLAN_RESPONSE belongs to"),
  round: z.number().int().min(1).describe("Round number (must match the PLAN_REQUEST)"),
  analysis_summary: z
    .string()
    .min(1)
    .max(2000)
    .describe("Brief analysis of the original user request and workspace state"),
  execution_prompt: z
    .string()
    .min(1)
    .max(10000)
    .describe("Concrete, executable instructions Cursor will follow. Avoid code blocks larger than ~50 lines."),
  validation: z
    .array(z.string().min(1).max(500))
    .max(20)
    .optional()
    .default([])
    .describe("Validation checklist items Cursor must satisfy"),
  constraints: z
    .array(z.string().min(1).max(500))
    .max(20)
    .optional()
    .default([])
    .describe("Hard constraints (e.g. 'do not edit business source')"),
  review_requirements: z
    .array(z.string().min(1).max(500))
    .max(20)
    .optional()
    .default([])
    .describe("What evidence Cursor must include in EXECUTION_REPORT"),
};

const submitReviewResponseInputShape = {
  request_id: z.string().min(8).describe("Request ID"),
  round: z.number().int().min(1).describe("Round number"),
  verdict: verdictSchema.describe("APPROVED | FIX_REQUIRED | BLOCKED"),
  summary: z
    .string()
    .min(1)
    .max(2000)
    .describe("Concise review summary (1-3 sentences)"),
  findings: z
    .array(z.string().min(1).max(1000))
    .max(50)
    .optional()
    .default([])
    .describe("Specific issues found (empty when APPROVED)"),
  execution_prompt: z
    .string()
    .max(10000)
    .optional()
    .default("")
    .describe("Fix instructions (required when FIX_REQUIRED, omit when APPROVED)"),
};

const submitV2InputShape = {
  request_id: z.string().min(8),
  round: z.number().int().min(1),
  type: z.enum(["TASK_REQUEST", "PLAN_CRITIQUE", "PLAN_DECISION"]),
  payload: z.record(z.unknown()),
};

const submitErrorInputShape = {
  request_id: z.string().min(8).describe("Request ID this error belongs to"),
  round: z.number().int().min(0).default(0).describe("Round number (0 for pre-plan errors)"),
  error_code: z
    .string()
    .min(1)
    .max(50)
    .describe("Machine-readable error code (e.g. INVALID_REQUEST, TIMEOUT)"),
  error_message: z
    .string()
    .min(1)
    .max(1000)
    .describe("Human-readable error message (no secrets, no source content)"),
};

// ---------------------------------------------------------------------------
// Parsers (constructed lazily to avoid duplication)
// ---------------------------------------------------------------------------

const listInputParser = z.object(listInputShape);
const getInputParser = z.object(getInputShape);
const submitPlanResponseParser = z.object(submitPlanResponseInputShape);
const submitReviewResponseParser = z.object(submitReviewResponseInputShape);
const submitV2Parser = z.object(submitV2InputShape);
const submitErrorParser = z.object(submitErrorInputShape);

// ---------------------------------------------------------------------------
// Output schemas (Zod objects â€” required by mcp.registerTool overload)
// ---------------------------------------------------------------------------

const messageSummaryOutputSchema = z.object({
  message_id: z.string(),
  request_id: z.string(),
  type: messageTypeSchema,
  round: z.number().int(),
  workspace_id: z.string(),
  created_at: z.string(),
  expires_at: z.string(),
  payload_summary: z.record(z.unknown()),
});

const mailboxListOutputSchema = z.object({
  messages: z.array(messageSummaryOutputSchema),
  total: z.number().int().nonnegative(),
});

const mailboxGetOutputSchema = z.object({
  message_id: z.string(),
  request_id: z.string(),
  type: messageTypeSchema,
  round: z.number().int(),
  workspace_id: z.string(),
  created_at: z.string(),
  payload: z.record(z.unknown()),
});

const mailboxSubmitOutputSchema = z.object({
  message_id: z.string(),
  request_id: z.string(),
  round: z.number().int(),
  type: messageTypeSchema,
  is_duplicate: z.boolean(),
  stored: z.boolean(),
});

const mailboxReviewSubmitOutputSchema = z.object({
  message_id: z.string(),
  request_id: z.string(),
  round: z.number().int(),
  verdict: verdictSchema,
  is_duplicate: z.boolean(),
  stored: z.boolean(),
});

const mailboxErrorSubmitOutputSchema = z.object({
  message_id: z.string(),
  request_id: z.string(),
  round: z.number().int(),
  stored: z.boolean(),
});

// ---------------------------------------------------------------------------
// Tool registrations
// ---------------------------------------------------------------------------

export function registerMailboxTools(
  server: McpServer,
  workspaceId: string,
  logger: Logger,
  workspaceRoot = process.cwd(),
  dispatchBackend: DispatchBackendInvoker = invokeDispatchBackend,
  executionAgentsBackend: ExecutionAgentsBackendInvoker = invokeExecutionAgentsBackend,
  v2TaskCreationBackend: V2TaskCreationBackendInvoker = invokeV2TaskCreationBackend,
  v2LifecycleBackend: V2LifecycleBackendInvoker = invokeV2LifecycleBackend,
  v2ExecutionControlBackend: V2ExecutionControlInvoker = invokeV2ExecutionControlBackend,
  registeredRequestWorkspaceLookup?: RegisteredRequestWorkspaceLookup,
  workspaceRegistryFile?: string,
  v2ExecutionWakeBackend: V2ExecutionWakeInvoker = invokeV2ExecutionWakeBackend
): void {
  const createV2TaskInputShape = {
    request_id: z.string().min(8).max(128).regex(/^[A-Za-z0-9_-]+$/),
    task: z.string().min(1).max(20000).refine((value) => value.trim().length > 0),
    agent_id: z.string().min(1).max(128).optional(),
    requested_model: z.string().min(1).max(256).optional(),
  };
  const createV2TaskOutputSchema = z.object({
    status: z.enum(["created", "reused"]), request_id: z.string(), workspace_id: z.string(),
    round: z.number().int().min(1), state: z.string(), message_id: z.string(),
  });
  server.registerTool("create_v2_task", {
    title: "Create executable C2C V2 task",
    description: "Create an executable V2 task through Python's canonical lifecycle. This creates authoritative request state and its TASK_REQUEST without launching an agent. Use this instead of mailbox_submit_v2 for executable tasks.",
    inputSchema: workspaceScopedInput(createV2TaskInputShape),
    outputSchema: createV2TaskOutputSchema.shape,
    annotations: { readOnlyHint: false, destructiveHint: false },
  }, async (args: unknown, extra: HandlerExtra) => {
    const denied = requireScope(extra.authInfo, "mailbox.write");
    if (denied) return denied;
    const parsed = z.object(createV2TaskInputShape).safeParse(args);
    if (!parsed.success) return fail("INVALID_ARGUMENTS", "request_id and a non-empty task are required");
    try {
      const result = await v2TaskCreationBackend({ workspaceRoot, workspaceId, workspaceRegistryFile, ...parsed.data },
        (diagnostic) => logger.error("V2 task backend invocation failed", diagnostic));
      if (!result || result.ok !== true) {
        const code = typeof result?.error_code === "string" ? result.error_code : "BACKEND_UNAVAILABLE";
        return fail(code, "Executable V2 task creation failed");
      }
      if (result.request_id !== parsed.data.request_id || result.workspace_id !== workspaceId ||
          result.round !== 1 || typeof result.state !== "string" || typeof result.message_id !== "string") {
        return fail("BACKEND_UNAVAILABLE", "V2 task creation backend returned an invalid identity");
      }
      return okStructured({ status: result.already_existed === true ? "reused" : "created",
        request_id: result.request_id, workspace_id: result.workspace_id, round: 1,
        state: result.state, message_id: result.message_id });
    } catch {
      return fail("BACKEND_UNAVAILABLE", "V2 task creation backend is unavailable");
    }
  });

  const lifecycleOutputSchema = z.object({
    status: z.literal("accepted"), request_id: z.string(), workspace_id: z.string(),
    round: z.number().int().min(1), state: z.string(), operation_status: z.string().optional(),
    dispatch_status: z.string().optional(), dispatch_error_code: z.string().optional(),
    assignment_id: z.string().optional(), agent_id: z.string().optional(), worker_pid: z.number().int().optional(),
  });
  const critiqueInputShape = {
    request_id: z.string().min(8).max(128).regex(/^[A-Za-z0-9_-]+$/),
    round: z.number().int().min(1), agent_id: z.string().min(1).max(128),
    critique: z.string().min(1).max(20000), requested_model: z.string().min(1).max(256).optional(),
  };
  server.registerTool("submit_v2_plan_critique", {
    title: "Submit V2 plan critique",
    description: "Submit an assigned critic's report through the canonical Python V2 lifecycle. Validates the claim and advances authoritative request state.",
    inputSchema: workspaceScopedInput(critiqueInputShape), outputSchema: lifecycleOutputSchema.shape,
    annotations: { readOnlyHint: false, destructiveHint: false },
  }, async (args: unknown, extra: HandlerExtra) => {
    const denied = requireScope(extra.authInfo, "mailbox.write");
    if (denied) return denied;
    const parsed = z.object(critiqueInputShape).safeParse(args);
    if (!parsed.success) return fail("INVALID_ARGUMENTS", "request_id, round, agent_id and critique are required");
    try {
      const result = await v2LifecycleBackend({ operation: "critique", workspaceRoot, workspaceId, workspaceRegistryFile, ...parsed.data },
        (diagnostic) => logger.error("V2 lifecycle backend invocation failed", diagnostic));
      if (!result || result.ok !== true || result.request_id !== parsed.data.request_id ||
          result.workspace_id !== workspaceId || result.round !== parsed.data.round ||
          result.state !== "AWAITING_WEB_PLAN_DECISION") {
        return fail(typeof result?.error_code === "string" ? result.error_code : "BACKEND_UNAVAILABLE",
          "Canonical V2 critique operation failed");
      }
      return okStructured({ status: "accepted", request_id: parsed.data.request_id, workspace_id: workspaceId,
        round: parsed.data.round, state: result.state,
        ...(typeof result.status === "string" ? { operation_status: result.status } : {}) });
    } catch { return fail("BACKEND_UNAVAILABLE", "Canonical V2 critique operation is unavailable"); }
  });

  const decisionInputShape = {
    request_id: z.string().min(8).max(128).regex(/^[A-Za-z0-9_-]+$/),
    round: z.number().int().min(1), decision: z.enum(["APPROVE", "REVISE", "BLOCK"]),
    plan: z.string().max(20000).optional().default(""), agent_id: z.string().min(1).max(128).optional(),
    requested_model: z.string().min(1).max(256).optional(),
  };
  server.registerTool("decide_v2_plan", {
    title: "Decide V2 plan",
    description: "Record a Web plan decision through the canonical Python V2 lifecycle, including routing and authoritative state transition.",
    inputSchema: workspaceScopedInput(decisionInputShape), outputSchema: lifecycleOutputSchema.shape,
    annotations: { readOnlyHint: false, destructiveHint: false },
  }, async (args: unknown, extra: HandlerExtra) => {
    const denied = requireScope(extra.authInfo, "mailbox.write");
    if (denied) return denied;
    const parsed = z.object(decisionInputShape).safeParse(args);
    if (!parsed.success) return fail("INVALID_ARGUMENTS", "request_id, round and a valid decision are required");
    try {
      const result = await v2LifecycleBackend({ operation: "decision", workspaceRoot, workspaceId, workspaceRegistryFile, ...parsed.data },
        (diagnostic) => logger.error("V2 lifecycle backend invocation failed", diagnostic));
      const expectedState = parsed.data.decision === "APPROVE" ? "PLAN_APPROVED"
        : parsed.data.decision === "REVISE" ? "AWAITING_AGENT_CRITIQUE" : "BLOCKED";
      const expectedRound = parsed.data.round + (parsed.data.decision === "REVISE" ? 1 : 0);
      if (!result || result.ok !== true || result.request_id !== parsed.data.request_id ||
          result.workspace_id !== workspaceId || result.round !== expectedRound || result.state !== expectedState) {
        return fail(typeof result?.error_code === "string" ? result.error_code : "BACKEND_UNAVAILABLE",
          "Canonical V2 plan decision failed");
      }
      let dispatchOutcome: Record<string, unknown> | undefined;
      if (parsed.data.decision === "REVISE") {
        try {
          const dispatched = dispatchBackend({ workspaceRoot, workspaceId, workspaceRegistryFile,
            request_id: parsed.data.request_id, round: expectedRound });
          dispatchOutcome = (dispatched && typeof dispatched.status === "string" &&
              dispatched.request_id === parsed.data.request_id &&
              dispatched.workspace_id === workspaceId && dispatched.round === expectedRound)
            ? dispatched
            : { status: "rejected", error_code: "BACKEND_UNAVAILABLE" };
        } catch {
          dispatchOutcome = { status: "rejected", error_code: "BACKEND_UNAVAILABLE" };
        }
      }
      return okStructured({ status: "accepted", request_id: parsed.data.request_id, workspace_id: workspaceId,
        round: expectedRound, state: result.state,
        ...(typeof result.status === "string" ? { operation_status: result.status } : {}),
        ...(dispatchOutcome ? {
          dispatch_status: String(dispatchOutcome.status),
          ...(typeof dispatchOutcome.error_code === "string" ? { dispatch_error_code: dispatchOutcome.error_code } : {}),
          ...(typeof dispatchOutcome.assignment_id === "string" ? { assignment_id: dispatchOutcome.assignment_id } : {}),
          ...(typeof dispatchOutcome.agent_id === "string" ? { agent_id: dispatchOutcome.agent_id } : {}),
          ...(typeof dispatchOutcome.worker_pid === "number" ? { worker_pid: dispatchOutcome.worker_pid } : {}),
        } : {}) });
    } catch { return fail("BACKEND_UNAVAILABLE", "Canonical V2 plan decision is unavailable"); }
  });

  const dispatchInputShape = {
    request_id: z.string().min(8).max(128).regex(/^[A-Za-z0-9_-]+$/),
    round: z.number().int().min(1),
  };
  const dispatchOutputSchema = z.object({
    status: z.string().regex(/^[a-z][a-z0-9_]{0,63}$/),
    request_id: z.string(), workspace_id: z.string(), round: z.number().int(),
    error_code: z.string().optional(), message: z.string().optional(), assignment_id: z.string().optional(),
    agent_id: z.string().optional(), worker_pid: z.number().int().optional(),
  });
  server.registerTool("dispatch_execution", {
    title: "Dispatch C2C execution",
    description: "Start or reuse the existing asynchronous V2 execution for one exact request and round.",
    inputSchema: workspaceScopedInput(dispatchInputShape),
    outputSchema: dispatchOutputSchema.shape,
    annotations: { readOnlyHint: false, destructiveHint: false },
  }, async (args: unknown, extra: HandlerExtra) => {
    const denied = requireScope(extra.authInfo, "mailbox.write");
    if (denied) return denied;
    const parsed = z.object(dispatchInputShape).safeParse(args);
    if (!parsed.success) return fail("INVALID_ARGUMENTS", "request_id and positive integer round are required");
    try {
      const result = dispatchBackend({ workspaceRoot, workspaceId, workspaceRegistryFile, ...parsed.data });
      if (!result || typeof result.status !== "string" || !/^[a-z][a-z0-9_]{0,63}$/.test(result.status) ||
          result.request_id !== parsed.data.request_id || result.workspace_id !== workspaceId ||
          result.round !== parsed.data.round) {
        return okStructured({ status: "rejected", request_id: parsed.data.request_id,
          workspace_id: workspaceId, round: parsed.data.round, error_code: "BACKEND_UNAVAILABLE",
          message: "Dispatch backend is unavailable" });
      }
      return okStructured(result);
    } catch {
      return okStructured({ status: "rejected", request_id: parsed.data.request_id,
        workspace_id: workspaceId, round: parsed.data.round, error_code: "BACKEND_UNAVAILABLE",
        message: "Dispatch backend is unavailable" });
    }
  });

  const executionControlTools: { name: string; operation: V2ExecutionControlInput["operation"]; shape: z.ZodRawShape }[] = [
    { name: "review_v2_execution", operation: "review" as const,
      shape: { verdict: verdictSchema, summary: z.string().min(1).max(20000).refine((v) => v.trim().length > 0),
        agent_id: z.string().min(1).max(128).optional(), requested_model: z.string().min(1).max(256).optional() } },
    { name: "reroute_v2_execution", operation: "reroute" as const,
      shape: { agent_id: z.string().min(1).max(128), requested_model: z.string().min(1).max(256).optional() } },
  ];
  for (const tool of executionControlTools) {
    const inputShape: z.ZodRawShape = { request_id: z.string().min(8).max(128).regex(/^[A-Za-z0-9_-]+$/),
      round: z.number().int().min(1), ...tool.shape };
    server.registerTool(tool.name, { title: tool.name.replaceAll("_", " "),
      description: `Delegate ${tool.name.replaceAll("_", " ")} to Python's canonical execution lifecycle.`,
      inputSchema: workspaceScopedInput(inputShape), outputSchema: lifecycleOutputSchema.shape,
      annotations: { readOnlyHint: false, destructiveHint: false } }, async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.write");
      if (denied) return denied;
      const parsed = z.object(inputShape).safeParse(args);
      if (!parsed.success) return fail("INVALID_ARGUMENTS", "request_id, round, and operation fields are required");
      const data = parsed.data as Omit<V2ExecutionControlInput, "workspaceRoot" | "workspaceId" | "operation">;
      // FIX_REQUIRED may omit agent_id. Python canonical review() resolves the
      // fixer from persisted review routing or the previous execute/fix assignment.
      // Do not duplicate that lifecycle policy in the Node adapter.
      // Resolve only against roots admitted by the bridge's canonical workspace
      // registry. A miss remains the backend's REQUEST_NOT_FOUND; an exact
      // request identity owned by another registered root is a workspace error.
      if (registeredRequestWorkspaceLookup) {
        try {
          const owner = registeredRequestWorkspaceLookup(data.request_id, workspaceId);
          if (owner !== null && owner !== workspaceId) {
            return fail("WORKSPACE_MISMATCH", "Request belongs to another registered workspace");
          }
        } catch {
          return fail("BACKEND_UNAVAILABLE", "Registered workspace request lookup failed");
        }
      }
      try {
        const result = await v2ExecutionControlBackend({ workspaceRoot, workspaceId, workspaceRegistryFile, operation: tool.operation, ...data });
        if (!result || result.ok !== true || result.request_id !== data.request_id ||
            result.workspace_id !== workspaceId || !validExecutionControlState({ workspaceRoot, workspaceId, operation: tool.operation, ...data }, result)) {
          return fail(safeExecutionControlError(result?.error_code), "Canonical execution operation failed");
        }
        let dispatchOutcome: Record<string, unknown> | undefined;
        if (tool.operation === "review" && data.verdict === "FIX_REQUIRED") {
          const nextRound = Number(result.round);
          try {
            const dispatched = dispatchBackend({ workspaceRoot, workspaceId, workspaceRegistryFile,
              request_id: data.request_id, round: nextRound });
            dispatchOutcome = (dispatched && typeof dispatched.status === "string" &&
                dispatched.request_id === data.request_id &&
                dispatched.workspace_id === workspaceId && dispatched.round === nextRound)
              ? dispatched
              : { status: "rejected", error_code: "BACKEND_UNAVAILABLE" };
          } catch {
            dispatchOutcome = { status: "rejected", error_code: "BACKEND_UNAVAILABLE" };
          }
        }
        return okStructured({ status: "accepted", request_id: data.request_id, workspace_id: workspaceId,
          round: result.round as number, state: result.state as string,
          ...(typeof result.status === "string" ? { operation_status: result.status } : {}),
          ...(dispatchOutcome ? {
            dispatch_status: String(dispatchOutcome.status),
            ...(typeof dispatchOutcome.error_code === "string" ? { dispatch_error_code: dispatchOutcome.error_code } : {}),
            ...(typeof dispatchOutcome.assignment_id === "string" ? { assignment_id: dispatchOutcome.assignment_id } : {}),
            ...(typeof dispatchOutcome.agent_id === "string" ? { agent_id: dispatchOutcome.agent_id } : {}),
            ...(typeof dispatchOutcome.worker_pid === "number" ? { worker_pid: dispatchOutcome.worker_pid } : {}),
          } : {}) });
      } catch { return fail("BACKEND_UNAVAILABLE", "Canonical execution operation is unavailable"); }
    });
  }

  const wakeInputShape = { request_id: z.string().min(8).max(128).regex(/^[A-Za-z0-9_-]+$/),
    round: z.number().int().min(1) };
  server.registerTool("recover_v2_execution_wake", {
    title: "Recover V2 execution wake",
    description: "Recover one exact V2 execution wake through Python's canonical lifecycle CLI.",
    inputSchema: workspaceScopedInput(wakeInputShape),
    outputSchema: z.object({ status: z.string().optional(), request_id: z.string(), workspace_id: z.string(),
      round: z.number().int(), state: z.string(), delivery: z.unknown().optional(), error_code: z.string().optional() }).shape,
    annotations: { readOnlyHint: false, destructiveHint: false },
  }, async (args: unknown, extra: HandlerExtra) => {
    const denied = requireScope(extra.authInfo, "mailbox.write");
    if (denied) return denied;
    const parsed = z.object({ ...wakeInputShape, workspace_id: z.string().optional() }).safeParse(args);
    if (!parsed.success) return fail("INVALID_ARGUMENTS", "request_id and positive integer round are required");
    if (parsed.data.workspace_id !== undefined && parsed.data.workspace_id !== workspaceId)
      return fail("WORKSPACE_MISMATCH", "workspace_id does not match the selected workspace");
    const { workspace_id: _requestedWorkspaceId, ...data } = parsed.data;
    if (registeredRequestWorkspaceLookup) {
      try {
        const owner = registeredRequestWorkspaceLookup(data.request_id, workspaceId);
        if (owner !== null && owner !== workspaceId) return fail("WORKSPACE_MISMATCH", "Request belongs to another registered workspace");
      } catch { return fail("BACKEND_UNAVAILABLE", "Registered workspace request lookup failed"); }
    }
    try {
      const result = await v2ExecutionWakeBackend({ workspaceRoot, workspaceId, workspaceRegistryFile, ...data });
      if (!result || result.ok !== true) return fail(safeWakeError(result?.error_code), "Canonical V2 wake recovery failed");
      if (result.request_id !== data.request_id || result.workspace_id !== workspaceId || result.round !== data.round ||
          typeof result.state !== "string") return fail("BACKEND_UNAVAILABLE", "Canonical V2 wake returned invalid correlation");
      const { ok: _ok, ...canonical } = result;
      return okStructured(canonical);
    } catch { return fail("BACKEND_UNAVAILABLE", "Canonical V2 wake recovery is unavailable"); }
  });

  server.registerTool("list_execution_agents", {
    title: "List execution agents",
    description: "Read-only discovery of installed execution runtime capabilities for this workspace.",
      inputSchema: workspaceScopedInput({}),
    annotations: { readOnlyHint: true, destructiveHint: false },
  }, async (_args: unknown, extra: HandlerExtra) => {
    const denied = requireScope(extra.authInfo, "mailbox.read");
    if (denied) return denied;
    try {
      const result = executionAgentsBackend({ workspaceRoot, workspaceId, workspaceRegistryFile });
      if (!validExecutionAgentsResult(result, workspaceId)) {
        return fail("BACKEND_UNAVAILABLE", "Execution capability backend is unavailable");
      }
      return okStructured(result);
    } catch {
      return fail("BACKEND_UNAVAILABLE", "Execution capability backend is unavailable");
    }
  });

  // ---- mailbox_list ---------------------------------------------------
  server.registerTool(
    "mailbox_list",
    {
      title: "Mailbox â€” list messages",
      description:
        `List pending control-plane messages in the workspace mailbox. ` +
        `Use this to find PLAN_REQUESTs waiting for a PLAN_RESPONSE, or ` +
        `EXECUTION_REPORTs waiting for a REVIEW_RESPONSE. ` +
        `CONTROL-PLANE ONLY: this does not read source files or execute commands.`,
      inputSchema: workspaceScopedInput(listInputShape),
      outputSchema: mailboxListOutputSchema.shape,
      annotations: { readOnlyHint: true },
    },
    async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.read");
      if (denied) return denied;

      const parsed = listInputParser.safeParse(args);
      if (!parsed.success) {
        return fail("INVALID_ARGUMENTS", parsed.error.message);
      }

      try {
        const store = getMailboxStore(workspaceId);
        const result = store.list(workspaceId, parsed.data);

        const stripped = result.messages.map((m) => ({
          message_id: m.message_id,
          request_id: m.request_id,
          type: m.type,
          round: m.round,
          workspace_id: m.workspace_id,
          created_at: m.created_at,
          expires_at: m.expires_at,
          payload_summary: m.payload as Record<string, unknown>,
        }));

        return okStructured({ messages: stripped, total: result.total });
      } catch (err) {
        logger.error("mailbox_list failed", { error: (err as Error).message });
        return fail("INTERNAL_ERROR", (err as Error).message);
      }
    }
  );

  // ---- mailbox_get ---------------------------------------------------
  server.registerTool(
    "mailbox_get",
    {
      title: "Mailbox â€” get message",
      description:
        `Retrieve the full payload of a specific mailbox message by message_id. ` +
        `CONTROL-PLANE ONLY.`,
      inputSchema: workspaceScopedInput(getInputShape),
      outputSchema: mailboxGetOutputSchema.shape,
      annotations: { readOnlyHint: true },
    },
    async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.read");
      if (denied) return denied;

      const parsed = getInputParser.safeParse(args);
      if (!parsed.success) {
        return fail("INVALID_ARGUMENTS", parsed.error.message);
      }

      try {
        const store = getMailboxStore(workspaceId);
        const msg = store.get(workspaceId, parsed.data.message_id);
        if (!msg) {
          return fail("NOT_FOUND", `Message '${parsed.data.message_id}' not found or expired`);
        }
        return okStructured({
          message_id: msg.message_id,
          request_id: msg.request_id,
          type: msg.type,
          round: msg.round,
          workspace_id: msg.workspace_id,
          created_at: msg.created_at,
          payload: msg.payload as Record<string, unknown>,
        });
      } catch (err) {
        logger.error("mailbox_get failed", { error: (err as Error).message });
        return fail("INTERNAL_ERROR", (err as Error).message);
      }
    }
  );

  // ---- mailbox_submit_plan_response -----------------------------------
  server.registerTool(
    "mailbox_submit_plan_response",
    {
      title: "Mailbox â€” submit PLAN_RESPONSE",
      description:
        `Submit a PLAN_RESPONSE for a PLAN_REQUEST. ` +
        `Call this AFTER inspecting the workspace via the existing read-only MCP tools. ` +
        `CONTROL-PLANE ONLY: no source files are written by this tool.`,
      inputSchema: workspaceScopedInput(submitPlanResponseInputShape),
      outputSchema: mailboxSubmitOutputSchema.shape,
      annotations: { readOnlyHint: false, destructiveHint: false },
    },
    async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.write");
      if (denied) return denied;

      const parsed = submitPlanResponseParser.safeParse(args);
      if (!parsed.success) {
        return fail("INVALID_ARGUMENTS", parsed.error.message);
      }
      const data = parsed.data;

      try {
        const store = getMailboxStore(workspaceId);
        const result = store.submit(workspaceId, data.request_id, "PLAN_RESPONSE", data.round, {
          analysis_summary: data.analysis_summary,
          execution_prompt: data.execution_prompt,
          validation: data.validation,
          constraints: data.constraints,
          review_requirements: data.review_requirements,
        });
        if (!result.ok) {
          const code = result.error.toUpperCase();
          return fail(code, result.message);
        }
        return okStructured({
          message_id: result.message.message_id,
          request_id: result.message.request_id,
          round: result.message.round,
          type: result.message.type,
          is_duplicate: result.is_duplicate,
          stored: true,
        });
      } catch (err) {
        logger.error("mailbox_submit_plan_response failed", { error: (err as Error).message });
        return fail("INTERNAL_ERROR", (err as Error).message);
      }
    }
  );

  // ---- mailbox_submit_review_response ---------------------------------
  server.registerTool(
    "mailbox_submit_review_response",
    {
      title: "Mailbox â€” submit REVIEW_RESPONSE",
      description:
        `Submit a REVIEW_RESPONSE for an EXECUTION_REPORT. ` +
        `Call this AFTER inspecting the execution report and git diff via other MCP tools. ` +
        `CONTROL-PLANE ONLY.`,
      inputSchema: workspaceScopedInput(submitReviewResponseInputShape),
      outputSchema: mailboxReviewSubmitOutputSchema.shape,
      annotations: { readOnlyHint: false, destructiveHint: false },
    },
    async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.write");
      if (denied) return denied;

      const parsed = submitReviewResponseParser.safeParse(args);
      if (!parsed.success) {
        return fail("INVALID_ARGUMENTS", parsed.error.message);
      }
      const data = parsed.data;

      try {
        const store = getMailboxStore(workspaceId);
        const result = store.submit(
          workspaceId,
          data.request_id,
          "REVIEW_RESPONSE",
          data.round,
          {
            verdict: data.verdict,
            summary: data.summary,
            findings: data.findings,
            execution_prompt: data.execution_prompt,
          }
        );
        if (!result.ok) {
          const code = result.error.toUpperCase();
          return fail(code, result.message);
        }
        return okStructured({
          message_id: result.message.message_id,
          request_id: result.message.request_id,
          round: result.message.round,
          verdict: data.verdict,
          is_duplicate: result.is_duplicate,
          stored: true,
        });
      } catch (err) {
        logger.error("mailbox_submit_review_response failed", { error: (err as Error).message });
        return fail("INTERNAL_ERROR", (err as Error).message);
      }
    }
  );

  // ---- mailbox_submit_v2 ----------------------------------------------
  server.registerTool(
    "mailbox_submit_v2",
    {
      title: "Mailbox â€” submit V2 control message",
      description: "Submit TASK_REQUEST, PLAN_CRITIQUE, or PLAN_DECISION. V1 tools remain unchanged.",
      inputSchema: workspaceScopedInput(submitV2InputShape),
      outputSchema: mailboxSubmitOutputSchema.shape,
      annotations: { readOnlyHint: false, destructiveHint: false },
    },
    async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.write");
      if (denied) return denied;
      const parsed = submitV2Parser.safeParse(args);
      if (!parsed.success) return fail("INVALID_ARGUMENTS", parsed.error.message);
      try {
        const data = parsed.data;
        const result = getMailboxStore(workspaceId).submit(workspaceId, data.request_id, data.type, data.round, data.payload);
        if (!result.ok) return fail(result.error.toUpperCase(), result.message);
        return okStructured({ message_id: result.message.message_id, request_id: result.message.request_id,
          round: result.message.round, type: result.message.type, is_duplicate: result.is_duplicate, stored: true });
      } catch (err) {
        logger.error("mailbox_submit_v2 failed", { error: (err as Error).message });
        return fail("INTERNAL_ERROR", (err as Error).message);
      }
    }
  );

  // ---- mailbox_submit_error -------------------------------------------
  server.registerTool(
    "mailbox_submit_error",
    {
      title: "Mailbox â€” submit ERROR",
      description:
        `Submit an ERROR message when the workflow cannot proceed. ` +
        `CONTROL-PLANE ONLY.`,
      inputSchema: workspaceScopedInput(submitErrorInputShape),
      outputSchema: mailboxErrorSubmitOutputSchema.shape,
      annotations: { readOnlyHint: false, destructiveHint: false },
    },
    async (args: unknown, extra: HandlerExtra) => {
      const denied = requireScope(extra.authInfo, "mailbox.write");
      if (denied) return denied;

      const parsed = submitErrorParser.safeParse(args);
      if (!parsed.success) {
        return fail("INVALID_ARGUMENTS", parsed.error.message);
      }
      const data = parsed.data;

      try {
        const store = getMailboxStore(workspaceId);
        const result = store.submit(workspaceId, data.request_id, "ERROR", data.round, {
          error_code: data.error_code,
          error_message: data.error_message,
        });
        if (!result.ok) {
          const code = result.error.toUpperCase();
          return fail(code, result.message);
        }
        return okStructured({
          message_id: result.message.message_id,
          request_id: result.message.request_id,
          round: result.message.round,
          stored: true,
        });
      } catch (err) {
        logger.error("mailbox_submit_error failed", { error: (err as Error).message });
        return fail("INTERNAL_ERROR", (err as Error).message);
      }
    }
  );
}
