import { describe, it, expect, beforeAll, afterAll } from "vitest";
import fs from "node:fs";
import path from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { startBridge, type Bridge } from "../src/bridge/server.js";
import type { V2ExecutionControlInput, V2ExecutionWakeInput, V2LifecycleBackendInput, V2TaskCreationBackendInput } from "../src/mcp/mailbox-tools.js";
import { appendExecutionRecord } from "../src/execution/records.js";
import { saveExecutionOutput } from "../src/execution/output.js";
import { makeTmpDir, cleanup, write, makeGitRepo, git, isolateStateDir } from "./helpers.js";

let root: string;
let bridge: Bridge;
let client: Client;
let accessToken: string;
let stateDir: string;
const dispatchCalls: { workspaceRoot: string; workspaceId: string; request_id: string; round: number }[] = [];
let dispatchResult: Record<string, unknown> = {};
let dispatchFailure = false;
const executionAgentsCalls: { workspaceRoot: string; workspaceId: string }[] = [];
let executionAgentsResult: Record<string, unknown> = {};
let executionAgentsFailure = false;
const v2TaskCalls: V2TaskCreationBackendInput[] = [];
let v2TaskResult: Record<string, unknown> = {};
let v2TaskFailure = false;
const v2LifecycleCalls: V2LifecycleBackendInput[] = [];
let v2LifecycleResult: Record<string, unknown> = {};
let v2LifecycleFailure = false;
const v2ExecutionControlCalls: V2ExecutionControlInput[] = [];
let executionControlOverride: Record<string, unknown> | undefined;
const v2WakeCalls: V2ExecutionWakeInput[] = [];
let v2WakeOverride: Record<string, unknown> | undefined;

function textOf(result: { content?: unknown }): string {
  const content = result.content as { type: string; text: string }[];
  return content?.[0]?.text ?? "";
}

function jsonOf<T = Record<string, unknown>>(result: { content?: unknown }): T {
  return JSON.parse(textOf(result)) as T;
}

function structuredJsonOf<T = Record<string, unknown>>(result: { content?: unknown; structuredContent?: unknown }): T {
  const parsed = jsonOf<T>(result);
  expect(result.structuredContent).toEqual(parsed);
  return parsed;
}

async function adminJson(pathname: string, init: RequestInit = {}): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`${bridge.localBaseUrl()}${pathname}`, {
    ...init,
    headers: {
      authorization: `Bearer ${bridge.adminToken}`,
      ...(init.body ? { "content-type": "application/json" } : {}),
      ...init.headers,
    },
  });
  return { status: response.status, body: await response.json() as Record<string, unknown> };
}

function expectToolOutputSchema(
  tools: Awaited<ReturnType<Client["listTools"]>>["tools"],
  name: string,
  properties: string[]
): void {
  const schema = tools.find((tool) => tool.name === name)?.outputSchema as
    | { type?: string; properties?: Record<string, unknown> }
    | undefined;
  expect(schema?.type).toBe("object");
  expect(Object.keys(schema?.properties ?? {})).toEqual(expect.arrayContaining(properties));
}

beforeAll(async () => {
  stateDir = isolateStateDir();
  root = makeTmpDir("mcp-ws");
  makeGitRepo(root);
  write(root, "package.json", JSON.stringify({ name: "demo", scripts: { test: "vitest run" }, dependencies: { react: "^19.0.0" } }));
  write(root, ".env", "API_KEY=supersecret\n");
  fs.writeFileSync(path.join(root, "pixel.png"), Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3]));
  // an uncommitted change so git_diff has content
  write(root, "src/index.ts", "export const answer = 43; // changed\n");

  bridge = await startBridge({
    workspaceRoot: root,
    port: 0,
    persistRuntime: false,
    authStoreFile: path.join(makeTmpDir("auth"), "store.json"),
    dispatchBackend: (input) => { dispatchCalls.push(input); if (dispatchFailure) throw new Error("private subprocess detail"); return dispatchResult; },
    executionAgentsBackend: (input) => { executionAgentsCalls.push(input); if (executionAgentsFailure) throw new Error("private probe detail"); return executionAgentsResult; },
    v2TaskCreationBackend: (input) => { v2TaskCalls.push(input); if (v2TaskFailure) throw new Error("private task detail"); return v2TaskResult; },
    v2LifecycleBackend: (input) => { v2LifecycleCalls.push(input); if (v2LifecycleFailure) throw new Error("private lifecycle detail"); return v2LifecycleResult; },
    v2ExecutionControlBackend: (input) => { v2ExecutionControlCalls.push(input); if (executionControlOverride) return executionControlOverride; return { ok: true,
      request_id: input.request_id, workspace_id: input.workspaceId, round: input.round + (input.operation === "review" && input.verdict === "FIX_REQUIRED" ? 1 : 0),
      state: input.operation === "review" ? ({ APPROVED: "DONE", BLOCKED: "BLOCKED", FIX_REQUIRED: "EXECUTING_FIX" }[input.verdict!]) : "EXECUTING_LOCAL", status: "OK" }; },
    v2ExecutionWakeBackend: (input) => { v2WakeCalls.push(input); return v2WakeOverride ?? { ok: true,
      status: "recovered", request_id: input.request_id, workspace_id: input.workspaceId, round: input.round,
      state: "REQUESTING_REVIEW", delivery: { status: "sent" } }; },
  });
  const tokens = bridge.authStore.issueTokens({
    clientId: "it-client",
    scopes: [
      "workspace.read",
      "workspace.search",
      "git.read",
      "execution.read",
      "mailbox.read",
      "mailbox.write",
    ],
  });
  accessToken = tokens.accessToken;

  client = new Client({ name: "c2c-test-client", version: "1.0.0" });
  const transport = new StreamableHTTPClientTransport(new URL(`${bridge.localBaseUrl()}/mcp`), {
    requestInit: { headers: { authorization: `Bearer ${accessToken}` } },
  });
  await client.connect(transport);
});

afterAll(async () => {
  await client.close();
  await bridge.close();
  cleanup(root);
});

describe("MCP tools over Streamable HTTP", () => {
  it("lists V2 execution tools and exposes safe canonical lifecycle schemas", async () => {
    const { tools } = await client.listTools();
    const names = tools.map((tool) => tool.name).sort();
    expect(names).toEqual([
      "create_v2_task",
      "decide_v2_plan",
      "dispatch_execution",
      "execution_output",
      "reroute_v2_execution",
      "recover_v2_execution_wake",
      "review_v2_execution",
      "execution_summary",
      "git_diff",
      "git_status",
      "list_directory",
      "list_execution_agents",
      "mailbox_get",
      "mailbox_list",
      "mailbox_submit_error",
      "mailbox_submit_plan_response",
      "mailbox_submit_review_response",
      "mailbox_submit_v2",
      "read_file",
      "read_image",
      "search_workspace",
      "submit_v2_plan_critique",
      "test_status",
      "workspace_info",
    ].sort());
    const wakeTools = tools.filter((tool) => tool.name === "recover_v2_execution_wake");
    expect(wakeTools).toHaveLength(1);
    const wakeSchema = wakeTools[0]?.inputSchema as { required?: string[]; properties?: Record<string, unknown> };
    expect(wakeSchema.required).toEqual(expect.arrayContaining(["request_id", "round"]));
    expect(wakeSchema.required).not.toContain("workspace_id");
    expect(Object.keys(wakeSchema.properties ?? {})).toEqual(expect.arrayContaining(["workspace_id"]));
    // Truly forbidden tools (would write source / execute shell / alter DB)
    for (const forbidden of ["write_file", "delete_file", "execute_shell", "git_commit", "install_package"]) {
      expect(names).not.toContain(forbidden);
    }

    expectToolOutputSchema(tools, "workspace_info", ["workspaceId", "workspaceName", "projectType", "git"]);
    expectToolOutputSchema(tools, "list_directory", ["path", "entries", "total", "hasMore"]);
    expectToolOutputSchema(tools, "read_file", ["path", "content", "startLine", "endLine", "nextStartLine"]);
    expectToolOutputSchema(tools, "read_image", ["path", "sizeBytes", "mimeType"]);
    expectToolOutputSchema(tools, "search_workspace", ["matches", "matchCount", "truncated", "engine"]);
    expectToolOutputSchema(tools, "git_status", ["isRepo", "branch", "staged", "unstaged", "untracked", "hidden"]);
    expectToolOutputSchema(tools, "git_diff", ["isRepo", "mode", "diff", "hasMore", "nextOffset"]);
    expectToolOutputSchema(tools, "test_status", ["available", "tests", "outputAvailable", "outputId"]);
    expectToolOutputSchema(tools, "execution_summary", ["records"]);
    expectToolOutputSchema(tools, "execution_output", ["action", "items", "text"]);
    expectToolOutputSchema(tools, "mailbox_list", ["messages", "total"]);
    expectToolOutputSchema(tools, "create_v2_task", ["status", "request_id", "workspace_id", "round", "state", "message_id"]);
    const createTask = tools.find((tool) => tool.name === "create_v2_task");
    expect(createTask?.inputSchema.properties).not.toHaveProperty("chat_url");
    expect(createTask?.inputSchema.required).toEqual(expect.arrayContaining(["request_id", "task"]));
    expect(wakeTools[0]?.inputSchema.properties).not.toHaveProperty("chat_url");
    const review = tools.find((tool) => tool.name === "review_v2_execution");
    expect(review?.inputSchema.required).toEqual(expect.arrayContaining(["verdict", "summary"]));
    expect(review?.inputSchema.properties).toHaveProperty("agent_id");
    expect(review?.inputSchema.properties).toHaveProperty("requested_model");
    expect(review?.inputSchema.properties).not.toHaveProperty("feedback");
    for (const tool of [createTask, ...tools.filter((item) => ["review_v2_execution", "reroute_v2_execution"].includes(item.name))]) {
      expect(tool?.inputSchema.properties).not.toHaveProperty("workspace_path");
      expect(tool?.inputSchema.properties).not.toHaveProperty("root");
      expect(tool?.inputSchema.properties).not.toHaveProperty("path");
    }
    expectToolOutputSchema(tools, "submit_v2_plan_critique", ["status", "request_id", "workspace_id", "round", "state"]);
    expectToolOutputSchema(tools, "decide_v2_plan", ["status", "request_id", "workspace_id", "round", "state"]);
    expectToolOutputSchema(tools, "mailbox_get", ["message_id", "request_id", "type", "round", "payload"]);
    const dispatch = tools.find((tool) => tool.name === "dispatch_execution");
    expect(dispatch?.inputSchema.required).toEqual(expect.arrayContaining(["request_id", "round"]));
    expect(dispatch?.inputSchema.properties).toMatchObject({ request_id: { type: "string" }, round: { type: "integer", minimum: 1 } });
    const discovery = tools.find((tool) => tool.name === "list_execution_agents");
    expect(discovery?.inputSchema.properties).toHaveProperty("workspace_id");
    expect(discovery?.inputSchema.required ?? []).toEqual([]);
  });

  it("forwards canonical wake result and rejects workspace/correlation/domain errors safely", async () => {
    const input = { request_id: "c2c_wake_contract", round: 4 };
    const success = structuredJsonOf<Record<string, unknown>>(await client.callTool({ name: "recover_v2_execution_wake", arguments: input }));
    expect(success).toEqual({ status: "recovered", request_id: input.request_id, workspace_id: bridge.workspace.id,
      round: 4, state: "REQUESTING_REVIEW", delivery: { status: "sent" } });
    expect(v2WakeCalls.at(-1)).toMatchObject({ workspaceRoot: root, workspaceId: bridge.workspace.id, ...input });
    const callCount = v2WakeCalls.length;
    await expect(client.callTool({ name: "recover_v2_execution_wake", arguments: { ...input, workspace_id: "wrong" } }))
      .rejects.toThrow(/unknown_workspace_id/i);
    expect(v2WakeCalls).toHaveLength(callCount);
    v2WakeOverride = { ok: true, request_id: "wrong_request", workspace_id: bridge.workspace.id, round: 4, state: "REQUESTING_REVIEW" };
    const requestMismatch = await client.callTool({ name: "recover_v2_execution_wake", arguments: input });
    expect(textOf(requestMismatch)).toContain("BACKEND_UNAVAILABLE");
    v2WakeOverride = { ok: true, request_id: input.request_id, workspace_id: bridge.workspace.id, round: 5, state: "REQUESTING_REVIEW" };
    const roundMismatch = await client.callTool({ name: "recover_v2_execution_wake", arguments: input });
    expect(textOf(roundMismatch)).toContain("BACKEND_UNAVAILABLE");
    v2WakeOverride = { ok: false, error_code: "REPORT_NOT_DURABLE" };
    const domain = await client.callTool({ name: "recover_v2_execution_wake", arguments: input });
    expect(textOf(domain)).toContain("REPORT_NOT_DURABLE");
    v2WakeOverride = { ok: false, error_code: "PRIVATE_DETAIL" };
    const unknown = await client.callTool({ name: "recover_v2_execution_wake", arguments: input });
    expect(textOf(unknown)).toContain("BACKEND_UNAVAILABLE");
    v2WakeOverride = undefined;
    const unknownTool = await client.callTool({ name: "unknown_tool_name", arguments: {} });
    expect(unknownTool.isError).toBe(true);
  });

  it("creates tasks without chat_url and delegates review and reroute", async () => {
    v2TaskCalls.length = 0;
    v2TaskResult = { ok: true, request_id: "c2c_no_chat_url_test", workspace_id: bridge.workspace.id,
      round: 1, state: "AWAITING_AGENT_CRITIQUE", message_id: "msg-1" };
    const created = await client.callTool({ name: "create_v2_task", arguments: {
      request_id: "c2c_no_chat_url_test", task: "task" } });
    expect(created.isError).not.toBe(true);
    expect(v2TaskCalls[0]).toMatchObject({ request_id: "c2c_no_chat_url_test", task: "task" });
    const request_id = "c2c_exec_control_test";
    const base = { request_id, round: 2 };
    for (const [name, arguments_] of [
      ["review_v2_execution", { ...base, verdict: "APPROVED", summary: "looks good", agent_id: "codex-local", requested_model: "gpt-6-sol" }],
      ["reroute_v2_execution", { ...base, agent_id: "codex-local", requested_model: "gpt-6-sol" }],
    ] as const) {
      const result = await client.callTool({ name, arguments: arguments_ });
      expect(result.isError).not.toBe(true);
    }
    expect(v2ExecutionControlCalls.map((call) => call.operation)).toEqual(["review", "reroute"]);
    expect(v2ExecutionControlCalls[0]).toMatchObject({ ...base, verdict: "APPROVED", summary: "looks good", agent_id: "codex-local", requested_model: "gpt-6-sol" });
    expect(v2ExecutionControlCalls[1]).toMatchObject({ ...base, agent_id: "codex-local", requested_model: "gpt-6-sol" });
    expect(v2ExecutionControlCalls.every((call) => call.workspaceRoot === root && call.workspaceId === bridge.workspace.id)).toBe(true);
  });

  it("accepts FIX_REQUIRED without explicit fixer, wakes next round, and retries idempotently", async () => {
    dispatchCalls.length = 0;
    const args = { request_id: "c2c_fix_round_test", round: 1, verdict: "FIX_REQUIRED", summary: "fix" };
    dispatchResult = { status: "accepted", request_id: args.request_id, workspace_id: bridge.workspace.id,
      round: 2, assignment_id: `${args.request_id}:2:fix`, agent_id: "codex-local", worker_pid: 12345 };
    const result = await client.callTool({ name: "review_v2_execution", arguments: args });
    expect(result.isError).not.toBe(true);
    expect(jsonOf(result)).toMatchObject({ round: 2, state: "EXECUTING_FIX",
      dispatch_status: "accepted", assignment_id: `${args.request_id}:2:fix`, worker_pid: 12345 });
    expect(v2ExecutionControlCalls.at(-1)).toMatchObject({ ...args, operation: "review" });
    expect(v2ExecutionControlCalls.at(-1)).not.toHaveProperty("agent_id");
    expect(dispatchCalls.at(-1)).toEqual({ workspaceRoot: root, workspaceId: bridge.workspace.id,
      workspaceRegistryFile: undefined, request_id: args.request_id, round: 2 });

    dispatchResult = { status: "already_running", request_id: args.request_id,
      workspace_id: bridge.workspace.id, round: 2, assignment_id: `${args.request_id}:2:fix`, agent_id: "codex-local" };
    const retry = await client.callTool({ name: "review_v2_execution", arguments: args });
    expect(retry.isError).not.toBe(true);
    expect(jsonOf(retry)).toMatchObject({ round: 2, state: "EXECUTING_FIX",
      dispatch_status: "already_running" });

    executionControlOverride = { ok: false, error_code: "SECRET_PRIVATE_BACKEND_DETAIL" };
    try {
      const failed = await client.callTool({ name: "review_v2_execution", arguments: args });
      expect(failed.isError).toBe(true);
      expect(textOf(failed)).not.toContain("SECRET_PRIVATE_BACKEND_DETAIL");
      expect(textOf(failed)).toContain("BACKEND_UNAVAILABLE");
    } finally { executionControlOverride = undefined; }
  });

  it("keeps durable FIX_REQUIRED accepted when next-round dispatch is temporarily unavailable", async () => {
    const args = { request_id: "c2c_fix_dispatch_retryable", round: 1, verdict: "FIX_REQUIRED", summary: "fix" };
    dispatchFailure = true;
    try {
      const result = await client.callTool({ name: "review_v2_execution", arguments: args });
      expect(result.isError).not.toBe(true);
      expect(jsonOf(result)).toMatchObject({ round: 2, state: "EXECUTING_FIX",
        dispatch_status: "rejected", dispatch_error_code: "BACKEND_UNAVAILABLE" });
    } finally { dispatchFailure = false; }
  });

  it("preserves allowlisted Python workspace errors at the Node MCP boundary", async () => {
    const args = { request_id: "c2c_workspace_error_map", round: 1, verdict: "APPROVED", summary: "mapping" };
    executionControlOverride = { ok: false, error_code: "WORKSPACE_MISMATCH" };
    try {
      const failed = await client.callTool({ name: "review_v2_execution", arguments: args });
      expect(failed.isError).toBe(true);
      expect(textOf(failed)).toContain("WORKSPACE_MISMATCH");
      expect(textOf(failed)).not.toContain("BACKEND_UNAVAILABLE");
    } finally { executionControlOverride = undefined; }
  });

  it("rejects unregistered workspace selection and publishes no path or root inputs", async () => {
    const response = await fetch(`${bridge.localBaseUrl()}/mcp`, { method: "POST", headers: {
      authorization: `Bearer ${accessToken}`, "content-type": "application/json", accept: "application/json, text/event-stream" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 9, method: "tools/call", params: { name: "dispatch_execution",
        arguments: { workspace_id: "ffffffffffff", request_id: "c2c_fake_workspace", round: 1 } } }) });
    expect(response.status).toBe(403);
  });

  it("delegates read-only discovery bound to the current workspace", async () => {
    executionAgentsCalls.length = 0;
    executionAgentsResult = { workspace_id: bridge.workspace.id, agents: ["codex", "cursor", "opencode", "vscode"].map((id) => ({
      id, display_name: id, installed: true, available: true, execution_supported: false,
      model_selection_supported: false, models: [], default_model: null,
      model_discovery_supported: false, limitations: "fixture",
    })) };
    const result = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "list_execution_agents", arguments: {} })
    );
    expect(result).toEqual(executionAgentsResult);
    expect(executionAgentsCalls).toEqual([{ workspaceRoot: root, workspaceId: bridge.workspace.id }]);
  });

  it("accepts all structured agents returned by the Python capability backend", async () => {
    executionAgentsResult = { workspace_id: bridge.workspace.id, agents: ["codex", "cursor", "opencode", "copilot", "vscode"].map((id) => ({
      id, display_name: id, installed: true, available: true, execution_supported: id !== "vscode",
      model_selection_supported: id !== "vscode", models: [], default_model: null,
      model_discovery_supported: id === "cursor", limitations: null,
    })) };
    const result = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "list_execution_agents", arguments: {} })
    );
    expect(result).toEqual(executionAgentsResult);
  });

  it("fails safely when capability backend output is malformed or unavailable", async () => {
    executionAgentsResult = { workspace_id: "wrong", agents: [] };
    const malformed = await client.callTool({ name: "list_execution_agents", arguments: {} });
    expect(malformed.isError).toBe(true);
    expect(textOf(malformed)).toContain("BACKEND_UNAVAILABLE");
    executionAgentsFailure = true;
    const failed = await client.callTool({ name: "list_execution_agents", arguments: {} });
    executionAgentsFailure = false;
    expect(failed.isError).toBe(true);
    expect(textOf(failed)).not.toContain("private probe detail");
  });

  it("delegates exact identity once and preserves backend statuses", async () => {
    dispatchCalls.length = 0;
    const input = { request_id: "c2c_dispatch_test_01", round: 3 };
    for (const status of ["accepted", "already_running", "already_completed", "already_superseded", "report_pending", "terminal_error", "rejected"] as const) {
      dispatchResult = { status, ...input, workspace_id: bridge.workspace.id,
        ...(status === "rejected" ? { error_code: "V2_DISABLED", message: "V2 execution is disabled" } : {}) };
      const result = structuredJsonOf<Record<string, unknown>>(
        await client.callTool({ name: "dispatch_execution", arguments: input })
      );
      expect(result).toEqual(dispatchResult);
    }
    expect(dispatchCalls).toHaveLength(7);
    expect(dispatchCalls[0]).toEqual({ workspaceRoot: root, workspaceId: bridge.workspace.id, ...input });
    expect(dispatchCalls.map(({ request_id, round }) => ({ request_id, round }))).toEqual(Array(7).fill(input));
  });

  it("creates executable V2 tasks through the canonical backend, leaving Node mailbox writes generic", async () => {
    v2TaskCalls.length = 0;
    const input = { request_id: "c2c_create_task_test_01", task: "Test a disposable task lifecycle", agent_id: "critic-a" };
    v2TaskResult = { ok: true, ...input, workspace_id: bridge.workspace.id, round: 1,
      state: "AWAITING_AGENT_CRITIQUE", message_id: "message_create_task_01", already_existed: false };
    const result = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "create_v2_task", arguments: input })
    );
    expect(result).toEqual({ status: "created", request_id: input.request_id,
      workspace_id: bridge.workspace.id, round: 1, state: "AWAITING_AGENT_CRITIQUE", message_id: "message_create_task_01" });
    expect(v2TaskCalls).toEqual([{ workspaceRoot: root, workspaceId: bridge.workspace.id, ...input }]);
    const nodeMailbox = structuredJsonOf<{ messages: { type: string }[] }>(
      await client.callTool({ name: "mailbox_list", arguments: { request_id: input.request_id, type: "TASK_REQUEST" } })
    );
    expect(nodeMailbox.messages).toEqual([]);
  });

  it("delegates canonical critique and decision tools with exact workspace/request/round", async () => {
    v2LifecycleCalls.length = 0;
    const critiqueInput = { request_id: "c2c_public_critique_test", round: 1, agent_id: "critic-a", critique: "safe plan" };
    v2LifecycleResult = { ok: true, request_id: critiqueInput.request_id, workspace_id: bridge.workspace.id,
      round: 1, state: "AWAITING_WEB_PLAN_DECISION", status: "CRITIQUE_RECEIVED" };
    const critique = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "submit_v2_plan_critique", arguments: critiqueInput })
    );
    expect(critique).toMatchObject({ status: "accepted", request_id: critiqueInput.request_id,
      workspace_id: bridge.workspace.id, round: 1, state: "AWAITING_WEB_PLAN_DECISION" });
    expect(v2LifecycleCalls[0]).toMatchObject({ operation: "critique", workspaceRoot: root,
      workspaceId: bridge.workspace.id, ...critiqueInput });

    const decisionInput = { request_id: critiqueInput.request_id, round: 1, decision: "APPROVE" as const,
      plan: "safe plan", agent_id: "executor-a" };
    v2LifecycleResult = { ok: true, request_id: decisionInput.request_id, workspace_id: bridge.workspace.id,
      round: 1, state: "PLAN_APPROVED", status: "APPROVE" };
    const decision = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "decide_v2_plan", arguments: decisionInput })
    );
    expect(decision).toMatchObject({ status: "accepted", request_id: decisionInput.request_id,
      workspace_id: bridge.workspace.id, round: 1, state: "PLAN_APPROVED" });
    expect(v2LifecycleCalls[1]).toMatchObject({ operation: "decision", workspaceRoot: root,
      workspaceId: bridge.workspace.id, ...decisionInput });

    dispatchCalls.length = 0;
    const reviseInput = { request_id: "c2c_public_revise_test", round: 1, decision: "REVISE" as const,
      plan: "revise plan", agent_id: "critic-a" };
    v2LifecycleResult = { ok: true, request_id: reviseInput.request_id, workspace_id: bridge.workspace.id,
      round: 2, state: "AWAITING_AGENT_CRITIQUE", status: "REVISE" };
    dispatchResult = { status: "accepted", request_id: reviseInput.request_id, workspace_id: bridge.workspace.id,
      round: 2, assignment_id: `${reviseInput.request_id}:2:critique`, agent_id: "critic-a", worker_pid: 54321 };
    const revised = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "decide_v2_plan", arguments: reviseInput })
    );
    expect(revised).toMatchObject({ status: "accepted", request_id: reviseInput.request_id,
      workspace_id: bridge.workspace.id, round: 2, state: "AWAITING_AGENT_CRITIQUE",
      dispatch_status: "accepted", worker_pid: 54321 });
    expect(dispatchCalls.at(-1)).toEqual({ workspaceRoot: root, workspaceId: bridge.workspace.id,
      workspaceRegistryFile: undefined, request_id: reviseInput.request_id, round: 2 });
  });

  it("rejects malformed and unavailable canonical lifecycle backend results", async () => {
    v2LifecycleCalls.length = 0;
    const input = { request_id: "c2c_public_critique_test_2", round: 1, agent_id: "critic-a", critique: "safe" };
    v2LifecycleResult = { ok: true, request_id: input.request_id, workspace_id: "wrong", round: 1,
      state: "AWAITING_WEB_PLAN_DECISION" };
    const malformed = await client.callTool({ name: "submit_v2_plan_critique", arguments: input });
    expect(malformed.isError).toBe(true);
    expect(textOf(malformed)).toContain("BACKEND_UNAVAILABLE");
    v2LifecycleFailure = true;
    const failed = await client.callTool({ name: "submit_v2_plan_critique", arguments: input });
    v2LifecycleFailure = false;
    expect(failed.isError).toBe(true);
    expect(textOf(failed)).not.toContain("private lifecycle detail");
  });

  it("validates create_v2_task arguments and sanitizes backend failures", async () => {
    v2TaskCalls.length = 0;
    const invalid = await client.callTool({ name: "create_v2_task", arguments: {
      request_id: "bad id", task: "",
    } });
    expect(invalid.isError).toBe(true);
    expect(v2TaskCalls).toHaveLength(0);
    v2TaskFailure = true;
    const failed = await client.callTool({ name: "create_v2_task", arguments: {
      request_id: "c2c_create_task_test_02", task: "safe test task",
    } });
    v2TaskFailure = false;
    expect(failed.isError).toBe(true);
    expect(textOf(failed)).toContain("BACKEND_UNAVAILABLE");
    expect(textOf(failed)).not.toContain("private task detail");
  });

  it("maps malformed or throwing backends to a safe unavailable result", async () => {
    dispatchResult = { status: "accepted", request_id: "wrong", workspace_id: bridge.workspace.id, round: 1 };
    const result = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "dispatch_execution", arguments: { request_id: "c2c_dispatch_test_02", round: 1 } })
    );
    expect(result).toMatchObject({ status: "rejected", error_code: "BACKEND_UNAVAILABLE" });
    dispatchResult = { status: "future_status", request_id: "c2c_dispatch_test_02", workspace_id: bridge.workspace.id, round: 1 };
    const future = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "dispatch_execution", arguments: { request_id: "c2c_dispatch_test_02", round: 1 } })
    );
    expect(future).toMatchObject({ status: "future_status", request_id: "c2c_dispatch_test_02" });
    dispatchResult = { status: "INVALID-STATUS", request_id: "c2c_dispatch_test_02", workspace_id: bridge.workspace.id, round: 1 };
    const malformedStatus = structuredJsonOf<Record<string, unknown>>(
      await client.callTool({ name: "dispatch_execution", arguments: { request_id: "c2c_dispatch_test_02", round: 1 } })
    );
    expect(malformedStatus).toMatchObject({ status: "rejected", error_code: "BACKEND_UNAVAILABLE" });
    dispatchFailure = true;
    const thrown = await client.callTool({ name: "dispatch_execution", arguments: { request_id: "c2c_dispatch_test_03", round: 1 } });
    dispatchFailure = false;
    expect(structuredJsonOf<Record<string, unknown>>(thrown)).toMatchObject({ status: "rejected", error_code: "BACKEND_UNAVAILABLE" });
    expect(textOf(thrown)).not.toContain("private subprocess detail");
  });

  it("documents git_diff pagination with its output field names", async () => {
    const { tools } = await client.listTools();
    const description = tools.find((tool) => tool.name === "git_diff")?.description;
    expect(description).toContain("hasMore");
    expect(description).toContain("nextOffset");
    expect(description).not.toContain("has_more");
    expect(description).not.toContain("next_offset");
  });

  it("workspace_info returns identity and project detection", async () => {
    const result = await client.callTool({ name: "workspace_info", arguments: {} });
    const info = structuredJsonOf<{ workspaceId: string; projectType: string; frameworks: string[]; git: { isRepo: boolean; branch: string } }>(result);
    expect(info.workspaceId).toBe(bridge.workspace.id);
    expect(info.projectType).toBe("node");
    expect(info.frameworks).toContain("React");
    expect(info.git.isRepo).toBe(true);
    expect(info.git.branch).toBe("main");
  });

  it("round-trips all V2 types through MCP and preserves V1 mailbox tools", async () => {
    const requestId = "c2c_mcp_v2_roundtrip";
    for (const type of ["TASK_REQUEST", "PLAN_CRITIQUE", "PLAN_DECISION"] as const) {
      const submitted = structuredJsonOf<{ message_id: string; type: string; stored: boolean }>(
        await client.callTool({ name: "mailbox_submit_v2", arguments: {
          request_id: requestId, round: 1, type, payload: { source: "mcp-integration", type },
        } })
      );
      expect(submitted).toMatchObject({ type, stored: true });
      const listed = structuredJsonOf<{ messages: { message_id: string; type: string; payload_summary: Record<string, unknown> }[] }>(
        await client.callTool({ name: "mailbox_list", arguments: { request_id: requestId, type } })
      );
      expect(listed.messages[0]).toMatchObject({ message_id: submitted.message_id, type, payload_summary: { type } });
      const fetched = structuredJsonOf<{ type: string; round: number; payload: Record<string, unknown> }>(
        await client.callTool({ name: "mailbox_get", arguments: { message_id: submitted.message_id } })
      );
      expect(fetched).toMatchObject({ type, round: 1, payload: { source: "mcp-integration", type } });
    }
    const v1 = structuredJsonOf<{ stored: boolean; verdict: string }>(
      await client.callTool({ name: "mailbox_submit_review_response", arguments: {
        request_id: "c2c_mcp_v1_roundtrip", round: 1, verdict: "APPROVED", summary: "V1 remains available",
      } })
    );
    expect(v1).toMatchObject({ verdict: "APPROVED", stored: true });
  });

  it("rejects invalid V2 MCP arguments before mailbox persistence", async () => {
    for (const arguments_ of [
      { request_id: "c2c_mcp_invalid_type", round: 1, type: "PLAN_REQUEST", payload: {} },
      { request_id: "c2c_mcp_invalid_round", round: 0, type: "TASK_REQUEST", payload: {} },
      { request_id: "c2c_mcp_invalid_payload", round: 1, type: "TASK_REQUEST", payload: [] },
    ]) {
      const result = await client.callTool({ name: "mailbox_submit_v2", arguments: arguments_ });
      expect(result.isError).toBe(true);
      expect(textOf(result)).toContain("Input validation error");
    }
  });

  it("round-trips V2 through the admin HTTP transport and rejects invalid input", async () => {
    const requestId = "c2c_admin_v2_roundtrip";
    const submitted = await adminJson("/admin/mailbox/submit", {
      method: "POST", body: JSON.stringify({ type: "TASK_REQUEST", request_id: requestId, round: 1, payload: { via: "admin" } }),
    });
    expect(submitted.status).toBe(200);
    const messageId = submitted.body.message_id as string;
    const listed = await adminJson(`/admin/mailbox/list?request_id=${requestId}&type=TASK_REQUEST&min_round=1&limit=1`);
    expect(listed.status).toBe(200);
    expect((listed.body.messages as { message_id: string }[])[0].message_id).toBe(messageId);
    const latest = await adminJson(`/admin/mailbox/latest?request_id=${requestId}&type=TASK_REQUEST&min_round=1`);
    expect(latest.status).toBe(200);
    expect(latest.body.message_id).toBe(messageId);
    const fetched = await adminJson(`/admin/mailbox/get?message_id=${messageId}`);
    expect(fetched.status).toBe(200);
    expect(fetched.body.payload).toEqual({ via: "admin" });
    for (const invalid of [
      ["/admin/mailbox/submit", { type: "UNKNOWN", request_id: requestId, round: 1, payload: {} }],
      ["/admin/mailbox/submit", { type: "TASK_REQUEST", request_id: requestId, round: 1.5, payload: {} }],
      ["/admin/mailbox/submit", { type: "TASK_REQUEST", request_id: requestId, round: 1, payload: [] }],
    ] as const) {
      const response = await adminJson(invalid[0], { method: "POST", body: JSON.stringify(invalid[1]) });
      expect(response.status).toBe(400);
    }
    for (const path of [
      "/admin/mailbox/list?type=UNKNOWN",
      "/admin/mailbox/list?min_round=1.5",
      "/admin/mailbox/list?limit=0",
      `/admin/mailbox/latest?request_id=${requestId}&type=UNKNOWN`,
    ]) {
      expect((await adminJson(path)).status).toBe(400);
    }
  });

  it("read_file returns hello.txt", async () => {
    const result = await client.callTool({ name: "read_file", arguments: { path: "hello.txt" } });
    const file = structuredJsonOf<{ content: string; totalLines: number }>(result);
    expect(file.content).toContain("Hello from Codex with ChatGPT!");
  });

  it("read_image returns metadata and image content", async () => {
    const result = await client.callTool({ name: "read_image", arguments: { path: "pixel.png" } });
    expect(result.structuredContent).toEqual({ path: "pixel.png", sizeBytes: 11, mimeType: "image/png" });
    const content = result.content as { type: string; mimeType?: string }[];
    expect(content.some((item) => item.type === "image" && item.mimeType === "image/png")).toBe(true);
  });

  it("read_file denies .env with ACCESS_DENIED_SENSITIVE_FILE and no content", async () => {
    const result = await client.callTool({ name: "read_file", arguments: { path: ".env" } });
    expect(result.isError).toBe(true);
    expect(textOf(result)).toContain("ACCESS_DENIED_SENSITIVE_FILE");
    expect(textOf(result)).not.toContain("supersecret");
  });

  it("read_file denies paths outside the workspace", async () => {
    const result = await client.callTool({ name: "read_file", arguments: { path: "../../etc/hosts" } });
    expect(result.isError).toBe(true);
    expect(textOf(result)).toContain("PATH_OUTSIDE_WORKSPACE");
  });

  it("list_directory lists the tree", async () => {
    const result = await client.callTool({ name: "list_directory", arguments: { path: ".", depth: 2 } });
    const listing = structuredJsonOf<{ entries: { path: string }[] }>(result);
    const paths = listing.entries.map((entry) => entry.path);
    expect(paths).toContain("hello.txt");
    expect(paths).toContain("src/index.ts");
    expect(paths).not.toContain(".env");
  });

  it("search_workspace finds matches", async () => {
    const result = await client.callTool({ name: "search_workspace", arguments: { query: "answer" } });
    const search = structuredJsonOf<{ matches: { path: string; line: number }[] }>(result);
    expect(search.matches.some((match) => match.path === "src/index.ts")).toBe(true);
  });

  it("git_status reports the dirty file", async () => {
    const result = await client.callTool({ name: "git_status", arguments: {} });
    const status = structuredJsonOf<{ isRepo: boolean; unstaged: { path: string }[] }>(result);
    expect(status.isRepo).toBe(true);
    expect(status.unstaged.some((entry) => entry.path === "src/index.ts")).toBe(true);
  });

  it("git_diff shows the change", async () => {
    const result = await client.callTool({ name: "git_diff", arguments: { mode: "unstaged" } });
    const diff = structuredJsonOf<{ diff: string; hasMore: boolean }>(result);
    expect(diff.diff).toContain("answer = 43");
    expect(diff.hasMore).toBe(false);
  });

  it("git_diff paginates large diffs", async () => {
    const big = Array.from({ length: 20000 }, (_, i) => `content line ${i}`).join("\n");
    write(root, "big-change.txt", big);
    git(root, "add", "big-change.txt");
    const first = structuredJsonOf<{ hasMore: boolean; nextOffset: number; totalBytes: number; returnedBytes: number }>(
      await client.callTool({ name: "git_diff", arguments: { mode: "staged", max_bytes: 4096 } })
    );
    expect(first.hasMore).toBe(true);
    expect(first.returnedBytes).toBeLessThanOrEqual(4096);
    const second = structuredJsonOf<{ offset: number; diff: string }>(
      await client.callTool({
        name: "git_diff",
        arguments: { mode: "staged", max_bytes: 4096, offset: first.nextOffset },
      })
    );
    expect(second.offset).toBe(first.nextOffset);
    expect(second.diff.length).toBeGreaterThan(0);
    git(root, "reset", "big-change.txt");
  });

  it("execution_summary and test_status read harness records", async () => {
    appendExecutionRecord(bridge.workspace.id, {
      taskId: "c2c_test1",
      iteration: 1,
      changedFiles: ["src/index.ts"],
      tests: "27 passed",
      exitStatus: "ok",
      timestamp: new Date().toISOString(),
    });
    const summary = structuredJsonOf<{ records: { taskId: string }[] }>(
      await client.callTool({ name: "execution_summary", arguments: {} })
    );
    expect(summary.records[0].taskId).toBe("c2c_test1");

    const status = structuredJsonOf<{ available: boolean; tests: string; outputAvailable: boolean; outputId: number | null }>(
      await client.callTool({ name: "test_status", arguments: {} })
    );
    expect(status.available).toBe(true);
    expect(status.tests).toBe("27 passed");
    expect(status.outputAvailable).toBe(false);
    expect(status.outputId).toBeNull();
  });

  it("skips invalid persisted records when reporting execution status", async () => {
    appendExecutionRecord(bridge.workspace.id, {
      taskId: "c2c_valid_before_invalid",
      iteration: 2,
      changedFiles: 0,
      tests: "31 passed",
      exitStatus: "ok",
      timestamp: new Date().toISOString(),
    });
    fs.appendFileSync(
      path.join(stateDir, "executions", `${bridge.workspace.id}.jsonl`),
      JSON.stringify({
        taskId: "c2c_invalid",
        iteration: null,
        changedFiles: 0,
        tests: null,
        exitStatus: "ok",
        timestamp: new Date().toISOString(),
      }) + "\n"
    );

    const statusResult = await client.callTool({ name: "test_status", arguments: {} });
    expect(statusResult.isError ?? false).toBe(false);
    const status = structuredJsonOf<{ taskId: string; iteration: number }>(statusResult);
    expect(status.taskId).toBe("c2c_valid_before_invalid");
    expect(status.iteration).toBe(2);

    const summaryResult = await client.callTool({ name: "execution_summary", arguments: { limit: 1 } });
    expect(summaryResult.isError ?? false).toBe(false);
    const summary = structuredJsonOf<{ records: { taskId: string }[] }>(summaryResult);
    expect(summary.records.map((record) => record.taskId)).toEqual(["c2c_valid_before_invalid"]);
  });

  it("execution_output lists readable items and refuses restricted bodies", async () => {
    const readable = saveExecutionOutput(bridge.workspace.id, {
      command: "pnpm test",
      raw: "FAIL src/a.test.ts\nAssertionError: expected true",
      exitCode: 1,
    });
    const hidden = saveExecutionOutput(bridge.workspace.id, {
      command: "print-key",
      raw: "-----BEGIN RSA PRIVATE KEY-----\nsecret\n-----END RSA PRIVATE KEY-----",
      exitCode: 0,
    });
    const listResult = await client.callTool({
      name: "execution_output",
      arguments: { action: "list" },
    });
    const list = structuredJsonOf<{
      action: "list";
      items: { id: number; status: string; command: string; text?: string }[];
    }>(listResult);
    expect(list.action).toBe("list");
    expect(list.items.some((item) => item.id === readable.id && item.status === "readable")).toBe(true);
    expect(list.items.some((item) => item.id === hidden.id && item.status === "restricted")).toBe(true);
    expect(list.items.every((item) => item.text === undefined)).toBe(true);

    const readResult = await client.callTool({
      name: "execution_output",
      arguments: { action: "read", id: readable.id },
    });
    const body = structuredJsonOf<{ action: "read"; text: string }>(readResult);
    expect(body.action).toBe("read");
    expect(body.text).toContain("AssertionError");

    const denied = await client.callTool({
      name: "execution_output",
      arguments: { action: "read", id: hidden.id },
    });
    expect(denied.isError).toBe(true);
    expect(textOf(denied)).toContain("OUTPUT_RESTRICTED");
    expect(textOf(denied)).not.toContain("BEGIN RSA");

    const missing = await client.callTool({
      name: "execution_output",
      arguments: { action: "read", id: 999999 },
    });
    expect(missing.isError).toBe(true);
    expect(textOf(missing)).toContain("NOT_FOUND");
  });

  it("enforces scopes per tool", async () => {
    const limited = bridge.authStore.issueTokens({ clientId: "limited", scopes: ["workspace.read"] });
    const limitedClient = new Client({ name: "limited", version: "1.0.0" });
    const transport = new StreamableHTTPClientTransport(new URL(`${bridge.localBaseUrl()}/mcp`), {
      requestInit: { headers: { authorization: `Bearer ${limited.accessToken}` } },
    });
    await limitedClient.connect(transport);
    const denied = await limitedClient.callTool({ name: "git_diff", arguments: {} });
    expect(denied.isError).toBe(true);
    expect(textOf(denied)).toContain("INSUFFICIENT_SCOPE");
    const outputDenied = await limitedClient.callTool({
      name: "execution_output",
      arguments: { action: "list" },
    });
    expect(outputDenied.isError).toBe(true);
    expect(textOf(outputDenied)).toContain("INSUFFICIENT_SCOPE");
    const dispatchDenied = await limitedClient.callTool({ name: "dispatch_execution", arguments: {
      request_id: "c2c_dispatch_auth_denied", round: 1,
    } });
    expect(dispatchDenied.isError).toBe(true);
    expect(textOf(dispatchDenied)).toContain("INSUFFICIENT_SCOPE");
    const createDenied = await limitedClient.callTool({ name: "create_v2_task", arguments: {
      request_id: "c2c_create_task_auth_denied", task: "must not delegate",
    } });
    expect(createDenied.isError).toBe(true);
    expect(textOf(createDenied)).toContain("INSUFFICIENT_SCOPE");
    for (const [name, args] of [
      ["review_v2_execution", { request_id: "c2c_control_auth_denied", round: 1, verdict: "APPROVED", summary: "safe" }],
      ["reroute_v2_execution", { request_id: "c2c_control_auth_denied", round: 1, agent_id: "fixer" }],
    ] as const) {
      const denied = await limitedClient.callTool({ name, arguments: args });
      expect(denied.isError).toBe(true);
      expect(textOf(denied)).toContain("INSUFFICIENT_SCOPE");
    }
    const discoveryDenied = await limitedClient.callTool({ name: "list_execution_agents", arguments: {} });
    expect(discoveryDenied.isError).toBe(true);
    expect(textOf(discoveryDenied)).toContain("INSUFFICIENT_SCOPE");
    const allowed = await limitedClient.callTool({ name: "read_file", arguments: { path: "hello.txt" } });
    expect(allowed.isError ?? false).toBe(false);
    await limitedClient.close();
  });

  it("git_diff over MCP excludes sensitive files like .npmrc and service-account*.json", async () => {
    write(root, ".npmrc", "//registry.npmjs.org/:_authToken=supersecret-npm-token\n");
    write(root, "service-account-test.json", '{"private_key": "supersecret-sa-key"}\n');
    write(root, "src/visible.ts", "export const visible = 'safe-change';\n");

    git(root, "add", "-f", ".npmrc", "service-account-test.json", "src/visible.ts");

    const result = jsonOf<{ diff: string; isRepo: boolean }>(
      await client.callTool({ name: "git_diff", arguments: { mode: "staged" } })
    );

    expect(result.isRepo).toBe(true);
    expect(result.diff).toContain("safe-change");
    expect(result.diff).not.toContain("supersecret-npm-token");
    expect(result.diff).not.toContain("supersecret-sa-key");

    git(root, "rm", "-f", "--cached", ".npmrc", "service-account-test.json", "src/visible.ts");
  });

  it("git_diff over MCP blocks sensitive-to-safe renames from leaking original content", async () => {
    write(root, ".npmrc", "//registry.npmjs.org/:_authToken=mcp-secret-token-123\n");
    git(root, "add", "-f", ".npmrc");
    git(root, "commit", "-m", "add secret to rename");

    git(root, "mv", ".npmrc", "public_harmless.txt");

    const result = jsonOf<{ diff: string; isRepo: boolean }>(
      await client.callTool({ name: "git_diff", arguments: { mode: "staged" } })
    );

    expect(result.isRepo).toBe(true);
    expect(result.diff).not.toContain("mcp-secret-token-123");
    expect(result.diff).not.toContain("public_harmless.txt");

    git(root, "reset", "--hard", "HEAD");
  });

  it("git_diff over MCP with path='src' blocks cross-boundary rename leaks from root secrets", async () => {
    write(root, ".npmrc", "//registry.npmjs.org/:_authToken=root-mcp-scoped-secret\n");
    git(root, "add", "-f", ".npmrc");
    git(root, "commit", "-m", "add root secret for scoped test");

    // Rename root .npmrc to src/public.txt
    git(root, "mv", ".npmrc", "src/public.txt");

    const result = jsonOf<{ diff: string; isRepo: boolean }>(
      await client.callTool({
        name: "git_diff",
        arguments: { mode: "staged", path: "src" },
      })
    );

    expect(result.isRepo).toBe(true);
    expect(result.diff).not.toContain("root-mcp-scoped-secret");
    expect(result.diff).not.toContain("src/public.txt");

    git(root, "reset", "--hard", "HEAD");
  });
});
