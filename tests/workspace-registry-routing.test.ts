import { describe, it, expect } from "vitest";
import path from "node:path";
import fs from "node:fs";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { startBridge } from "../src/bridge/server.js";
import { Workspace } from "../src/workspace/manager.js";
import { makeTmpDir, cleanup, write, isolateStateDir } from "./helpers.js";

describe("registered workspace routing", () => {
  it("refreshes a changed registry on a healthy bridge and routes backend calls to the added root", async () => {
    isolateStateDir();
    const primary = makeTmpDir("registry-refresh-primary");
    const secondary = makeTmpDir("registry-refresh-secondary");
    const primaryWorkspace = new Workspace(primary);
    const secondaryWorkspace = new Workspace(secondary);
    const registry = path.join(makeTmpDir("registry-refresh-file"), "workspace-registry.json");
    fs.writeFileSync(registry, JSON.stringify({ [primaryWorkspace.id]: primary }), "utf8");
    const calls: string[] = [];
    const bridge = await startBridge({ workspaceRoot: primary, workspaceRegistryFile: registry,
      port: 0, persistRuntime: false, authStoreFile: path.join(makeTmpDir("registry-refresh-auth"), "store.json"),
      v2TaskCreationBackend: async (input) => ({ ok: true, request_id: input.request_id,
        workspace_id: input.workspaceId, round: 1, state: "AWAITING_AGENT_CRITIQUE", message_id: "fixture-message" }),
      executionAgentsBackend: (input) => {
        calls.push(`${input.workspaceId}:${input.workspaceRoot}`);
        return { workspace_id: input.workspaceId, agents: ["codex", "cursor", "opencode", "vscode"].map((id) => ({ id,
          display_name: id, installed: true, available: true, execution_supported: false,
          model_selection_supported: false, models: [], default_model: null,
          model_discovery_supported: false, limitations: null })) };
      },
    });
    const token = bridge.authStore.issueTokens({ clientId: "registry-refresh", scopes: ["execution.read", "mailbox.read", "workspace.read", "mailbox.write"] }).accessToken;
    const client = new Client({ name: "registry-refresh-test", version: "1" });
    const transport = new StreamableHTTPClientTransport(new URL(`${bridge.localBaseUrl()}/mcp`), {
      requestInit: { headers: { Authorization: `Bearer ${token}` } },
    });
    try {
      await client.connect(transport);
      const primaryResult = await client.callTool({ name: "list_execution_agents", arguments: {} });
      expect(primaryResult.isError).not.toBe(true);
      fs.writeFileSync(registry, JSON.stringify({ [primaryWorkspace.id]: primary, [secondaryWorkspace.id]: secondary }), "utf8");
      const secondaryResult = await client.callTool({ name: "list_execution_agents", arguments: { workspace_id: secondaryWorkspace.id } });
      expect(secondaryResult.isError).not.toBe(true);
      const secondaryInfo = await client.callTool({ name: "workspace_info", arguments: { workspace_id: secondaryWorkspace.id } });
      expect(JSON.parse(String(secondaryInfo.content?.[0]?.text)).workspaceId).toBe(secondaryWorkspace.id);
      const task = await client.callTool({ name: "create_v2_task", arguments: {
        workspace_id: secondaryWorkspace.id, request_id: "c2c_dynamic_registry_task", task: "disposable routing seam",
      } });
      expect(task.isError).not.toBe(true);
      expect(calls).toEqual([`${primaryWorkspace.id}:${primaryWorkspace.root}`, `${secondaryWorkspace.id}:${secondaryWorkspace.root}`]);
    } finally {
      await client.close();
      await bridge.close();
      cleanup(primary);
      cleanup(secondary);
      cleanup(path.dirname(registry));
    }
  });

  it("refreshes a changed registry on a healthy bridge and preserves primary routing", async () => {
    isolateStateDir();
    const primary = makeTmpDir("registry-refresh-primary");
    const secondary = makeTmpDir("registry-refresh-secondary");
    const primaryWorkspace = new Workspace(primary);
    const secondaryWorkspace = new Workspace(secondary);
    const registry = path.join(makeTmpDir("registry-refresh-file"), "workspace-registry.json");
    fs.writeFileSync(registry, JSON.stringify({}), "utf8");
    const agentCalls: Array<{ workspaceId: string; workspaceRoot: string }> = [];
    const taskCalls: Array<{ workspaceId: string; workspaceRoot: string }> = [];
    const bridge = await startBridge({ workspaceRoot: primary, workspaceRegistryFile: registry,
      port: 0, persistRuntime: false, authStoreFile: path.join(makeTmpDir("registry-refresh-auth"), "store.json"),
      executionAgentsBackend: ({ workspaceId, workspaceRoot }) => {
        agentCalls.push({ workspaceId, workspaceRoot });
        return { workspace_id: workspaceId, agents: ["codex", "cursor"].map((id) => ({ id,
          display_name: id, installed: true, available: true, execution_supported: true,
          model_selection_supported: true, models: [], default_model: null,
          model_discovery_supported: false, limitations: null })) };
      },
      v2TaskCreationBackend: async ({ workspaceId, workspaceRoot, request_id }) => {
        taskCalls.push({ workspaceId, workspaceRoot });
        return { ok: true, request_id, workspace_id: workspaceId, round: 1,
          state: "AWAITING_AGENT_CRITIQUE", message_id: "msg_registered_task" };
      },
    });
    const token = bridge.authStore.issueTokens({ clientId: "registry-refresh", scopes: ["mailbox.read", "mailbox.write", "workspace.read"] }).accessToken;
    const client = new Client({ name: "registry-refresh-test", version: "1" });
    const transport = new StreamableHTTPClientTransport(new URL(`${bridge.localBaseUrl()}/mcp`), {
      requestInit: { headers: { Authorization: `Bearer ${token}` } },
    });
    try {
      await client.connect(transport);
      const primaryInfo = await client.callTool({ name: "workspace_info", arguments: {} });
      expect(JSON.parse(String(primaryInfo.content?.[0]?.text)).workspaceId).toBe(primaryWorkspace.id);

      // Simulate a workspace being registered after the already-healthy bridge started.
      fs.writeFileSync(registry, JSON.stringify({ [secondaryWorkspace.id]: secondary }), "utf8");
      const listed = await client.callTool({ name: "list_execution_agents", arguments: { workspace_id: secondaryWorkspace.id } });
      expect(listed.isError).not.toBe(true);
      expect(agentCalls).toEqual([{ workspaceId: secondaryWorkspace.id, workspaceRoot: secondaryWorkspace.root }]);
      const created = await client.callTool({ name: "create_v2_task", arguments: {
        workspace_id: secondaryWorkspace.id, request_id: "registry_refresh_task_01", task: "disposable routing probe",
      } });
      expect(created.isError).not.toBe(true);
      expect(JSON.parse(String(created.content?.[0]?.text))).toMatchObject({
        status: "created", workspace_id: secondaryWorkspace.id, state: "AWAITING_AGENT_CRITIQUE",
      });
      expect(taskCalls).toEqual([{ workspaceId: secondaryWorkspace.id, workspaceRoot: secondaryWorkspace.root }]);

      const primaryAgents = await client.callTool({ name: "list_execution_agents", arguments: {} });
      expect(primaryAgents.isError).not.toBe(true);
      expect(agentCalls.at(-1)).toEqual({ workspaceId: primaryWorkspace.id, workspaceRoot: primaryWorkspace.root });
    } finally {
      await client.close();
      await bridge.close();
      cleanup(primary);
      cleanup(secondary);
    }
  });

  it("preserves request ownership across registered workspaces without probing unregistered roots", async () => {
    isolateStateDir();
    const primary = makeTmpDir("registry-owner-primary");
    const owner = makeTmpDir("registry-owner-other");
    const primaryWorkspace = new Workspace(primary);
    const ownerWorkspace = new Workspace(owner);
    const requestId = "c2c_registered_owner_probe";
    write(owner, `runtime/state/${ownerWorkspace.id}/${requestId}.json`, JSON.stringify({
      request_id: requestId, workspace_id: ownerWorkspace.id, state: "REQUESTING_REVIEW", round: 1,
    }));
    const calls: Array<{ request_id: string; workspaceId: string }> = [];
    const bridge = await startBridge({ workspaceRoot: primary,
      workspaceRoots: { [ownerWorkspace.id]: owner }, port: 0, persistRuntime: false,
      authStoreFile: path.join(makeTmpDir("registry-owner-auth"), "store.json"),
      v2ExecutionControlBackend: async (input) => {
        calls.push({ request_id: input.request_id, workspaceId: input.workspaceId });
        return input.request_id === requestId
          ? { ok: true, request_id: requestId, workspace_id: input.workspaceId, round: 1, state: "DONE" }
          : { ok: false, error_code: "REQUEST_NOT_FOUND" };
      },
    });
    const token = bridge.authStore.issueTokens({ clientId: "registry-review", scopes: ["mailbox.write"] }).accessToken;
    const client = new Client({ name: "registered-review-owner-test", version: "1" });
    const transport = new StreamableHTTPClientTransport(new URL(`${bridge.localBaseUrl()}/mcp`), {
      requestInit: { headers: { Authorization: `Bearer ${token}` } },
    });
    try {
      await client.connect(transport);
      const wrong = await client.callTool({ name: "review_v2_execution", arguments: {
        request_id: requestId, round: 1, workspace_id: primaryWorkspace.id,
        verdict: "APPROVED", summary: "negative workspace routing probe",
      } });
      expect(wrong.isError).toBe(true);
      expect(String(wrong.content?.[0]?.text)).toContain("WORKSPACE_MISMATCH");
      expect(calls).toEqual([]);

      const unknown = await client.callTool({ name: "review_v2_execution", arguments: {
        request_id: "c2c_unknown_registered_request", round: 1, workspace_id: primaryWorkspace.id,
        verdict: "APPROVED", summary: "unknown request remains not found",
      } });
      expect(unknown.isError).toBe(true);
      expect(String(unknown.content?.[0]?.text)).toContain("REQUEST_NOT_FOUND");

      const correct = await client.callTool({ name: "review_v2_execution", arguments: {
        request_id: requestId, round: 1, workspace_id: ownerWorkspace.id,
        verdict: "APPROVED", summary: "correct registered workspace",
      } });
      expect(correct.isError).not.toBe(true);
      expect(JSON.parse(String(correct.content?.[0]?.text))).toMatchObject({ state: "DONE", workspace_id: ownerWorkspace.id });
      expect(calls).toEqual([{ request_id: "c2c_unknown_registered_request", workspaceId: primaryWorkspace.id },
        { request_id: requestId, workspaceId: ownerWorkspace.id }]);
    } finally {
      await client.close();
      await bridge.close();
      cleanup(primary);
      cleanup(owner);
    }
  });

  it("selects only configured canonical roots by workspace_id and rejects escapes", async () => {
    isolateStateDir();
    const dev = makeTmpDir("registry-dev");
    const hotfix = makeTmpDir("registry-hotfix");
    const c2c = makeTmpDir("registry-c2c");
    const outside = makeTmpDir("registry-outside");
    write(dev, "README.md", "dev root");
    write(hotfix, "README.md", "hotfix root");
    write(c2c, "README.md", "c2c root");
    write(outside, "README.md", "outside root");
    const devWorkspace = new Workspace(dev);
    const hotfixWorkspace = new Workspace(hotfix);
    const c2cWorkspace = new Workspace(c2c);
    const bridge = await startBridge({
      workspaceRoot: hotfix,
      workspaceRoots: { [devWorkspace.id]: dev, [c2cWorkspace.id]: c2c },
      port: 0,
      persistRuntime: false,
      authStoreFile: path.join(makeTmpDir("registry-auth"), "store.json"),
    });
    const token = bridge.authStore.issueTokens({ clientId: "registry-test", scopes: ["workspace.read"] }).accessToken;
    const client = new Client({ name: "workspace-registry-test", version: "1" });
    const transport = new StreamableHTTPClientTransport(new URL(`${bridge.localBaseUrl()}/mcp`), {
      requestInit: { headers: { Authorization: `Bearer ${token}` } },
    });
    try {
      await client.connect(transport);
      const devInfo = await client.callTool({ name: "workspace_info", arguments: { workspace_id: devWorkspace.id } });
      expect(JSON.parse(String(devInfo.content?.[0]?.text)).workspaceId).toBe(devWorkspace.id);
      const devRead = await client.callTool({ name: "read_file", arguments: {
        workspace_id: devWorkspace.id, path: "README.md",
      } });
      expect(JSON.parse(String(devRead.content?.[0]?.text)).content).toBe("dev root");
      const injectedRoot = await client.callTool({ name: "read_file", arguments: {
        workspace_id: devWorkspace.id, workspace_root: outside, path: "README.md",
      } });
      expect(JSON.parse(String(injectedRoot.content?.[0]?.text)).content).toBe("dev root");
      const hotfixInfo = await client.callTool({ name: "workspace_info", arguments: { workspace_id: hotfixWorkspace.id } });
      expect(JSON.parse(String(hotfixInfo.content?.[0]?.text)).workspaceId).toBe(hotfixWorkspace.id);
      const hotfixRead = await client.callTool({ name: "read_file", arguments: {
        workspace_id: hotfixWorkspace.id, path: "README.md",
      } });
      expect(JSON.parse(String(hotfixRead.content?.[0]?.text)).content).toBe("hotfix root");
      const c2cInfo = await client.callTool({ name: "workspace_info", arguments: { workspace_id: c2cWorkspace.id } });
      expect(JSON.parse(String(c2cInfo.content?.[0]?.text)).workspaceId).toBe(c2cWorkspace.id);
      const c2cRead = await client.callTool({ name: "read_file", arguments: {
        workspace_id: c2cWorkspace.id, path: "README.md",
      } });
      expect(JSON.parse(String(c2cRead.content?.[0]?.text)).content).toBe("c2c root");

      const traversal = await client.callTool({ name: "read_file", arguments: {
        workspace_id: devWorkspace.id, path: "../outside.txt",
      } });
      expect(traversal.isError).toBe(true);
      const absolute = await client.callTool({ name: "read_file", arguments: {
        workspace_id: devWorkspace.id, path: path.join(hotfixWorkspace.root, "README.md"),
      } });
      expect(absolute.isError).toBe(true);
    } finally {
      await client.close();
      await bridge.close();
      cleanup(dev);
      cleanup(hotfix);
      cleanup(c2c);
      cleanup(outside);
    }
  });

  it("fails closed for unknown IDs and registry ID/root mismatches", async () => {
    isolateStateDir();
    const root = makeTmpDir("registry-primary");
    const wrong = makeTmpDir("registry-wrong");
    const bridge = await startBridge({ workspaceRoot: root, port: 0, persistRuntime: false,
      authStoreFile: path.join(makeTmpDir("registry-auth"), "store.json") });
    const token = bridge.authStore.issueTokens({ clientId: "registry-test", scopes: ["workspace.read"] }).accessToken;
    try {
      const response = await fetch(`${bridge.localBaseUrl()}/mcp`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json",
          Accept: "application/json, text/event-stream" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: {
          name: "workspace_info", arguments: { workspace_id: "ffffffffffff" },
        } }),
      });
      expect(response.status).toBe(403);
      await expect(startBridge({ workspaceRoot: root, workspaceRoots: { "ffffffffffff": wrong },
        port: 0, persistRuntime: false })).rejects.toThrow(/does not match its canonical root/);
    } finally {
      await bridge.close();
      cleanup(root);
      cleanup(wrong);
    }
  });

  it("routes authenticated admin mailbox operations only through registered workspace IDs", async () => {
    isolateStateDir();
    const dev = makeTmpDir("admin-registry-dev");
    const hotfix = makeTmpDir("admin-registry-hotfix");
    const c2c = makeTmpDir("admin-registry-c2c");
    const outside = makeTmpDir("admin-registry-outside");
    const devWorkspace = new Workspace(dev);
    const hotfixWorkspace = new Workspace(hotfix);
    const c2cWorkspace = new Workspace(c2c);
    const bridge = await startBridge({
      workspaceRoot: hotfix,
      workspaceRoots: { [devWorkspace.id]: dev, [c2cWorkspace.id]: c2c },
      port: 0,
      persistRuntime: false,
      authStoreFile: path.join(makeTmpDir("admin-registry-auth"), "store.json"),
    });
    const base = bridge.localBaseUrl();
    const auth = { Authorization: `Bearer ${bridge.adminToken}`, "Content-Type": "application/json" };
    const submit = async (body: Record<string, unknown>, headers = auth) => fetch(`${base}/admin/mailbox/submit`, {
      method: "POST", headers, body: JSON.stringify(body),
    });
    try {
      for (const selected of [devWorkspace, hotfixWorkspace, c2cWorkspace]) {
        const requestId = `admin-route-${selected.id}`;
        const response = await submit({ workspace_id: selected.id, type: "PLAN_REQUEST", request_id: requestId,
          round: 1, payload: { marker: selected.id } });
        expect(response.status).toBe(200);
        const created = await response.json() as { message_id: string };
        const qs = `workspace_id=${selected.id}&request_id=${requestId}&type=PLAN_REQUEST&min_round=1`;
        const listed = await fetch(`${base}/admin/mailbox/list?${qs}`, { headers: auth });
        expect(listed.status).toBe(200);
        const listData = await listed.json() as { messages: Array<{ workspace_id: string; message_id: string }> };
        expect(listData.messages.map((message) => message.workspace_id)).toEqual([selected.id]);
        const fetched = await fetch(`${base}/admin/mailbox/get?workspace_id=${selected.id}&message_id=${created.message_id}`, { headers: auth });
        expect((await fetched.json() as { workspace_id: string }).workspace_id).toBe(selected.id);
        const latest = await fetch(`${base}/admin/mailbox/latest?${qs}`, { headers: auth });
        expect((await latest.json() as { workspace_id: string }).workspace_id).toBe(selected.id);
      }

      const unknown = await submit({ workspace_id: "ffffffffffff", type: "PLAN_REQUEST", request_id: "unknown-id",
        round: 1, payload: {} });
      expect(unknown.status).toBe(403);
      for (const injected of [outside, "D:\\", "..\\outside"]) {
        const response = await submit({ workspace_id: injected, type: "PLAN_REQUEST", request_id: `injected-${injected}`,
          round: 1, payload: {} });
        expect(response.status).toBe(403);
      }
      const mismatch = await submit({ workspace_id: devWorkspace.id, type: "PLAN_REQUEST", request_id: "payload-mismatch",
        round: 1, payload: { workspace_id: hotfixWorkspace.id } });
      expect(mismatch.status).toBe(403);
      const injectedRoot = await submit({ workspace_id: devWorkspace.id, workspace_root: outside, path: "..\\secret",
        type: "PLAN_REQUEST", request_id: "root-fields-ignored", round: 1, payload: {} });
      expect(injectedRoot.status).toBe(200);

      const primaryId = hotfixWorkspace.id;
      const primaryRequest = "admin-route-primary-default";
      const primarySubmit = await submit({ type: "PLAN_REQUEST", request_id: primaryRequest, round: 1, payload: {} });
      expect(primarySubmit.status).toBe(200);
      const primaryCreated = await primarySubmit.json() as { message_id: string };
      const primaryList = await fetch(`${base}/admin/mailbox/list?request_id=${primaryRequest}`, { headers: auth });
      expect(((await primaryList.json()) as { messages: Array<{ workspace_id: string }> }).messages[0].workspace_id).toBe(primaryId);
      const primaryGet = await fetch(`${base}/admin/mailbox/get?message_id=${primaryCreated.message_id}`, { headers: auth });
      expect((await primaryGet.json() as { workspace_id: string }).workspace_id).toBe(primaryId);
      const primaryLatest = await fetch(`${base}/admin/mailbox/latest?request_id=${primaryRequest}&type=PLAN_REQUEST`, { headers: auth });
      expect((await primaryLatest.json() as { workspace_id: string }).workspace_id).toBe(primaryId);

      const original = { workspace_id: c2cWorkspace.id, type: "PLAN_REQUEST", request_id: "admin-idempotency",
        round: 1, payload: { value: "same" } };
      const first = await submit(original);
      const firstBody = await first.json() as { message_id: string };
      const duplicate = await submit(original);
      expect(duplicate.status).toBe(200);
      expect(await duplicate.json()).toMatchObject({ message_id: firstBody.message_id, is_duplicate: true });
      const conflict = await submit({ ...original, payload: { value: "different" } });
      expect(conflict.status).toBe(409);

      expect((await submit({ type: "PLAN_REQUEST", request_id: "missing-auth", round: 1, payload: {} },
        { "Content-Type": "application/json" })).status).toBe(404);
      expect((await submit({ type: "PLAN_REQUEST", request_id: "wrong-auth", round: 1, payload: {} },
        { ...auth, Authorization: "Bearer wrong" })).status).toBe(404);
      expect((await submit({ type: "PLAN_REQUEST", request_id: "forwarded-auth", round: 1, payload: {} },
        { ...auth, "x-forwarded-for": "203.0.113.1" })).status).toBe(404);
      expect((await submit({ type: "PLAN_REQUEST", request_id: "cloudflare-forwarded-auth", round: 1, payload: {} },
        { ...auth, "cf-connecting-ip": "203.0.113.1" })).status).toBe(404);

      const mcpUnauthenticated = await fetch(`${base}/mcp`, {
        method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json, text/event-stream" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }),
      });
      expect(mcpUnauthenticated.status).toBe(401);
    } finally {
      await bridge.close();
      cleanup(dev);
      cleanup(hotfix);
      cleanup(c2c);
      cleanup(outside);
    }
  });
});
