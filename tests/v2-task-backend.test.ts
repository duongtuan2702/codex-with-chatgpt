import { describe, expect, it } from "vitest";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { makeTmpDir } from "./helpers.js";
import { createServer } from "node:http";
import { buildV2ExecutionControlCommand, invokeV2ExecutionControlBackend, buildV2ExecutionWakeCommand, invokeV2ExecutionWakeBackend, buildV2LifecycleCommand, buildV2TaskCreationCommand, classifyPythonStderrDetail,
  invokeV2LifecycleBackend, invokeV2TaskCreationBackend, invokeExecutionAgentsBackend,
  invokeDispatchBackend, safePythonStderrEvidence } from "../src/mcp/mailbox-tools.js";

async function withPythonShim<T>(
  mode: string,
  test: (workspaceRoot: string, pidFile: string, argsFile: string) => Promise<T>
): Promise<T> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "c2c-v2-backend-"));
  const moduleRoot = path.join(root, "python");
  fs.mkdirSync(path.join(root, "config"), { recursive: true });
  fs.writeFileSync(path.join(root, "config", "autonomous.local.json"), "{}", "utf8");
  fs.mkdirSync(path.join(moduleRoot, "orchestrator"), { recursive: true });
  fs.writeFileSync(path.join(moduleRoot, "orchestrator", "__init__.py"), "", "utf8");
  fs.writeFileSync(path.join(moduleRoot, "orchestrator", "cli_public.py"), [
    "import http.client, json, os, sys, time",
    "mode = os.environ.get('C2C_TEST_MODE')",
    "if mode == 'timeout':",
    "    open(os.environ['C2C_TEST_PID_FILE'], 'w').write(str(os.getpid()))",
    "    time.sleep(60)",
    "elif mode == 'slow_task_creation':",
    "    time.sleep(16)",
    "    request_id = sys.argv[sys.argv.index('--request-id') + 1]",
    "    print(json.dumps({'ok': True, 'command': 'web-task', 'status': 'WEB_TASK_CREATED', 'request_id': request_id, 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'round': 1, 'state': 'AWAITING_AGENT_CRITIQUE', 'details': {'receipt': {'message_id': 'test-message', 'request_id': request_id, 'type': 'TASK_REQUEST', 'round': 1}, 'already_existed': False}}))",
    "    sys.exit(0)",
    "elif mode == 'nonzero':",
    "    sys.stderr.write('private backend detail\\n')",
    "    sys.exit(7)",
    "elif mode == 'critique_claim_error':",
    "    sys.stderr.write('Traceback (most recent call last):\\n  File \\\"orchestrator/v2.py\\\", line 1, in submit_plan_critique\\n    assignments.validate_claim(...)\\norchestrator.assignments.AssignmentError: ASSIGNMENT_CLAIM_INVALID\\n')",
    "    sys.exit(1)",
    "elif mode == 'decision_agent_error':",
    "    sys.stderr.write('Traceback (most recent call last):\\n  File \\\"orchestrator/agents.py\\\", line 1, in route\\n    raise ValueError(...)\\nValueError: AGENT_NOT_FOUND\\n')",
    "    sys.exit(1)",
    "elif mode in ('lifecycle_round_error', 'lifecycle_busy_error', 'lifecycle_model_error', 'lifecycle_conflict_error', 'lifecycle_agent_required_error', 'lifecycle_decision_invalid_error'):",
    "    code = {'lifecycle_round_error': 'ROUND_MISMATCH', 'lifecycle_busy_error': 'REQUEST_BUSY', 'lifecycle_model_error': 'MODEL_UNSUPPORTED', 'lifecycle_conflict_error': 'STATE_CONFLICT', 'lifecycle_agent_required_error': 'AGENT_ID_REQUIRED', 'lifecycle_decision_invalid_error': 'PLAN_DECISION_INVALID'}[mode]",
    "    command = 'v2-critique' if 'v2-critique' in sys.argv else 'v2-decision'",
    "    print(json.dumps({'ok': False, 'command': command, 'error_code': code, 'request_id': sys.argv[sys.argv.index('--request-id') + 1], 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID']}))",
    "    sys.exit(2)",
    "elif mode == 'task_agent_error':",
    "    print(json.dumps({'ok': False, 'command': 'web-task', 'status': 'AGENT_NOT_FOUND', 'error_code': 'AGENT_NOT_FOUND', 'request_id': sys.argv[sys.argv.index('--request-id') + 1], 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID']}))",
    "    sys.exit(50)",
    "elif mode in ('conversation_busy', 'state_conflict', 'duplicate_active_request'):",
    "    code = {'conversation_busy': 'CONVERSATION_BUSY', 'state_conflict': 'STATE_CONFLICT', 'duplicate_active_request': 'DUPLICATE_ACTIVE_REQUEST'}[mode]",
    "    print(json.dumps({'ok': False, 'command': 'web-task', 'status': code, 'error_code': code, 'request_id': sys.argv[sys.argv.index('--request-id') + 1], 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID']}))",
    "    sys.exit(50)",
    "elif mode == 'malformed':",
    "    print('not-json')",
    "elif mode == 'large':",
    "    print('x' * 70000)",
    "else:",
    "    url = os.environ.get('C2C_TEST_HEALTH_URL')",
    "    if url:",
    "        from urllib.parse import urlsplit",
    "        parsed = urlsplit(url)",
    "        conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=4)",
    "        conn.request('GET', parsed.path)",
    "        response = conn.getresponse()",
    "        response.read()",
    "        if response.status != 200: sys.exit(8)",
    "    args = sys.argv",
    "    if 'dispatch-contract' in args:",
    "        print(json.dumps({'command': 'dispatch-contract', 'execution_statuses': ['accepted','already_running','already_completed','already_superseded','report_pending','terminal_error'], 'backend_statuses': ['accepted','already_running','already_completed','already_superseded','report_pending','terminal_error','rejected']}))",
    "        sys.exit(0)",
    "    if 'dispatch-backend' in args:",
    "        if mode == 'slow_dispatch': time.sleep(5.5)",
    "        request_id = args[args.index('--request-id') + 1]",
    "        round_ = int(args[args.index('--round') + 1])",
    "        status = {'dispatch_report_pending': 'report_pending', 'dispatch_terminal_error': 'terminal_error', 'dispatch_unknown': 'future_unknown'}.get(mode, 'accepted')",
    "        print(json.dumps({'status': status, 'request_id': request_id, 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'round': round_, 'worker_pid': 12345}))",
    "        sys.exit(0)",
    "    if mode == 'slow_wake' and 'v2-recover-wake' in args: time.sleep(16)",
    "    with open(os.environ['C2C_TEST_ARGS_FILE'], 'a', encoding='utf-8') as f: f.write(json.dumps(args[1:]) + '\\n')",
    "    if 'execution-agents' in args:",
    "        print(json.dumps({'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'agents': [{'id': 'codex', 'display_name': 'Codex', 'installed': True, 'available': True, 'execution_supported': True, 'model_selection_supported': False, 'models': [], 'default_model': None, 'model_discovery_supported': False, 'limitations': None}, {'id': 'cursor', 'display_name': 'Cursor', 'installed': True, 'available': True, 'execution_supported': False, 'model_selection_supported': False, 'models': [], 'default_model': None, 'model_discovery_supported': False, 'limitations': None}]}))",
    "    elif 'agents' in args:",
    "        print(json.dumps({'ok': True, 'command': 'agents', 'status': 'READY', 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'details': {'agents': [{'agent_id': 'codex-local', 'host': 'codex', 'status': 'online', 'capabilities': ['read_workspace'], 'protocol_versions': ['c2c-v2']}]}}))",
    "    else:",
    "        request_id = args[args.index('--request-id') + 1]",
    "        if any(cmd in args for cmd in ['v2-review', 'v2-recover-wake', 'v2-reroute']):",
    "            round_ = int(args[args.index('--round') + 1])",
    "            state = 'REQUESTING_REVIEW' if 'v2-recover-wake' in args else 'EXECUTING_LOCAL'",
    "            if 'v2-review' in args:",
    "                verdict = args[args.index('--verdict') + 1]",
    "                state = {'APPROVED': 'DONE', 'BLOCKED': 'BLOCKED', 'FIX_REQUIRED': 'EXECUTING_FIX'}[verdict]",
    "                round_ += verdict == 'FIX_REQUIRED'",
    "            print(json.dumps({'ok': True, 'request_id': request_id, 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'round': round_, 'state': state}))",
    "        elif 'v2-critique' in args:",
    "            print(json.dumps({'ok': True, 'command': 'v2-critique', 'status': 'CRITIQUE_RECEIVED', 'request_id': request_id, 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'round': int(args[args.index('--round') + 1]), 'state': 'AWAITING_WEB_PLAN_DECISION'}))",
    "        elif 'v2-decision' in args:",
    "            decision = args[args.index('--decision') + 1]",
    "            round_ = int(args[args.index('--round') + 1])",
    "            state = 'PLAN_APPROVED' if decision == 'approve' else ('AWAITING_AGENT_CRITIQUE' if decision == 'revise' else 'BLOCKED')",
    "            if decision == 'revise': round_ += 1",
    "            print(json.dumps({'ok': True, 'command': 'v2-decision', 'status': decision.upper(), 'request_id': request_id, 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'round': round_, 'state': state}))",
    "        elif mode == 'duplicate':",
    "            sys.stderr.write('orchestrator.state.StateError: The same task is already active as c2c_chatgpt_e2e_20260926_a1; resume that request instead of creating another\\n')",
    "            sys.exit(1)",
    "        else:",
    "            print(json.dumps({'ok': True, 'command': 'web-task', 'status': 'WEB_TASK_CREATED', 'request_id': request_id, 'workspace_id': os.environ['C2C_TEST_WORKSPACE_ID'], 'round': 1, 'state': 'AWAITING_AGENT_CRITIQUE', 'details': {'receipt': {'message_id': 'test-message', 'request_id': request_id, 'type': 'TASK_REQUEST', 'round': 1}, 'already_existed': False}}))",
  ].join("\n"), "utf8");

  const keys = ["C2C_PYTHON_EXECUTABLE", "C2C_PYTHON", "PYTHONPATH", "C2C_AUTONOMOUS_CONFIG", "C2C_CONFIG_LOCAL", "C2C_TEST_MODE",
    "C2C_TEST_WORKSPACE_ID", "C2C_TEST_HEALTH_URL", "C2C_TEST_PID_FILE", "C2C_TEST_ARGS_FILE"];
  const previous = new Map(keys.map((key) => [key, process.env[key]]));
  const pythonPath = process.env.PYTHONPATH ? moduleRoot + path.delimiter + process.env.PYTHONPATH : moduleRoot;
  process.env.C2C_AUTONOMOUS_CONFIG = path.join(root, "config", "autonomous.local.json");
  process.env.C2C_CONFIG_LOCAL = path.join(root, "config", "config.local.json");
  fs.writeFileSync(process.env.C2C_CONFIG_LOCAL, "{}", "utf8");
  process.env.C2C_PYTHON_EXECUTABLE = process.env.C2C_PYTHON_EXECUTABLE ?? process.env.C2C_PYTHON ?? "python";
  delete process.env.C2C_PYTHON;
  process.env.PYTHONPATH = pythonPath;
  process.env.C2C_TEST_MODE = mode;
  process.env.C2C_TEST_WORKSPACE_ID = "b1c422b1eab0";
  const pidFile = path.join(root, "child.pid");
  process.env.C2C_TEST_PID_FILE = pidFile;
  const argsFile = path.join(root, "child-args.jsonl");
  process.env.C2C_TEST_ARGS_FILE = argsFile;
  try {
    return await test(root, pidFile, argsFile);
  } finally {
    for (const [key, value] of previous) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function backendInput(workspaceRoot: string, request_id: string) {
  return { workspaceRoot, workspaceId: "b1c422b1eab0", request_id, task: "diagnostic fixture task" };
}

describe("V2 task creation backend command", () => {
  it("keeps an explicit autonomous config authoritative even when it does not exist", () => {
    const previous = process.env.C2C_AUTONOMOUS_CONFIG;
    const configured = path.join(os.tmpdir(), "explicit-fixture", "autonomous.local.json");
    process.env.C2C_AUTONOMOUS_CONFIG = configured;
    try {
      const command = buildV2TaskCreationCommand({ ...backendInput("C:/secondary", "c2c_explicit_cfg"), task: "fixture" }, "python");
      expect(command.configPath).toBe(configured);
    } finally {
      if (previous === undefined) delete process.env.C2C_AUTONOMOUS_CONFIG;
      else process.env.C2C_AUTONOMOUS_CONFIG = previous;
    }
  });

  it("uses only an explicitly supplied registry for shared config fallback", () => {
    const keys = ["C2C_AUTONOMOUS_CONFIG", "C2C_CONFIG_LOCAL", "PYTHONPATH", "LOCALAPPDATA", "C2C_WORKSPACE_REGISTRY"];
    const previous = new Map(keys.map((key) => [key, process.env[key]]));
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "c2c-explicit-registry-"));
    const primary = path.join(root, "primary");
    const registry = path.join(root, "registry.json");
    for (const key of keys) delete process.env[key];
    fs.writeFileSync(registry, JSON.stringify({ b1c422b1eab0: primary }), "utf8");
    try {
      const secondary = path.join(root, "secondary");
      const selected = buildV2TaskCreationCommand({ ...backendInput(secondary, "c2c_registry_cfg"),
        workspaceRegistryFile: registry }, "python");
      expect(selected.configPath).toBe(path.join(primary, "config", "autonomous.local.json"));
      const isolated = buildV2TaskCreationCommand({ ...backendInput(secondary, "c2c_registry_none") }, "python");
      expect(isolated.configPath).toBe(path.join(secondary, "config", "autonomous.local.json"));
    } finally {
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("matches the Python web-task CLI contract and preserves request/task argv", () => {
    const input = {
      workspaceRoot: path.resolve("C:/C2C-Orchestrator"),
      workspaceId: "b1c422b1eab0",
      request_id: "c2c_backend_args_test_01",
      task: "Unicode-safe task: café 🛒; --submit remains a separate argument",
    };
    const command = buildV2TaskCreationCommand(input, "python-test-runtime");

    expect(command.executable).toBe("python-test-runtime");
    expect(command.cwd).toBe(input.workspaceRoot);
    expect(command.configPath).toBe(path.join(input.workspaceRoot, "config", "autonomous.local.json"));
    expect(command.args).toEqual([
      "-m", "orchestrator.cli_public",
      "--workspace", input.workspaceRoot,
      "--state-dir", path.join(input.workspaceRoot, "runtime", "state"),
      "--config", command.configPath,
      "web-task", "--request-id", input.request_id,
      "--task", input.task, "--submit", "--json",
    ]);
    expect(command.args).not.toContain(input.workspaceId);
  });

  it("routes registered external workspaces to the shared machine autonomous config", () => {
    const prior = process.env.C2C_AUTONOMOUS_CONFIG;
    const sharedConfig = path.resolve("C:/C2C-Orchestrator/config/autonomous.local.json");
    process.env.C2C_AUTONOMOUS_CONFIG = sharedConfig;
    try {
      const input = { workspaceRoot: path.resolve("D:/history-video-factory"),
        workspaceId: "b1ede48bf1e9", request_id: "c2c_shared_config_route", task: "diagnostic",
      };
      const task = buildV2TaskCreationCommand(input, "python-test-runtime");
      const critique = buildV2LifecycleCommand({ ...input, operation: "critique",
        round: 1, agent_id: "codex-local", critique: "safe" }, "python-test-runtime");
      const review = buildV2ExecutionControlCommand({ ...input, operation: "review",
        round: 1, verdict: "APPROVED", summary: "safe" }, "python-test-runtime");
      expect(task.configPath).toBe(sharedConfig);
      expect(critique.configPath).toBe(sharedConfig);
      expect(review.configPath).toBe(sharedConfig);
      expect(task.args).toContain(sharedConfig);
      expect(critique.args).toContain(sharedConfig);
      expect(review.args).toContain(sharedConfig);
    } finally {
      if (prior === undefined) delete process.env.C2C_AUTONOMOUS_CONFIG;
      else process.env.C2C_AUTONOMOUS_CONFIG = prior;
    }
  });
  it("classifies only the terminal Python exception line, not traceback source text", () => {
    const stderr = [
      "Traceback (most recent call last):",
      '  File "orchestrator\\bridge.py", line 118, in from_config',
      '    "C2C bridge not reachable for workspace_id={workspace_id}"',
      "orchestrator.bridge.BridgeError: 401 from http://127.0.0.1:60585/admin/mailbox/latest?request_id=x: unauthorized",
    ].join("\n");
    expect(classifyPythonStderrDetail(stderr)).toBe("bridge_http_401");
    expect(safePythonStderrEvidence(stderr)).toEqual({
      http_status: "401",
      endpoint: "http://127.0.0.1:60585/admin/mailbox/latest",
    });
  });

  it("logs only safe bridge discovery evidence from a terminal exception", () => {
    const stderr = [
      "Traceback (most recent call last):",
      '  File "orchestrator\\bridge.py", line 118, in from_config',
      '    "C2C bridge not reachable for workspace_id={workspace_id}"',
      "orchestrator.bridge.BridgeError: C2C bridge not reachable for workspace_id=b1c422b1eab0 (tried 127.0.0.1:48765, scanned C:\\Users\\SUDECONS\\AppData\\Local\\codex-with-chatgpt\\runtime)",
    ].join("\n");
    expect(classifyPythonStderrDetail(stderr)).toBe("bridge_runtime_not_found");
    expect(safePythonStderrEvidence(stderr)).toEqual({
      attempted_endpoint: "127.0.0.1:48765",
      scanned_runtime_dir: "C:\\Users\\SUDECONS\\AppData\\Local\\codex-with-chatgpt\\runtime",
    });
  });

  it("classifies duplicate active tasks without logging task text", () => {
    const stderr = [
      "Traceback (most recent call last):",
      "orchestrator.state.StateError: The same task is already active as c2c_chatgpt_e2e_20260926_a1; resume that request instead of creating another",
    ].join("\n");
    expect(classifyPythonStderrDetail(stderr)).toBe("duplicate_active_request");
    expect(safePythonStderrEvidence(stderr)).toEqual({
      python_error_code: "DUPLICATE_ACTIVE_REQUEST",
      active_request_id: "c2c_chatgpt_e2e_20260926_a1",
    });
  });

  it("records a correlated safe cause for duplicate active task failures", async () => {
    await withPythonShim("duplicate", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2TaskCreationBackend(
        { ...backendInput(workspaceRoot, "c2c_duplicate_active_test"), agent_id: "codex-local" },
        (diagnostic) => diagnostics.push(diagnostic));
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(diagnostics[0]).toMatchObject({
        failure: "INVALID_JSON",
        request_id: "c2c_duplicate_active_test",
        stderr_class: "orchestrator.state.StateError",
        stderr_detail: "duplicate_active_request",
        stderr_evidence: JSON.stringify({
          python_error_code: "DUPLICATE_ACTIVE_REQUEST",
          active_request_id: "c2c_chatgpt_e2e_20260926_a1",
        }),
      });
      expect(JSON.stringify(diagnostics)).not.toContain("diagnostic fixture task");
    });
  });

  it("keeps the same Node event loop responsive while the Python backend calls bridge health", async () => {
    await withPythonShim("health", async (workspaceRoot) => {
      let healthCalls = 0;
      const server = createServer((_req, res) => {
        healthCalls += 1;
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "ok", workspaceId: "b1c422b1eab0" }));
      });
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => resolve());
      });
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("test server did not bind");
      process.env.C2C_TEST_HEALTH_URL = "http://127.0.0.1:" + address.port + "/health";
      try {
        const result = await invokeV2TaskCreationBackend(
          backendInput(workspaceRoot, "c2c_async_backend_health_test")
        );
        expect(result).toMatchObject({ ok: true, workspace_id: "b1c422b1eab0", round: 1,
          state: "AWAITING_AGENT_CRITIQUE", message_id: "test-message" });
        expect(healthCalls).toBe(1);
      } finally {
        await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      }
    });
  });

  it("maps nonzero child exit to a sanitized backend failure", async () => {
    await withPythonShim("nonzero", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2TaskCreationBackend(backendInput(workspaceRoot, "c2c_nonzero_test"),
        (diagnostic) => diagnostics.push(diagnostic));
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(JSON.stringify(result)).not.toContain("private backend detail");
      expect(JSON.stringify(diagnostics)).not.toContain("private backend detail");
    });
  });

  it("keeps malformed backend stdout sanitized", async () => {
    await withPythonShim("malformed", async (workspaceRoot) => {
      const result = await invokeV2TaskCreationBackend(backendInput(workspaceRoot, "c2c_malformed_test"));
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(JSON.stringify(result)).not.toContain("not-json");
    });
  });

  it("enforces the backend output bound", async () => {
    await withPythonShim("large", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2TaskCreationBackend(backendInput(workspaceRoot, "c2c_output_limit_test"),
        (diagnostic) => diagnostics.push(diagnostic));
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(diagnostics[0]).toMatchObject({ failure: "OUTPUT_LIMIT" });
    });
  });

  it("times out the lifecycle backend and terminates the child process", async () => {
    await withPythonShim("timeout", async (workspaceRoot, pidFile) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_timeout", round: 1, operation: "decision",
        decision: "APPROVE", plan: "safe" }, (diagnostic) => diagnostics.push(diagnostic));
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(diagnostics[0]).toMatchObject({ failure: "TIMEOUT", spawn_error_code: "ETIMEDOUT" });
      const pid = Number(fs.readFileSync(pidFile, "utf8"));
      let alive = true;
      const deadline = Date.now() + 3000;
      while (alive && Date.now() < deadline) {
        try { process.kill(pid, 0); } catch { alive = false; }
        if (alive) await new Promise((resolve) => setTimeout(resolve, 100));
      }
      expect(alive).toBe(false);
    });
  }, 70000);

  it("allows slow but valid task creation discovery to finish", async () => {
    await withPythonShim("slow_task_creation", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2TaskCreationBackend(backendInput(workspaceRoot,
        "c2c_slow_task_creation"), (diagnostic) => diagnostics.push(diagnostic));
      if (result.ok !== true) throw new Error(JSON.stringify(diagnostics));
      expect(result).toMatchObject({ ok: true, request_id: "c2c_slow_task_creation",
        workspace_id: "b1c422b1eab0", round: 1, state: "AWAITING_AGENT_CRITIQUE" });
    });
  }, 25000);

});

describe("V2 lifecycle backend command", () => {
  it("delegates critique and decision to the Python canonical CLI with exact identity", () => {
    const base = { workspaceRoot: path.resolve("C:/C2C-Orchestrator"), workspaceId: "b1c422b1eab0",
      request_id: "c2c_lifecycle_args", round: 1 };
    const previousConfig = process.env.C2C_AUTONOMOUS_CONFIG;
    process.env.C2C_AUTONOMOUS_CONFIG = path.join(base.workspaceRoot, "config", "autonomous.local.json");
    try {
    expect(buildV2LifecycleCommand({ ...base, operation: "critique", agent_id: "critic",
      critique: "safe critique", requested_model: "model-a" }, "python-test").args).toEqual([
      "-m", "orchestrator.cli_public", "--workspace", base.workspaceRoot,
      "--state-dir", path.join(base.workspaceRoot, "runtime", "state"), "--config",
      process.env.C2C_AUTONOMOUS_CONFIG!, "v2-critique",
      "--request-id", base.request_id, "--round", "1", "--agent-id", "critic",
      "--critique", "safe critique", "--requested-model", "model-a", "--json",
    ]);
    expect(buildV2LifecycleCommand({ ...base, operation: "decision", decision: "APPROVE",
      plan: "safe plan", agent_id: "codex", requested_model: "model-b" }, "python-test").args).toEqual([
      "-m", "orchestrator.cli_public", "--workspace", base.workspaceRoot,
      "--state-dir", path.join(base.workspaceRoot, "runtime", "state"), "--config",
      process.env.C2C_AUTONOMOUS_CONFIG!, "v2-decision",
      "--request-id", base.request_id, "--round", "1", "--decision", "approve",
      "--plan", "safe plan", "--agent-id", "codex", "--requested-model", "model-b", "--json",
    ]);
    } finally {
      if (previousConfig === undefined) delete process.env.C2C_AUTONOMOUS_CONFIG;
      else process.env.C2C_AUTONOMOUS_CONFIG = previousConfig;
    }
  });

  it("resolves shared backend config from the canonical registry when launcher env is missing", () => {
    const configRoot = path.resolve("C:/canonical-c2c-config-fallback");
    const registryFile = path.join(makeTmpDir("canonical-config-registry"), "workspace-registry.json");
    fs.mkdirSync(path.join(configRoot, "config"), { recursive: true });
    fs.writeFileSync(path.join(configRoot, "config", "autonomous.local.json"), "{}", "utf8");
    fs.writeFileSync(registryFile, JSON.stringify({ b1c422b1eab0: configRoot }), "utf8");
    const previous = new Map(["C2C_AUTONOMOUS_CONFIG", "C2C_CONFIG_LOCAL", "PYTHONPATH"]
      .map((key) => [key, process.env[key]]));
    delete process.env.C2C_AUTONOMOUS_CONFIG;
    delete process.env.C2C_CONFIG_LOCAL;
    delete process.env.PYTHONPATH;
    try {
      const command = buildV2TaskCreationCommand({ workspaceRoot: path.resolve("D:/officeflow"),
        workspaceId: "f7e5ea5d7a26", request_id: "canonical_registry_fallback", task: "probe",
        workspaceRegistryFile: registryFile }, "python");
      expect(command.configPath).toBe(path.join(configRoot, "config", "autonomous.local.json"));
      expect(fs.existsSync(path.join(path.resolve("D:/officeflow"), "config", "autonomous.local.json"))).toBe(false);
    } finally {
      for (const [key, value] of previous) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });

  it("accepts only correlated canonical Python lifecycle results", async () => {
    await withPythonShim("success", async (workspaceRoot) => {
      const critique = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_critique", round: 1, operation: "critique", agent_id: "critic", critique: "safe" });
      expect(critique).toMatchObject({ ok: true, state: "AWAITING_WEB_PLAN_DECISION", round: 1 });
      const decision = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_decision", round: 1, operation: "decision", decision: "APPROVE", plan: "safe", agent_id: "executor" });
      expect(decision).toMatchObject({ ok: true, state: "PLAN_APPROVED", round: 1 });
      const revised = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_revise", round: 1, operation: "decision", decision: "REVISE", plan: "safe", agent_id: "critic" });
      expect(revised).toMatchObject({ ok: true, state: "AWAITING_AGENT_CRITIQUE", round: 2 });
    });
  });

  it("logs correlated, sanitized Python domain diagnostics when critique fails", async () => {
    await withPythonShim("critique_claim_error", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const critique = "private critique text must not enter diagnostics";
      const result = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_live_critique_diag", round: 1, operation: "critique",
        agent_id: "codex-local", critique }, (diagnostic) => diagnostics.push(diagnostic));

      expect(result).toMatchObject({ ok: false, error_code: "ASSIGNMENT_CLAIM_INVALID" });
      expect(diagnostics).toHaveLength(1);
      expect(diagnostics[0]).toMatchObject({ failure: "INVALID_JSON", operation: "critique",
        request_id: "c2c_live_critique_diag", workspace_id: "b1c422b1eab0", agent_id: "codex-local",
        cwd: workspaceRoot, exit_code: 1, stdout_type: "string", stdout_bytes: 0,
        stderr_type: "string", stderr_class: "orchestrator.assignments.AssignmentError",
        python_error_code: "ASSIGNMENT_CLAIM_INVALID", node_response: "ASSIGNMENT_CLAIM_INVALID" });
      expect(String(diagnostics[0]?.argv_shape)).toContain("<redacted>");
      expect(JSON.stringify(diagnostics)).not.toContain(critique);
    });
  });

  it.each([
    ['lifecycle_round_error', 'critique', 'ROUND_MISMATCH'],
    ['lifecycle_conflict_error', 'critique', 'STATE_CONFLICT'],
    ['lifecycle_busy_error', 'decision', 'REQUEST_BUSY'],
    ['lifecycle_model_error', 'decision', 'MODEL_UNSUPPORTED'],
    ['lifecycle_agent_required_error', 'decision', 'AGENT_ID_REQUIRED'],
    ['lifecycle_decision_invalid_error', 'decision', 'PLAN_DECISION_INVALID'],
  ] as const)("preserves %s JSON domain errors on nonzero exits", async (mode, operation, code) => {
    await withPythonShim(mode, async (workspaceRoot) => {
      const input = operation === 'critique'
        ? { workspaceRoot, workspaceId: 'b1c422b1eab0', request_id: 'c2c_lifecycle_json_error', round: 1,
            operation: 'critique' as const, agent_id: 'critic', critique: 'safe' }
        : { workspaceRoot, workspaceId: 'b1c422b1eab0', request_id: 'c2c_lifecycle_json_error', round: 1,
            operation: 'decision' as const, decision: 'APPROVE' as const, plan: 'safe' };
      expect(await invokeV2LifecycleBackend(input)).toMatchObject({ ok: false, error_code: code });
    });
  });
  it("keeps unknown Python failures and malformed JSON sanitized", async () => {
    await withPythonShim("nonzero", async (workspaceRoot) => {
      const result = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_nonzero", round: 1, operation: "critique",
        agent_id: "critic", critique: "safe" });
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(JSON.stringify(result)).not.toContain("private backend detail");
    });
    await withPythonShim("malformed", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_malformed", round: 1, operation: "critique",
        agent_id: "critic", critique: "safe" }, (diagnostic) => diagnostics.push(diagnostic));
      expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      expect(diagnostics[0]).toMatchObject({ failure: "INVALID_JSON" });
      expect(JSON.stringify(result)).not.toContain("not-json");
    });
  });

  it("maps known decision routing errors without exposing Python stderr", async () => {
    await withPythonShim("decision_agent_error", async (workspaceRoot) => {
      const diagnostics: Record<string, unknown>[] = [];
      const result = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_lifecycle_agent_error", round: 1, operation: "decision",
        decision: "APPROVE", plan: "safe plan", agent_id: "codex", requested_model: "gpt-6-luna" },
        (diagnostic) => diagnostics.push(diagnostic));
      expect(result).toMatchObject({ ok: false, error_code: "AGENT_NOT_FOUND" });
      expect(result).not.toHaveProperty("stderr");
      expect(diagnostics[0]).toMatchObject({ python_error_code: "AGENT_NOT_FOUND",
        node_response: "AGENT_NOT_FOUND" });
      expect(JSON.stringify(diagnostics)).not.toContain("Traceback");
    });
  });
});

describe("V2 task creation agent identity", () => {
  it("delegates public host aliases to Python's canonical registry bootstrap", async () => {
    await withPythonShim("success", async (workspaceRoot, _pidFile, argsFile) => {
      const task = "Validation only. Do not modify files. Backend agent alias regression.";
      const base = { workspaceRoot, workspaceId: "b1c422b1eab0", task };
      const withoutAgent = await invokeV2TaskCreationBackend({ ...base, request_id: "c2c_create_no_agent" });
      expect(withoutAgent).toMatchObject({ ok: true, request_id: "c2c_create_no_agent",
        workspace_id: "b1c422b1eab0", round: 1, state: "AWAITING_AGENT_CRITIQUE" });
      const withHostAlias = await invokeV2TaskCreationBackend({ ...base,
        request_id: "c2c_create_codex_alias", agent_id: "codex" });
      expect(withHostAlias).toMatchObject({ ok: true, request_id: "c2c_create_codex_alias",
        workspace_id: "b1c422b1eab0", round: 1, state: "AWAITING_AGENT_CRITIQUE" });
      const calls = fs.readFileSync(argsFile, "utf8").trim().split(/\r?\n/)
        .map((line) => JSON.parse(line) as string[]);
      expect(calls).toHaveLength(2);
      expect(calls[0]).toContain("web-task");
      expect(calls[1]).toContain("web-task");
      expect(calls[1]).toContain("--agent-id");
      expect(calls[1][calls[1].indexOf("--agent-id") + 1]).toBe("codex");
    });
  });

  it("preserves canonical unknown-agent errors returned by Python", async () => {
    await withPythonShim("task_agent_error", async (workspaceRoot) => {
      const result = await invokeV2TaskCreationBackend({ ...backendInput(workspaceRoot,
        "c2c_unknown_task_agent"), agent_id: "missing-agent" });
      expect(result).toMatchObject({ ok: false, error_code: "AGENT_NOT_FOUND" });
    });
  });

  it.each(["conversation_busy", "state_conflict", "duplicate_active_request"] as const)(
    "propagates %s from valid JSON on a nonzero Python exit", async (mode) => {
      await withPythonShim(mode, async (workspaceRoot) => {
        const code = { conversation_busy: "CONVERSATION_BUSY", state_conflict: "STATE_CONFLICT",
          duplicate_active_request: "DUPLICATE_ACTIVE_REQUEST" }[mode];
        const result = await invokeV2TaskCreationBackend(backendInput(workspaceRoot, `c2c_${mode}_test`));
        expect(result).toEqual({ ok: false, error_code: code, message: "Python rejected V2 task creation" });
      });
    });

  it("maps child spawn failures to BACKEND_UNAVAILABLE for creation and lifecycle", async () => {
    await withPythonShim("success", async (workspaceRoot) => {
      const previous = process.env.C2C_PYTHON_EXECUTABLE;
      process.env.C2C_PYTHON_EXECUTABLE = path.join(workspaceRoot, "missing-python-executable");
      try {
        const result = await invokeV2TaskCreationBackend(backendInput(workspaceRoot, "c2c_spawn_failure"));
        expect(result).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
        const lifecycle = await invokeV2LifecycleBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
          request_id: "c2c_lifecycle_spawn_failure", round: 1, operation: "decision",
          decision: "APPROVE", plan: "safe" });
        expect(lifecycle).toMatchObject({ ok: false, error_code: "BACKEND_UNAVAILABLE" });
      } finally {
        if (previous === undefined) delete process.env.C2C_PYTHON_EXECUTABLE;
        else process.env.C2C_PYTHON_EXECUTABLE = previous;
      }
    });
  });
});

describe("V2 dispatch backend timeout", () => {
  it("allows cold Python startup and worker launch to finish", async () => {
    await withPythonShim("slow_dispatch", async (workspaceRoot) => {
      const result = invokeDispatchBackend({ workspaceRoot, workspaceId: "b1c422b1eab0",
        request_id: "c2c_dispatch_cold_start", round: 1 });
      expect(result).toMatchObject({ status: "accepted", request_id: "c2c_dispatch_cold_start",
        workspace_id: "b1c422b1eab0", round: 1 });
    });
  });
});


describe("registered workspace shared config routing", () => {
  it("injects canonical C2C root into PYTHONPATH for dispatch from another registered workspace", async () => {
    await withPythonShim("success", async (configRoot) => {
      const moduleRoot = path.join(configRoot, "python");
      fs.mkdirSync(path.join(moduleRoot, "config"), { recursive: true });
      fs.writeFileSync(path.join(moduleRoot, "config", "autonomous.local.json"), "{}", "utf8");
      const workspaceRoot = path.join(configRoot, "registered-app");
      fs.mkdirSync(workspaceRoot, { recursive: true });
      const registryFile = path.join(configRoot, "workspace-registry.json");
      fs.writeFileSync(registryFile, JSON.stringify({
        b1c422b1eab0: moduleRoot,
        "3648e2286a20": workspaceRoot,
      }), "utf8");

      const previousPythonPath = process.env.PYTHONPATH;
      const previousAutonomous = process.env.C2C_AUTONOMOUS_CONFIG;
      const previousLocal = process.env.C2C_CONFIG_LOCAL;
      const previousWorkspaceId = process.env.C2C_TEST_WORKSPACE_ID;
      delete process.env.PYTHONPATH;
      delete process.env.C2C_AUTONOMOUS_CONFIG;
      delete process.env.C2C_CONFIG_LOCAL;
      process.env.C2C_TEST_WORKSPACE_ID = "3648e2286a20";
      try {
        const result = invokeDispatchBackend({ workspaceRoot, workspaceId: "3648e2286a20",
          request_id: "c2c_registered_dispatch", round: 2, workspaceRegistryFile: registryFile });
        expect(result).toMatchObject({ status: "accepted", request_id: "c2c_registered_dispatch",
          workspace_id: "3648e2286a20", round: 2 });
      } finally {
        if (previousPythonPath === undefined) delete process.env.PYTHONPATH;
        else process.env.PYTHONPATH = previousPythonPath;
        if (previousAutonomous === undefined) delete process.env.C2C_AUTONOMOUS_CONFIG;
        else process.env.C2C_AUTONOMOUS_CONFIG = previousAutonomous;
        if (previousLocal === undefined) delete process.env.C2C_CONFIG_LOCAL;
        else process.env.C2C_CONFIG_LOCAL = previousLocal;
        if (previousWorkspaceId === undefined) delete process.env.C2C_TEST_WORKSPACE_ID;
        else process.env.C2C_TEST_WORKSPACE_ID = previousWorkspaceId;
      }
    });
  });

  it("runs execution capability discovery with the host config outside the workspace root", async () => {
    await withPythonShim("success", async (configRoot, _pidFile, argsFile) => {
      const workspaceRoot = path.join(configRoot, "registered-workspace");
      fs.mkdirSync(workspaceRoot, { recursive: true });
      const result = invokeExecutionAgentsBackend({ workspaceRoot, workspaceId: "b1c422b1eab0" });
      expect(result).toMatchObject({ workspace_id: "b1c422b1eab0" });
      expect(Array.isArray(result.agents) && result.agents.some((agent) => agent.id === "codex")).toBe(true);
      const argv = JSON.parse(fs.readFileSync(argsFile, "utf8").trim()) as string[];
      expect(argv[argv.indexOf("--config") + 1]).toBe(path.join(configRoot, "config", "autonomous.local.json"));
      expect(argv[argv.indexOf("--workspace") + 1]).toBe(workspaceRoot);
    });
  });
});
describe("V2 execution control canonical CLI", () => {
  it("forwards exact review and reroute arguments without a Python -c adapter", () => {
    const base = { workspaceRoot: path.resolve("C:/C2C-Orchestrator"), workspaceId: "b1c422b1eab0", request_id: "c2c_control_argv", round: 3 };
    const prefix = ["-m", "orchestrator.cli_public", "--workspace", base.workspaceRoot,
      "--state-dir", path.join(base.workspaceRoot, "runtime", "state"), "--config", path.join(base.workspaceRoot, "config", "autonomous.local.json")];
    expect(buildV2ExecutionControlCommand({ ...base, operation: "review", verdict: "FIX_REQUIRED", summary: "fix exact", agent_id: "fixer", requested_model: "gpt-6-sol" }, "python").args).toEqual([
      ...prefix, "v2-review", "--request-id", base.request_id, "--round", "3", "--verdict", "FIX_REQUIRED", "--summary", "fix exact", "--agent-id", "fixer", "--requested-model", "gpt-6-sol", "--json"]);
    expect(buildV2ExecutionControlCommand({ ...base, operation: "reroute", agent_id: "fixer", requested_model: "gpt-6-sol" }, "python").args).toEqual([
      ...prefix, "v2-reroute", "--request-id", base.request_id, "--round", "3", "--agent-id", "fixer", "--requested-model", "gpt-6-sol", "--json"]);
  });
  it("accepts canonical FIX_REQUIRED next round and exact-round reroute results", async () => {
    await withPythonShim("success", async (workspaceRoot) => {
      const base = { workspaceRoot, workspaceId: "b1c422b1eab0", request_id: "c2c_control_backend", round: 1 };
      expect(await invokeV2ExecutionControlBackend({ ...base, operation: "review", verdict: "FIX_REQUIRED", summary: "fix", agent_id: "fixer" })).toMatchObject({ ok: true, round: 2, state: "EXECUTING_FIX" });
      expect(await invokeV2ExecutionControlBackend({ ...base, operation: "reroute", agent_id: "fixer" })).toMatchObject({ ok: true, round: 1, state: "EXECUTING_LOCAL" });
    });
  });
});

describe("V2 execution wake canonical CLI", () => {
  it("builds and invokes the exact canonical v2-recover-wake command", async () => {
    await withPythonShim("success", async (workspaceRoot, _pidFile, argsFile) => {
      const input = { workspaceRoot, workspaceId: "b1c422b1eab0", request_id: "c2c_wake_cli_contract", round: 3 };
      const command = buildV2ExecutionWakeCommand(input, "python");
      const config = path.join(workspaceRoot, "config", "autonomous.local.json");
      expect(command.cwd).toBe(workspaceRoot);
      expect(command.configPath).toBe(config);
      expect(command.args).toEqual(["-m", "orchestrator.cli_public", "--workspace", workspaceRoot,
        "--state-dir", path.join(workspaceRoot, "runtime", "state"), "--config", config,
        "v2-recover-wake", "--request-id", input.request_id, "--round", "3", "--json"]);
      const result = await invokeV2ExecutionWakeBackend(input);
      expect(result).toMatchObject({ ok: true, request_id: input.request_id, workspace_id: input.workspaceId, round: 3,
        state: "REQUESTING_REVIEW" });
      const argv = JSON.parse(fs.readFileSync(argsFile, "utf8").trim()) as string[];
      expect(argv).toEqual(command.args.slice(2));
      expect(result).not.toHaveProperty("operation_status");
    });
  });
});


