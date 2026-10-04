import { afterEach, describe, expect, it } from "vitest";
import { spawn } from "node:child_process";
import path from "node:path";
import { startBridge } from "../src/bridge/server.js";
import {
  findBridgeObservation,
  findLiveBridge,
  readRuntimeState,
  writeRuntimeState,
  type RuntimeState,
} from "../src/bridge/runtime.js";
import { ensureBridge } from "../src/process/daemon.js";
import { SERVICE_NAME, VERSION } from "../src/version.js";
import { Workspace } from "../src/workspace/manager.js";
import { cleanup, isolateStateDir, makeTmpDir, write } from "./helpers.js";

function stubRuntime(workspaceId: string, workspaceRoot: string, pid: number, port: number): RuntimeState {
  return {
    service: SERVICE_NAME,
    version: VERSION,
    workspaceId,
    workspaceRoot,
    pid,
    port,
    adminToken: "test-token",
    publicUrl: null,
    startedAt: new Date().toISOString(),
  };
}

describe("findBridgeObservation", () => {
  const dirs: string[] = [];

  afterEach(() => {
    for (const dir of dirs) cleanup(dir);
    dirs.length = 0;
    delete process.env.C2C_STATE_DIR;
  });

  it("treats a missing runtime file as stopped", async () => {
    dirs.push(isolateStateDir());
    const root = makeTmpDir("obs-missing");
    dirs.push(root);
    write(root, "a.txt", "a");
    const workspace = new Workspace(root);
    const observation = await findBridgeObservation(workspace.id);
    expect(observation.state).toBe("stopped");
    if (observation.state === "stopped") expect(observation.reason).toBe("runtime_missing");
    expect(await findLiveBridge(workspace.id)).toBeNull();
  });

  it("treats a dead pid plus a failed probe as stopped", async () => {
    dirs.push(isolateStateDir());
    const root = makeTmpDir("obs-dead");
    dirs.push(root);
    write(root, "a.txt", "a");
    const workspace = new Workspace(root);
    writeRuntimeState(stubRuntime(workspace.id, workspace.root, 999_999_999, 1));
    const observation = await findBridgeObservation(workspace.id);
    expect(observation.state).toBe("stopped");
    if (observation.state === "stopped") expect(observation.reason).toBe("pid_missing");
    expect(await findLiveBridge(workspace.id)).toBeNull();
  });

  it("does not treat a live pid plus a failed probe as stopped", async () => {
    dirs.push(isolateStateDir());
    const root = makeTmpDir("obs-unknown");
    dirs.push(root);
    write(root, "a.txt", "a");
    const workspace = new Workspace(root);
    const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      stdio: "ignore",
      detached: true,
    });
    child.unref();
    try {
      if (!child.pid) throw new Error("failed to spawn helper");
      writeRuntimeState(stubRuntime(workspace.id, workspace.root, child.pid, 1));
      const observation = await findBridgeObservation(workspace.id);
      expect(observation.state).toBe("unknown");
      if (observation.state === "unknown") expect(observation.reason).toBe("probe_failed");
      expect(await findLiveBridge(workspace.id)).toBeNull();
      await expect(ensureBridge(root)).rejects.toThrow(/uncertain/);
    } finally {
      if (child.pid) {
        try {
          process.kill(child.pid, "SIGKILL");
        } catch {
          /* ignore */
        }
      }
    }
  });

  it.runIf(process.platform === "win32")("treats a reused PID owned by a non-Node process as stopped", async () => {
    dirs.push(isolateStateDir());
    const root = makeTmpDir("obs-pid-reused");
    dirs.push(root);
    write(root, "a.txt", "a");
    const workspace = new Workspace(root);
    // Keep a known non-Node process alive while tasklist verifies its image.
    const helper = spawn("powershell.exe", ["-NoProfile", "-Command", "Start-Sleep -Seconds 20"], {
      stdio: "ignore",
    });
    try {
      if (!helper.pid) throw new Error("failed to spawn helper");
      writeRuntimeState(stubRuntime(workspace.id, workspace.root, helper.pid, 1));
      const observation = await findBridgeObservation(workspace.id);
      expect(observation.state).toBe("stopped");
      if (observation.state === "stopped") expect(observation.reason).toBe("pid_reused");
    } finally {
      if (helper.pid) {
        try {
          process.kill(helper.pid, "SIGKILL");
        } catch {
          /* ignore */
        }
      }
    }
  });

  it("reports healthy when the local bridge answers", async () => {
    dirs.push(isolateStateDir());
    const root = makeTmpDir("obs-live");
    dirs.push(root);
    write(root, "a.txt", "a");
    const auth = path.join(makeTmpDir("obs-auth"), "store.json");
    dirs.push(path.dirname(auth));
    const unregisteredRoot = makeTmpDir("obs-unregistered");
    dirs.push(unregisteredRoot);
    const unregistered = new Workspace(unregisteredRoot);
    const bridge = await startBridge({
      workspaceRoot: root,
      port: 0,
      persistRuntime: true,
      authStoreFile: auth,
    });
    try {
      const observation = await findBridgeObservation(bridge.workspace.id);
      expect(observation.state).toBe("healthy");
      expect(await findLiveBridge(bridge.workspace.id)).not.toBeNull();
      expect(readRuntimeState(bridge.workspace.id)).toMatchObject({
        workspaceId: bridge.workspace.id,
        workspaceRoot: bridge.workspace.root,
        port: bridge.port,
        pid: process.pid,
        adminToken: bridge.adminToken,
      });
      expect(readRuntimeState(unregistered.id)).toBeNull();
    } finally {
      await bridge.close();
    }
    expect(readRuntimeState(bridge.workspace.id)).toBeNull();
  });

  it("publishes and clears a truthful runtime record for every registered workspace", async () => {
    dirs.push(isolateStateDir());
    const primaryRoot = makeTmpDir("runtime-primary");
    const registeredRoot = makeTmpDir("runtime-registered");
    dirs.push(primaryRoot, registeredRoot);
    write(primaryRoot, "a.txt", "primary");
    write(registeredRoot, "b.txt", "registered");
    const primary = new Workspace(primaryRoot);
    const registered = new Workspace(registeredRoot);
    const auth = path.join(makeTmpDir("runtime-auth"), "store.json");
    dirs.push(path.dirname(auth));
    const bridge = await startBridge({
      workspaceRoot: primaryRoot,
      workspaceRoots: { [registered.id]: registeredRoot },
      port: 0,
      persistRuntime: true,
      authStoreFile: auth,
    });
    try {
      const primaryState = readRuntimeState(primary.id);
      const registeredState = readRuntimeState(registered.id);
      expect(primaryState).not.toBeNull();
      expect(registeredState).not.toBeNull();
      expect(primaryState).toMatchObject({
        workspaceId: primary.id,
        workspaceRoot: primary.root,
        pid: process.pid,
        port: bridge.port,
        adminToken: bridge.adminToken,
        publicUrl: null,
      });
      expect(registeredState).toEqual({ ...primaryState!, workspaceId: registered.id, workspaceRoot: registered.root });
    } finally {
      await bridge.close();
    }
    expect(readRuntimeState(primary.id)).toBeNull();
    expect(readRuntimeState(registered.id)).toBeNull();
  });
});
