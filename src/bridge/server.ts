import express, { type Request, type Response, type NextFunction } from "express";
import type { Server } from "node:http";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { Workspace } from "../workspace/manager.js";
import { AuthStore } from "../auth/store.js";
import { createOAuthRouter } from "../auth/oauth.js";
import { bearerAuth } from "../auth/middleware.js";
import { PairingManager } from "../pairing/manager.js";
import { createMcpServer } from "../mcp/server.js";
import { createMcpHttpHandler } from "../mcp/http.js";
import { CloudflaredQuickTunnel } from "../tunnel/cloudflared.js";
import { CloudflaredNamedTunnel } from "../tunnel/cloudflared-named.js";
import type { TunnelProvider } from "../tunnel/provider.js";
import { namedTunnelBinding, readTunnelState } from "../tunnel/state.js";
import { Logger, nullLogger } from "../logger/index.js";
import { DEFAULT_HOST, DEFAULT_PORT } from "../config/paths.js";
import { SERVICE_NAME, VERSION } from "../version.js";
import { writeRuntimeState, clearRuntimeState, type RuntimeState } from "./runtime.js";
import { getMailboxStore, isMessageType, isPayloadRecord } from "../mailbox/store.js";
import type { DispatchBackendInvoker, ExecutionAgentsBackendInvoker, V2TaskCreationBackendInvoker,
  V2LifecycleBackendInvoker } from "../mcp/mailbox-tools.js";
import type { V2ExecutionControlInvoker } from "../mcp/mailbox-tools.js";
import type { V2ExecutionWakeInvoker } from "../mcp/mailbox-tools.js";

function tunnelForWorkspace(workspaceId: string, logger: Logger): TunnelProvider {
  const binding = namedTunnelBinding(readTunnelState(workspaceId));
  if (binding) {
    return new CloudflaredNamedTunnel({
      tunnelName: binding.tunnelName,
      hostname: binding.hostname,
      logger,
    });
  }
  return new CloudflaredQuickTunnel(logger);
}

export interface BridgeOptions {
  workspaceRoot: string;
  /** Fixed machine-local workspace roots keyed by their derived C2C IDs. */
  workspaceRoots?: Record<string, string>;
  /** Optional canonical registry re-read for each authenticated MCP request. */
  workspaceRegistryFile?: string;
  port?: number;
  host?: string;
  logger?: Logger;
  tunnelProvider?: TunnelProvider;
  /** Persist runtime state file (disable in tests). */
  persistRuntime?: boolean;
  authStoreFile?: string;
  pairingTtlMs?: number;
  accessTokenTtlMs?: number;
  /** Test seam; production uses the bounded Python backend adapter. */
  dispatchBackend?: DispatchBackendInvoker;
  executionAgentsBackend?: ExecutionAgentsBackendInvoker;
  v2TaskCreationBackend?: V2TaskCreationBackendInvoker;
  v2LifecycleBackend?: V2LifecycleBackendInvoker;
  v2ExecutionControlBackend?: V2ExecutionControlInvoker;
  v2ExecutionWakeBackend?: V2ExecutionWakeInvoker;
}

export interface Bridge {
  workspace: Workspace;
  port: number;
  host: string;
  adminToken: string;
  authStore: AuthStore;
  pairing: PairingManager;
  tunnel: TunnelProvider;
  getPublicBaseUrl(): string | null;
  localBaseUrl(): string;
  close(): Promise<void>;
}

/**
 * Listen on the preferred port; on EADDRINUSE fall back to an ephemeral port.
 */
function listen(app: express.Express, host: string, preferredPort: number): Promise<{ server: Server; port: number }> {
  return new Promise((resolve, reject) => {
    const tryListen = (port: number, allowFallback: boolean): void => {
      const server = app.listen(port, host);
      server.once("listening", () => {
        const address = server.address();
        const actual = typeof address === "object" && address ? address.port : port;
        resolve({ server, port: actual });
      });
      server.once("error", (error: NodeJS.ErrnoException) => {
        if (error.code === "EADDRINUSE" && allowFallback) {
          tryListen(0, false);
        } else {
          reject(error);
        }
      });
    };
    tryListen(preferredPort, preferredPort !== 0);
  });
}

export async function startBridge(opts: BridgeOptions): Promise<Bridge> {
  const logger = opts.logger ?? nullLogger;
  const workspace = new Workspace(opts.workspaceRoot);
  const workspaces = new Map<string, Workspace>([[workspace.id, workspace]]);
  for (const [configuredId, root] of Object.entries(opts.workspaceRoots ?? {})) {
    const registered = new Workspace(root);
    if (configuredId !== registered.id) {
      throw new Error(`Workspace registry ID does not match its canonical root: ${configuredId}`);
    }
    const prior = workspaces.get(registered.id);
    if (prior && prior.root !== registered.root) {
      throw new Error(`Workspace registry contains conflicting roots for ID ${registered.id}`);
    }
    workspaces.set(registered.id, registered);
  }
  const refreshWorkspaceRegistry = (): void => {
    if (!opts.workspaceRegistryFile) return;
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(opts.workspaceRegistryFile, "utf8").replace(/^\uFEFF/, ""));
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
      const refreshed = new Map<string, Workspace>([[workspace.id, workspace]]);
      for (const [id, root] of Object.entries(parsed)) {
        if (typeof root !== "string" || !root.trim()) continue;
        const candidate = new Workspace(root);
        if (candidate.id !== id) continue;
        refreshed.set(id, candidate);
      }
      workspaces.clear();
      for (const [id, registered] of refreshed) workspaces.set(id, registered);
    } catch {
      // Keep the last validated registry snapshot during transient file writes.
    }
  };
  const host = opts.host ?? DEFAULT_HOST;
  if (host !== "127.0.0.1" && host !== "::1" && host !== "localhost") {
    throw new Error("The bridge only binds to loopback addresses. Public exposure goes through the tunnel.");
  }

  const authStore = new AuthStore(workspace.id, { file: opts.authStoreFile });
  const pairing = new PairingManager(workspace.id, { ttlMs: opts.pairingTtlMs });
  const tunnel = opts.tunnelProvider ?? tunnelForWorkspace(workspace.id, logger);
  const adminToken = `c2c_admin_${randomBytes(24).toString("base64url")}`;

  let publicBaseUrl: string | null = null;

  const app = express();
  app.set("trust proxy", true);
  app.disable("x-powered-by");

  const getBaseUrl = (req: Request): string => {
    if (publicBaseUrl) return publicBaseUrl;
    const proto = req.protocol;
    const hostHeader = req.get("host") ?? `${host}:${port}`;
    return `${proto}://${hostHeader}`;
  };

  // ---- Health (public but minimal) ---------------------------------------

  app.get("/health", (_req, res) => {
    res.json({ service: SERVICE_NAME, version: VERSION, workspaceId: workspace.id, status: "ok" });
  });

  // ---- OAuth + discovery ---------------------------------------------------

  app.use(
    createOAuthRouter({
      store: authStore,
      pairing,
      workspaceName: workspace.name,
      getBaseUrl,
      logger,
    })
  );

  // JSON body parser: applies globally so admin POST endpoints get req.body.
  // MCP handler re-parses via its own express.json() call — stream is already
  // consumed on second read so this has no effect on MCP; the handler will
  // use the req.body already set by the MCP-layer express.json() call.
  app.use(express.json({ limit: "8mb" }));

  // ---- MCP endpoint (bearer-protected) --------------------------------------
  // NOTE: this must come AFTER app.use(express.json()) so the MCP handler's
  // own express.json() call runs first (and sets req.body) before this route
  // handler is invoked. The MCP transport uses req.body if already-parsed.
  app.all(
    "/mcp",
    bearerAuth({ store: authStore, workspaceId: workspace.id, getBaseUrl, logger }),
    (req: Request, res: Response) => {
      refreshWorkspaceRegistry();
      const body = req.body as { method?: unknown; params?: { name?: unknown; arguments?: unknown } } | undefined;
      const args = body?.method === "tools/call" && body.params?.name
        ? body.params.arguments as Record<string, unknown> | undefined
        : undefined;
      const requestedId = args?.workspace_id;
      if (requestedId !== undefined && (typeof requestedId !== "string" || !workspaces.has(requestedId))) {
        res.status(403).json({ error: "unknown_workspace_id", message: "Workspace ID is not registered on this endpoint" });
        return;
      }
      const selected = requestedId ? workspaces.get(requestedId)! : workspace;
      const mcpHandler = createMcpHttpHandler(() => createMcpServer({ workspace: selected, logger,
        mailboxId: selected.id, dispatchBackend: opts.dispatchBackend,
        executionAgentsBackend: opts.executionAgentsBackend,
        v2TaskCreationBackend: opts.v2TaskCreationBackend,
        v2LifecycleBackend: opts.v2LifecycleBackend,
        v2ExecutionControlBackend: opts.v2ExecutionControlBackend,
        v2ExecutionWakeBackend: opts.v2ExecutionWakeBackend,
        workspaceRegistryFile: opts.workspaceRegistryFile,
        registeredRequestWorkspaceLookup: (requestId, selectedWorkspaceId) => {
          if (!/^[A-Za-z0-9_-]{8,128}$/.test(requestId)) return null;
          for (const candidate of workspaces.values()) {
            if (candidate.id === selectedWorkspaceId) continue;
            const stateFile = path.join(candidate.root, "runtime", "state", candidate.id, `${requestId}.json`);
            let raw: unknown;
            try { raw = JSON.parse(fs.readFileSync(stateFile, "utf8")); }
            catch { continue; }
            if (raw && typeof raw === "object" && !Array.isArray(raw)) {
              const state = raw as Record<string, unknown>;
              if (state.request_id === requestId && state.workspace_id === candidate.id) return candidate.id;
            }
          }
          return null;
        } }), logger);
      void mcpHandler(req, res);
    }
  );

  // ---- Admin API (loopback + admin token only; used by the CLI/Skill) --------
  const adminGuard = (req: Request, res: Response, next: NextFunction): void => {
    // Defense in depth: reject anything that arrived through a proxy/tunnel.
    const remote = req.socket.remoteAddress ?? "";
    const isLoopback = remote === "127.0.0.1" || remote === "::1" || remote === "::ffff:127.0.0.1";
    const viaProxy = Boolean(req.headers["cf-connecting-ip"] || req.headers["x-forwarded-for"]);
    const header = req.headers.authorization ?? "";
    const token = header.toLowerCase().startsWith("bearer ") ? header.slice(7).trim() : "";
    if (!isLoopback || viaProxy || token !== adminToken) {
      res.status(404).end(); // do not advertise the admin surface
      return;
    }
    refreshWorkspaceRegistry();
    next();
  };

  app.post("/admin/pairing", adminGuard, (_req, res) => {
    const session = pairing.create();
    logger.info("Created pairing session");
    res.json({ code: session.code, expiresAt: session.expiresAt });
  });

  app.get("/admin/info", adminGuard, (_req, res) => {
    res.json({
      service: SERVICE_NAME,
      version: VERSION,
      workspaceId: workspace.id,
      workspaceName: workspace.name,
      workspaceRoot: workspace.root,
      port,
      publicUrl: publicBaseUrl,
      tunnel: tunnel.status(),
      tokenCount: authStore.tokenCount(),
      pairingActive: pairing.hasActiveSession(),
      pid: process.pid,
      startedAt,
    });
  });

  app.post("/admin/tunnel/start", adminGuard, (_req, res) => {
    tunnel
      .start(port)
      .then((url) => {
        publicBaseUrl = url;
        persistRuntime();
        res.json({ url });
      })
      .catch((error: Error) => {
        logger.error(`Tunnel start failed: ${error.message}`);
        res.status(500).json({ error: "tunnel_failed", message: error.message });
      });
  });

  app.post("/admin/tunnel/stop", adminGuard, (_req, res) => {
    void tunnel.stop().then(() => {
      publicBaseUrl = null;
      persistRuntime();
      res.json({ stopped: true });
    });
  });

  app.post("/admin/revoke-all", adminGuard, (_req, res) => {
    const count = authStore.revokeAll();
    pairing.invalidateAll();
    logger.info(`Revoked all tokens (${count})`);
    res.json({ revoked: count });
  });

  // ---- C2C Mailbox admin API (loopback + admin token only) ---------------

  // Admin mailbox routing shares the same server-side registry as /mcp.
  // Clients can select a registered ID, but can never provide a filesystem root.
  const selectMailboxWorkspace = (requestedId: unknown, res: Response): Workspace | undefined => {
    if (requestedId === undefined) return workspace;
    if (typeof requestedId !== "string") {
      res.status(403).json({ error: "unknown_workspace_id", message: "Workspace ID is not registered on this endpoint" });
      return undefined;
    }
    const selected = workspaces.get(requestedId);
    if (!selected) {
      res.status(403).json({ error: "unknown_workspace_id", message: "Workspace ID is not registered on this endpoint" });
      return undefined;
    }
    return selected;
  };

  app.post("/admin/mailbox/submit", adminGuard, (req: Request, res: Response) => {
    const body = req.body as Record<string, unknown> | undefined;
    if (!body || typeof body !== "object") {
      res.status(400).json({ error: "INVALID_BODY", message: "Expected JSON body" });
      return;
    }

    const selected = selectMailboxWorkspace(body.workspace_id, res);
    if (!selected) return;

    const {
      type,
      request_id,
      round,
      payload,
    } = body as Record<string, unknown>;

    if (!isMessageType(type)) {
      res.status(400).json({
        error: "INVALID_TYPE",
        message: "type must be a supported mailbox message type",
      });
      return;
    }
    if (!request_id || typeof request_id !== "string") {
      res.status(400).json({ error: "INVALID_REQUEST_ID", message: "request_id is required" });
      return;
    }
    if (typeof round !== "number" || !Number.isInteger(round) || round < 0 || (round === 0 && type !== "ERROR")) {
      res.status(400).json({ error: "INVALID_ROUND", message: "round must be an integer >= 1; ERROR may use round 0" });
      return;
    }
    if (!isPayloadRecord(payload)) {
      res.status(400).json({ error: "INVALID_PAYLOAD", message: "payload is required and must be an object" });
      return;
    }
    if (Object.prototype.hasOwnProperty.call(payload, "workspace_id") && payload.workspace_id !== selected.id) {
      res.status(403).json({ error: "workspace_mismatch", message: "payload workspace_id must match the selected workspace" });
      return;
    }

    const store = getMailboxStore(selected.id);
    const result = store.submit(
      selected.id,
      String(request_id),
      type, round, payload
    );

    if (!result.ok) {
      const status = result.error === "workspace_mismatch" ? 403 : 409;
      res.status(status).json({ error: result.error, message: result.message, existing_message_id: result.existing_message_id });
      return;
    }

    logger.info(`mailbox submit: ${result.message.type} ${result.message.message_id} workspace=${selected.id}`);
    res.json({
      message_id: result.message.message_id,
      request_id: result.message.request_id,
      type: result.message.type,
      round: result.message.round,
      created_at: result.message.created_at,
      is_duplicate: result.is_duplicate,
    });
  });

  app.get("/admin/mailbox/list", adminGuard, (req: Request, res: Response) => {
    const { request_id, type, min_round, limit, workspace_id } = req.query as Record<string, unknown>;
    const selected = selectMailboxWorkspace(workspace_id, res);
    if (!selected) return;

    if (type !== undefined && !isMessageType(type)) {
      res.status(400).json({ error: "INVALID_TYPE", message: "type must be a supported mailbox message type" });
      return;
    }
    const parsedMinRound = min_round === undefined ? undefined : Number(min_round);
    if (parsedMinRound !== undefined && (!Number.isInteger(parsedMinRound) || parsedMinRound < 0)) {
      res.status(400).json({ error: "INVALID_MIN_ROUND", message: "min_round must be a non-negative integer" });
      return;
    }
    const parsedLimit = limit === undefined ? 20 : Number(limit);
    if (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) {
      res.status(400).json({ error: "INVALID_LIMIT", message: "limit must be an integer from 1 to 100" });
      return;
    }

    const store = getMailboxStore(selected.id);
    const result = store.list(selected.id, {
      request_id: typeof request_id === "string" ? request_id : undefined,
      type: typeof type === "string" ? type : undefined,
      min_round: parsedMinRound,
      limit: parsedLimit,
    });

    res.json({ messages: result.messages, total: result.total });
  });

  app.get("/admin/mailbox/get", adminGuard, (req: Request, res: Response) => {
    const { message_id, workspace_id } = req.query as Record<string, unknown>;
    const selected = selectMailboxWorkspace(workspace_id, res);
    if (!selected) return;
    if (!message_id) {
      res.status(400).json({ error: "MISSING_MESSAGE_ID", message: "message_id query param required" });
      return;
    }
    const store = getMailboxStore(selected.id);
    const msg = store.get(selected.id, String(message_id));
    if (!msg) {
      res.status(404).json({ error: "NOT_FOUND", message: "Message not found or expired" });
      return;
    }
    res.json(msg);
  });

  app.get("/admin/mailbox/latest", adminGuard, (req: Request, res: Response) => {
    const { request_id, type, min_round, workspace_id } = req.query as Record<string, unknown>;
    const selected = selectMailboxWorkspace(workspace_id, res);
    if (!selected) return;
    if (!request_id || !type) {
      res.status(400).json({ error: "MISSING_PARAMS", message: "request_id and type are required" });
      return;
    }
    if (!isMessageType(type)) {
      res.status(400).json({ error: "INVALID_TYPE", message: "type must be a supported mailbox message type" });
      return;
    }
    const parsedMinRound = min_round === undefined ? undefined : Number(min_round);
    if (parsedMinRound !== undefined && (!Number.isInteger(parsedMinRound) || parsedMinRound < 0)) {
      res.status(400).json({ error: "INVALID_MIN_ROUND", message: "min_round must be a non-negative integer" });
      return;
    }
    const store = getMailboxStore(selected.id);
    const msg = store.getLatest(
      selected.id,
      String(request_id),
      type, parsedMinRound
    );
    if (!msg) {
      res.status(404).json({ error: "NOT_FOUND", message: "No matching message found" });
      return;
    }
    res.json(msg);
  });

  app.post("/admin/shutdown", adminGuard, (_req, res) => {
    res.json({ shuttingDown: true });
    setTimeout(() => {
      void shutdown().then(() => process.exit(0));
    }, 100);
  });

  const { server, port } = await listen(app, host, opts.port ?? DEFAULT_PORT);
  const startedAt = new Date().toISOString();
  logger.info(`Bridge listening on ${host}:${port} for workspace ${workspace.name} (${workspace.id})`);

  const persistRuntime = (): void => {
    if (opts.persistRuntime === false) return;
    for (const servedWorkspace of workspaces.values()) {
      const state: RuntimeState = {
        service: SERVICE_NAME,
        version: VERSION,
        workspaceId: servedWorkspace.id,
        workspaceRoot: servedWorkspace.root,
        pid: process.pid,
        port,
        adminToken,
        publicUrl: publicBaseUrl,
        startedAt,
      };
      writeRuntimeState(state);
    }
  };
  persistRuntime();

  let closed = false;
  const shutdown = async (): Promise<void> => {
    if (closed) return;
    closed = true;
    await tunnel.stop().catch(() => undefined);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    if (opts.persistRuntime !== false) {
      for (const servedWorkspace of workspaces.values()) clearRuntimeState(servedWorkspace.id);
    }
    logger.info("Bridge stopped");
  };

  return {
    workspace,
    port,
    host,
    adminToken,
    authStore,
    pairing,
    tunnel,
    getPublicBaseUrl: () => publicBaseUrl,
    localBaseUrl: () => `http://${host}:${port}`,
    close: shutdown,
  };
}
