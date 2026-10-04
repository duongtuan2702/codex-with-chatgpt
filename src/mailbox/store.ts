/**
 * C2C Mailbox â€” Control-plane message store.
 *
 * Architecture:
 *   - One JSON file per workspace: <stateDir>/mailbox/<workspaceId>.json
 *   - Cross-process serialization via an exclusive lock file
 *   - Atomic writes via temp-file + rename
 *   - Workspace-scoped: workspace ID enforced at every operation
 *   - Idempotent: duplicate identical messages return existing; duplicate
 *     conflicting messages are rejected
 *   - Retention: configurable (default 7 days), max 30 days
 *   - No secrets ever stored
 *
 * Message types (control-plane only â€” no source write, no shell, no DB):
 *   PLAN_REQUEST      â€” Cursor â†’ mailbox (user intent)
 *   PLAN_RESPONSE     â€” ChatGPT â†’ mailbox (plan)
 *   EXECUTION_REPORT  â€” Cursor â†’ mailbox (evidence)
 *   REVIEW_RESPONSE   â€” ChatGPT â†’ mailbox (verdict)
 *   ERROR             â€” system â†’ mailbox (error info)
 */

import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { ensureDir, getStateDir, readJsonIfExists, writeSecureJson } from "../config/paths.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export const MESSAGE_TYPES = [
  "PLAN_REQUEST", "PLAN_RESPONSE", "EXECUTION_REPORT", "REVIEW_RESPONSE",
  "TASK_REQUEST", "PLAN_CRITIQUE", "PLAN_DECISION", "ERROR",
] as const;

export type MessageType = (typeof MESSAGE_TYPES)[number];

export function isMessageType(value: unknown): value is MessageType {
  return typeof value === "string" && (MESSAGE_TYPES as readonly string[]).includes(value);
}

export function isPayloadRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value) &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}

export interface MailboxMessage {
  message_id: string;
  request_id: string;
  workspace_id: string;
  type: MessageType;
  round: number;
  created_at: string;
  payload: Record<string, unknown>;
  /** ISO timestamp; messages older than retention_days are eligible for cleanup */
  expires_at: string;
}

export interface RequestMeta {
  request_id: string;
  workspace_id: string;
  current_round: number;
  created_at: string;
  /** Most recent message_id per type, for quick lookups */
  latest: Partial<Record<MessageType, string>>;
}

export interface MailboxStore {
  messages: Record<string, MailboxMessage>;
  requests: Record<string, RequestMeta>;
  retention_days: number;
}

export interface SubmitResult {
  ok: true;
  message: MailboxMessage;
  is_duplicate: boolean;
}

export interface SubmitError {
  ok: false;
  error: "duplicate_conflict" | "invalid_payload" | "workspace_mismatch" | "parse_error";
  message: string;
  existing_message_id?: string;
}

export type SubmitMessageResult = SubmitResult | SubmitError;

export interface ListOptions {
  request_id?: string;
  type?: MessageType;
  /** Only messages of this round or newer */
  min_round?: number;
  /** Max results (default 20) */
  limit?: number;
}

export interface ListResult {
  messages: MailboxMessage[];
  total: number;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAILBOX_SUBDIR = "mailbox";
const MAX_MESSAGE_BYTES = 512 * 1024;       // 512 KB per message
const MAX_TOTAL_MESSAGES = 2000;             // per workspace
const DEFAULT_RETENTION_DAYS = 7;
const MAX_RETENTION_DAYS = 30;
const LOCK_TIMEOUT_MS = 10_000;
const LOCK_POLL_MS = 20;
const INVALID_LOCK_STALE_MS = 500;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export function mailboxDir(): string {
  return ensureDir(path.join(getStateDir(), MAILBOX_SUBDIR));
}

export function mailboxFile(workspaceId: string): string {
  return path.join(mailboxDir(), `${workspaceId}.json`);
}

function newId(): string {
  return randomBytes(12).toString("hex");
}

function expiresAt(retentionDays: number): string {
  const d = new Date();
  d.setDate(d.getDate() + Math.min(retentionDays, MAX_RETENTION_DAYS));
  return d.toISOString();
}

/** Workspace-scoped uniqueness key: (workspace, request, round, type) */
function uniquenessKey(msg: Pick<MailboxMessage, "workspace_id" | "request_id" | "round" | "type">): string {
  return `${msg.workspace_id}::${msg.request_id}::${msg.round}::${msg.type}`;
}

interface MailboxLockOwner {
  pid: number;
  nonce: string;
  created_at: string;
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ESRCH") return false;
    // EPERM means the process exists but cannot be signalled. Treat unknown
    // platform errors conservatively as alive so a live lock is never stolen.
    return true;
  }
}

function pauseSync(ms: number): void {
  // Store APIs are intentionally synchronous. Atomics.wait provides a bounded
  // cross-platform sleep without spinning while another process owns the lock.
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

// ---------------------------------------------------------------------------
// MailboxData â€” the persisted shape (separate from the class)
// ---------------------------------------------------------------------------

export interface MailboxData {
  messages: Record<string, MailboxMessage>;
  requests: Record<string, RequestMeta>;
  retention_days: number;
}

// ---------------------------------------------------------------------------
// MailboxStore class
// ---------------------------------------------------------------------------

export class MailboxStore {
  private _data: MailboxData;
  private readonly file: string;
  private readonly lockFile: string;
  private readonly workspaceId: string;

  constructor(workspaceId: string) {
    this.workspaceId = workspaceId;
    this.file = mailboxFile(workspaceId);
    this.lockFile = `${this.file}.lock`;
    this._data = this._load();
  }

  private _load(): MailboxData {
    const raw = readJsonIfExists<MailboxData>(this.file);
    if (raw) {
      // Validate shape (MailboxData only â€” class shape lives on the live instance)
      if (
        typeof raw === "object" &&
        raw !== null &&
        "messages" in raw &&
        "requests" in raw
      ) {
        return raw as MailboxData;
      }
    }
    return { messages: {}, requests: {}, retention_days: DEFAULT_RETENTION_DAYS };
  }

  /** Persist through a same-directory temp file; never fall back to in-place writes. */
  private _atomicSave(): void {
    const tmp = this.file + `.tmp.${newId()}`;
    try {
      writeSecureJson(tmp, this._data);
      fs.renameSync(tmp, this.file);
    } catch (err) {
      try { fs.unlinkSync(tmp); } catch { /* ignore */ }
      throw err;
    }
  }

  private _refresh(): void {
    this._data = this._load();
  }

  private _readLockOwner(): { owner: MailboxLockOwner | null; mtimeMs: number; raw: string } | null {
    try {
      const stat = fs.statSync(this.lockFile);
      const raw = fs.readFileSync(this.lockFile, "utf8");
      let owner: MailboxLockOwner | null = null;
      try {
        const parsed = JSON.parse(raw) as Partial<MailboxLockOwner>;
        if (Number.isSafeInteger(parsed.pid) && (parsed.pid ?? 0) > 0 &&
            typeof parsed.nonce === "string" && typeof parsed.created_at === "string") {
          owner = parsed as MailboxLockOwner;
        }
      } catch { /* an interrupted lock creation is handled by its age */ }
      return { owner, mtimeMs: stat.mtimeMs, raw };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
      throw error;
    }
  }

  /**
   * Reclaim a dead owner lock. Re-check its identity immediately before unlink
   * so a contender never intentionally removes a lock that has changed hands.
   */
  private _reclaimStaleLock(): boolean {
    const observed = this._readLockOwner();
    if (!observed) return true;
    const ownerIsDead = observed.owner !== null && !processIsAlive(observed.owner.pid);
    const incompleteIsOld = observed.owner === null && Date.now() - observed.mtimeMs >= INVALID_LOCK_STALE_MS;
    if (!ownerIsDead && !incompleteIsOld) return false;

    const current = this._readLockOwner();
    if (!current || current.raw !== observed.raw || current.mtimeMs !== observed.mtimeMs) return false;
    try {
      fs.unlinkSync(this.lockFile);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return true;
      throw error;
    }
  }

  private _acquireLock(): () => void {
    ensureDir(path.dirname(this.lockFile));
    const deadline = Date.now() + LOCK_TIMEOUT_MS;
    const owner: MailboxLockOwner = {
      pid: process.pid,
      nonce: newId(),
      created_at: new Date().toISOString(),
    };

    while (true) {
      let fd: number | undefined;
      try {
        fd = fs.openSync(this.lockFile, "wx", 0o600);
        fs.writeFileSync(fd, JSON.stringify(owner), "utf8");
        fs.fsyncSync(fd);
        fs.closeSync(fd);
        fd = undefined;
        return () => {
          const current = this._readLockOwner();
          if (!current?.owner || current.owner.nonce !== owner.nonce || current.owner.pid !== owner.pid) return;
          try { fs.unlinkSync(this.lockFile); } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
          }
        };
      } catch (error) {
        if (fd !== undefined) {
          try { fs.closeSync(fd); } catch { /* best effort */ }
          // We created this path exclusively and still own the open handle.
          // Remove an incomplete lock before another contender can acquire it.
          try { fs.unlinkSync(this.lockFile); } catch { /* best effort */ }
        }
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
        if (this._reclaimStaleLock()) continue;
        if (Date.now() >= deadline) {
          throw new Error(`Mailbox lock acquisition timed out for workspace ${this.workspaceId}`);
        }
        pauseSync(Math.min(LOCK_POLL_MS, Math.max(1, deadline - Date.now())));
      }
    }
  }

  private _withLock<T>(operation: () => T): T {
    const release = this._acquireLock();
    try {
      // Refresh only after acquiring the cross-process lock. This is the
      // read-modify-write boundary that prevents updates based on stale state.
      this._refresh();
      return operation();
    } finally {
      release();
    }
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Submit a message. Idempotent for identical payloads; rejects conflicting
   * duplicates (same key, different payload).
   */
  submit(
    workspaceId: string,
    requestId: string,
    type: MessageType,
    round: number,
    payload: Record<string, unknown>
  ): SubmitMessageResult {
    return this._withLock(() => this._submitLocked(workspaceId, requestId, type, round, payload));
  }

  private _submitLocked(
    workspaceId: string,
    requestId: string,
    type: MessageType,
    round: number,
    payload: Record<string, unknown>
  ): SubmitMessageResult {
    if (workspaceId !== this.workspaceId) {
      return { ok: false, error: "workspace_mismatch", message: "workspace_id does not match store scope" };
    }
    if (!isMessageType(type)) {
      return { ok: false, error: "invalid_payload", message: "Unsupported mailbox message type" };
    }
    if (!Number.isInteger(round) || round < 0 || (round === 0 && type !== "ERROR")) {
      return { ok: false, error: "invalid_payload", message: "round must be a non-negative integer; round 0 is only valid for ERROR" };
    }
    if (!isPayloadRecord(payload)) {
      return { ok: false, error: "invalid_payload", message: "payload must be a plain JSON object" };
    }

    // Size guard
    const payloadBytes = Buffer.byteLength(JSON.stringify(payload), "utf8");
    if (payloadBytes > MAX_MESSAGE_BYTES) {
      return {
        ok: false,
        error: "invalid_payload",
        message: `Payload exceeds ${MAX_MESSAGE_BYTES} bytes (got ${payloadBytes})`,
      };
    }

    // Check for duplicate
    const uk = uniquenessKey({ workspace_id: workspaceId, request_id: requestId, round, type });
    const existing = Object.values(this._data.messages).find(
      (m) => uniquenessKey(m) === uk
    );

    if (existing) {
      // Idempotent: identical payload â†’ return existing
      const existingPayloadBytes = Buffer.byteLength(JSON.stringify(existing.payload), "utf8");
      if (payloadBytes === existingPayloadBytes &&
          JSON.stringify(payload) === JSON.stringify(existing.payload)) {
        return { ok: true, message: existing, is_duplicate: true };
      }
      // Conflict: same key, different payload
      return {
        ok: false,
        error: "duplicate_conflict",
        message: `A ${type} message for round ${round} already exists with a different payload`,
        existing_message_id: existing.message_id,
      };
    }

    // New message
    const messageId = newId();
    const now = new Date().toISOString();
    const msg: MailboxMessage = {
      message_id: messageId,
      request_id: requestId,
      workspace_id: workspaceId,
      type,
      round,
      created_at: now,
      payload,
      expires_at: expiresAt(this._data.retention_days),
    };

    this._data.messages[messageId] = msg;

    // Update request meta
    if (!this._data.requests[requestId]) {
      this._data.requests[requestId] = {
        request_id: requestId,
        workspace_id: workspaceId,
        current_round: round,
        created_at: now,
        latest: {},
      };
    } else {
      const meta = this._data.requests[requestId];
      meta.current_round = Math.max(meta.current_round, round);
      meta.latest[type] = messageId;
    }
    // Always update latest for this type
    this._data.requests[requestId].latest[type] = messageId;

    // Cleanup if over limit
    this._enforceLimits();

    this._atomicSave();
    return { ok: true, message: msg, is_duplicate: false };
  }

  /**
   * List pending messages (not expired).
   */
  list(workspaceId: string, opts: ListOptions = {}): ListResult {
    // Reads use the atomic file snapshot and do not retain a process-local view.
    this._refresh();
    if (workspaceId !== this.workspaceId) {
      return { messages: [], total: 0 };
    }

    const limit = opts.limit ?? 20;
    const now = new Date().toISOString();

    let msgs = Object.values(this._data.messages).filter((m) => {
      if (m.workspace_id !== workspaceId) return false;
      if (m.expires_at < now) return false;
      if (opts.request_id && m.request_id !== opts.request_id) return false;
      if (opts.type && m.type !== opts.type) return false;
      if (opts.min_round !== undefined && m.round < opts.min_round) return false;
      return true;
    });

    // Sort newest first
    msgs.sort((a, b) => b.created_at.localeCompare(a.created_at));

    return { messages: msgs.slice(0, limit), total: msgs.length };
  }

  /**
   * Get a specific message by ID.
   */
  get(workspaceId: string, messageId: string): MailboxMessage | null {
    this._refresh();
    if (workspaceId !== this.workspaceId) return null;
    const m = this._data.messages[messageId];
    if (!m || m.expires_at < new Date().toISOString()) return null;
    return m;
  }

  /**
   * Get the latest message of a given type for a request/round.
   */
  getLatest(
    workspaceId: string,
    requestId: string,
    type: MessageType,
    minRound?: number
  ): MailboxMessage | null {
    this._refresh();
    if (workspaceId !== this.workspaceId) return null;
    const meta = this._data.requests[requestId];
    if (!meta) return null;
    const msgId = meta.latest[type];
    if (!msgId) return null;
    const msg = this._data.messages[msgId];
    if (!msg) return null;
    if (msg.expires_at < new Date().toISOString()) return null;
    if (minRound !== undefined && msg.round < minRound) return null;
    return msg;
  }

  /** Get request metadata */
  getRequest(workspaceId: string, requestId: string): RequestMeta | null {
    this._refresh();
    if (workspaceId !== this.workspaceId) return null;
    return this._data.requests[requestId] ?? null;
  }

  /** Enforce retention + message count limits */
  cleanup(): { removed: number } {
    return this._withLock(() => this._cleanupLocked());
  }

  /** Prune messages strictly before an explicit created_at cutoff. */
  pruneBeforeCreatedAt(cutoff: string, options: { dryRun?: boolean } = {}): {
    removed: number; remaining: number; backupPath?: string;
  } {
    if (!Number.isFinite(Date.parse(cutoff))) throw new Error("cutoff must be a valid timestamp");
    return this._withLock(() => {
      const removedIds = Object.values(this._data.messages)
        .filter((message) => message.created_at < cutoff)
        .map((message) => message.message_id);
      if (options.dryRun) return { removed: removedIds.length, remaining: Object.keys(this._data.messages).length - removedIds.length };
      if (removedIds.length === 0) return { removed: 0, remaining: Object.keys(this._data.messages).length };

      const backupPath = `${this.file}.backup.${new Date().toISOString().replace(/[:.]/g, "-")}`;
      // Preserve the exact original bytes before any mutation.
      fs.copyFileSync(this.file, backupPath, fs.constants.COPYFILE_EXCL);
      const removed = new Set(removedIds);
      for (const id of removed) delete this._data.messages[id];

      // Request metadata is an index over retained messages. Derive it from
      // those messages so any request with a post-cutoff message survives.
      const requests: Record<string, RequestMeta> = {};
      for (const message of Object.values(this._data.messages)) {
        let meta = requests[message.request_id];
        if (!meta) {
          meta = requests[message.request_id] = {
            request_id: message.request_id,
            workspace_id: message.workspace_id,
            current_round: message.round,
            created_at: message.created_at,
            latest: {},
          };
        }
        meta.current_round = Math.max(meta.current_round, message.round);
        if (message.created_at < meta.created_at) meta.created_at = message.created_at;
        const previousId = meta.latest[message.type];
        const previous = previousId ? this._data.messages[previousId] : undefined;
        if (!previous || message.created_at >= previous.created_at) meta.latest[message.type] = message.message_id;
      }
      this._data.requests = requests;
      this._atomicSave();
      return { removed: removedIds.length, remaining: Object.keys(this._data.messages).length, backupPath };
    });
  }

  private _cleanupLocked(): { removed: number } {
    const now = new Date().toISOString();
    const before = Object.keys(this._data.messages).length;

    for (const [id, msg] of Object.entries(this._data.messages)) {
      if (msg.expires_at < now) {
        delete this._data.messages[id];
      }
    }

    // Also expire old request metadata if all its messages are gone
    for (const [rid, meta] of Object.entries(this._data.requests)) {
      const hasMessages = Object.values(meta.latest).some(
        (mid) => mid !== undefined && !!this._data.messages[mid]
      );
      if (!hasMessages) {
        delete this._data.requests[rid];
      }
    }

    const removed = before - Object.keys(this._data.messages).length;
    if (removed > 0) this._atomicSave();
    return { removed };
  }

  /** Force-save current state */
  flush(): void {
    this._withLock(() => this._atomicSave());
  }

  private _enforceLimits(): void {
    if (Object.keys(this._data.messages).length <= MAX_TOTAL_MESSAGES) return;

    // Remove oldest expired first, then oldest by created_at
    const entries = Object.values(this._data.messages)
      .sort((a, b) => a.created_at.localeCompare(b.created_at));

    while (entries.length > MAX_TOTAL_MESSAGES) {
      const oldest = entries.shift();
      if (oldest) {
        delete this._data.messages[oldest.message_id];
        // Clean up request meta
        const meta = this._data.requests[oldest.request_id];
        if (meta) {
          delete meta.latest[oldest.type];
          const hasAny = Object.values(meta.latest).some(Boolean);
          if (!hasAny) delete this._data.requests[oldest.request_id];
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/** One lightweight store instance per workspaceId; each operation refreshes disk state. */
const _cache = new Map<string, MailboxStore>();

export function getMailboxStore(workspaceId: string): MailboxStore {
  if (!_cache.has(workspaceId)) {
    _cache.set(workspaceId, new MailboxStore(workspaceId));
  }
  return _cache.get(workspaceId)!;
}

/** Drop cache entry (useful for testing) */
export function clearMailboxCache(workspaceId?: string): void {
  if (workspaceId) _cache.delete(workspaceId);
  else _cache.clear();
}
