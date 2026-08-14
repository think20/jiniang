#!/usr/bin/env bun
/**
 * qqbot-gateway.ts — QQ 消息网关（独立于 Claude Code 运行）
 *
 * 功能：
 * - 连接 QQ WebSocket 网关，接收私聊和群聊消息 → pending_messages.json
 * - 监控 replies.json → 发送回复 → QQ API（支持文字+富媒体）
 * - 完全独立运行，不依赖 MCP / Claude Code
 *
 * 配合 check-pending.ts 使用：
 *   gateway 负责消息收+发，check-pending 负责检测+唤醒 Claude
 *   Claude 处理后将回复写入 replies.json，gateway 自动发送
 */

import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { join, resolve, isAbsolute, basename } from "node:path";

// ── 配置 ──
const CONFIG = {
  appId: process.env.QQ_APPID ?? "",
  appSecret: process.env.QQ_APP_SECRET ?? "",
  allowedUsers: (process.env.QQ_ALLOWED_USERS ?? "").split(",").filter(Boolean),
  sandbox: process.env.QQ_SANDBOX === "true",
};

const QQ_API_BASE = CONFIG.sandbox
  ? "https://sandbox.api.sgroup.qq.com"
  : "https://api.sgroup.qq.com";

const TEMP_DIR = join(import.meta.dir, ".tmp");
if (!existsSync(TEMP_DIR)) mkdirSync(TEMP_DIR, { recursive: true });

// ── 消息类型 ──
interface QueuedMessage {
  type: "c2c" | "group";
  user_openid: string;
  group_openid: string;
  msg_id: string;
  content: string;
  image_paths: string[];
  timestamp: string;
}

interface ReplyEntry {
  type: "c2c" | "group";
  user_openid?: string;
  group_openid?: string;
  msg_id?: string;
  text: string;
  image_url?: string;
  file_url?: string;
  file_type?: 1 | 2 | 3 | 4;
}

interface QQMessageData {
  id: string;
  content: string;
  timestamp: string;
  author: {
    id: string;
    username?: string;
    member_openid?: string;
    user_openid?: string;
  };
  group_id?: string;
  group_openid?: string;
  attachments?: QQAttachment[];
}

interface QQAttachment {
  content_type: string;
  url: string;
  filename: string;
}

// ── 日志 ──
function log(...args: any[]) {
  console.error(`[gateway ${new Date().toISOString()}]`, ...args);
}

// ═══════════════════════════════════════════
// 消息队列（接收）
// ═══════════════════════════════════════════
const QUEUE_FILE = join(TEMP_DIR, "pending_messages.json");
const REPLY_FILE = join(TEMP_DIR, "replies.json");

function readQueue(): QueuedMessage[] {
  try {
    if (!existsSync(QUEUE_FILE)) return [];
    return JSON.parse(readFileSync(QUEUE_FILE, "utf-8")) as QueuedMessage[];
  } catch { return []; }
}

function writeQueue(queue: QueuedMessage[]): void {
  Bun.write(QUEUE_FILE, JSON.stringify(queue, null, 2));
}

// ── 去重（防止重连后 WebSocket 重放消息）──
const RECENT_IDS = new Set<string>();
const RECENT_IDS_MAX = 200; // 最多保留200条，防止内存泄漏

function isDuplicate(msgId: string): boolean {
  return RECENT_IDS.has(msgId);
}

function markSeen(msgId: string): void {
  if (RECENT_IDS.size >= RECENT_IDS_MAX) {
    // 移除最早的一半
    const toDelete = Math.floor(RECENT_IDS_MAX / 2);
    let i = 0;
    for (const key of RECENT_IDS) {
      if (i++ >= toDelete) break;
      RECENT_IDS.delete(key);
    }
  }
  RECENT_IDS.add(msgId);
}

function appendToQueue(msg: QueuedMessage): void {
  if (isDuplicate(msg.msg_id)) {
    log(`⏭ Skipped duplicate: ${msg.msg_id.slice(0, 20)}...`);
    return;
  }
  markSeen(msg.msg_id);
  const queue = readQueue();
  queue.push(msg);
  writeQueue(queue);
  log(`📥 Queued: ${msg.type} from ${msg.user_openid.slice(0, 12)}... → "${msg.content.slice(0, 40)}"`);
}

// ═══════════════════════════════════════════
// 回复队列（发送）
// ═══════════════════════════════════════════
function readReplies(): ReplyEntry[] {
  try {
    if (!existsSync(REPLY_FILE)) return [];
    const raw = readFileSync(REPLY_FILE, "utf-8").trim();
    if (!raw) return [];
    return JSON.parse(raw) as ReplyEntry[];
  } catch { return []; }
}

function writeReplies(replies: ReplyEntry[]): void {
  Bun.write(REPLY_FILE, JSON.stringify(replies, null, 2));
}

function popReply(): ReplyEntry | null {
  const replies = readReplies();
  if (replies.length === 0) return null;
  const entry = replies.shift()!;
  writeReplies(replies);
  return entry;
}

// ═══════════════════════════════════════════
// Access Token
// ═══════════════════════════════════════════
let accessToken = "";
let tokenExpiry = 0;

async function getAccessToken(): Promise<string> {
  if (Date.now() < tokenExpiry - 60_000) return accessToken;
  if (!CONFIG.appId || !CONFIG.appSecret) {
    throw new Error("QQ_APPID or QQ_APP_SECRET env var is missing");
  }
  const res = await fetch("https://bots.qq.com/app/getAppAccessToken", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ appId: CONFIG.appId, clientSecret: CONFIG.appSecret }),
  });
  const data = (await res.json()) as { access_token: string; expires_in: number };
  if (!data.access_token) {
    throw new Error(`Failed to get access token: ${JSON.stringify(data)}`);
  }
  accessToken = data.access_token;
  tokenExpiry = Date.now() + data.expires_in * 1000;
  log("Access token refreshed");
  return accessToken;
}

function authHeaders(): Record<string, string> {
  return {
    Authorization: `QQBot ${accessToken}`,
    "Content-Type": "application/json",
    "X-Union-Appid": CONFIG.appId,
  };
}

// ═══════════════════════════════════════════
// 富媒体上传
// ═══════════════════════════════════════════
async function uploadMedia(
  target: { type: "c2c"; userOpenid: string } | { type: "group"; groupOpenid: string },
  fileType: number,
  urlOrPath: string,
): Promise<string | null> {
  await getAccessToken();
  const endpoint =
    target.type === "c2c"
      ? `${QQ_API_BASE}/v2/users/${target.userOpenid}/files`
      : `${QQ_API_BASE}/v2/groups/${target.groupOpenid}/files`;

  const isLocalFile = !urlOrPath.startsWith("http://") && !urlOrPath.startsWith("https://");
  let body: any;
  if (isLocalFile) {
    const filePath = isAbsolute(urlOrPath) ? urlOrPath : resolve(import.meta.dir, urlOrPath);
    if (!existsSync(filePath)) {
      log(`⚠ Local file not found: ${filePath}`);
      return null;
    }
    const fileData = readFileSync(filePath).toString("base64");
    body = { file_type: fileType, file_data: fileData };
    if (fileType === 4) body.file_name = basename(filePath);
  } else {
    body = { file_type: fileType, url: urlOrPath };
    if (fileType === 4) {
      try { const name = basename(new URL(urlOrPath).pathname); if (name) body.file_name = name; } catch {}
    }
  }

  const res = await fetch(endpoint, {
    method: "POST", headers: authHeaders(), body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    log(`⚠ Upload failed ${res.status}: ${errText.slice(0, 200)}`);
    return null;
  }
  return ((await res.json()) as any).file_info as string;
}

// ═══════════════════════════════════════════
// 发送消息
// ═══════════════════════════════════════════
let msgSeqCounter = 1;

async function sendC2CMessage(
  userOpenid: string, content: string, msgId?: string,
  mediaUrl?: string, mediaType?: number,
): Promise<boolean> {
  await getAccessToken();
  const body: any = { content, msg_type: mediaUrl ? 7 : 0, msg_id: msgId, msg_seq: msgSeqCounter++ };
  if (mediaUrl && mediaType) {
    const fileInfo = await uploadMedia({ type: "c2c", userOpenid }, mediaType, mediaUrl);
    if (fileInfo) body.media = { file_info: fileInfo };
    else { log("⚠ Media upload failed, sending text only"); }
  }
  const res = await fetch(`${QQ_API_BASE}/v2/users/${userOpenid}/messages`, {
    method: "POST", headers: authHeaders(), body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    log(`⚠ C2C send failed ${res.status}: ${errText.slice(0, 200)}`);
    return false;
  }
  return true;
}

async function sendGroupMessage(
  groupOpenid: string, content: string, msgId?: string,
  mediaUrl?: string, mediaType?: number,
): Promise<boolean> {
  await getAccessToken();
  const body: any = { content, msg_type: mediaUrl ? 7 : 0, msg_id: msgId, msg_seq: msgSeqCounter++ };
  if (mediaUrl && mediaType) {
    const fileInfo = await uploadMedia({ type: "group", groupOpenid }, mediaType, mediaUrl);
    if (fileInfo) body.media = { file_info: fileInfo };
    else { log("⚠ Media upload failed, sending text only"); }
  }
  const res = await fetch(`${QQ_API_BASE}/v2/groups/${groupOpenid}/messages`, {
    method: "POST", headers: authHeaders(), body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    log(`⚠ Group send failed ${res.status}: ${errText.slice(0, 200)}`);
    return false;
  }
  return true;
}

// ═══════════════════════════════════════════
// 回复轮询器（每 2 秒批量处理 replies.json）
// ═══════════════════════════════════════════
async function replyPoller() {
  const all = readReplies();
  if (all.length === 0) return;

  // 一次性读取全部，立即清空文件（避免竞态丢失条目）
  writeReplies([]);

  for (const entry of all) {
    const { type, user_openid, group_openid, msg_id, text, file_url, file_type } = entry;

    if (type === "c2c" && user_openid) {
      const ok = await sendC2CMessage(user_openid, text, msg_id, file_url, file_type);
      log(ok ? `📤 C2C → ${user_openid.slice(0, 12)}...` : `❌ C2C failed`);
    } else if (type === "group" && group_openid) {
      const ok = await sendGroupMessage(group_openid, text, msg_id, file_url, file_type);
      log(ok ? `📤 Group → ${group_openid.slice(0, 12)}...` : `❌ Group failed`);
    } else {
      log("⚠ Invalid reply, skipped");
    }
  }
}

// ═══════════════════════════════════════════
// 附件处理（接收）
// ═══════════════════════════════════════════
function isImage(att: QQAttachment): boolean {
  return /^image\//.test(att.content_type) ||
    /\.(jpg|jpeg|png|gif|webp|bmp)$/i.test(att.filename);
}

async function downloadFile(url: string, filename: string): Promise<string> {
  const res = await fetch(url);
  const buf = Buffer.from(await res.arrayBuffer());
  const path = join(TEMP_DIR, `${Date.now()}_${filename}`);
  await Bun.write(path, buf);
  return path;
}

async function processAttachments(attachments: QQAttachment[]) {
  const result: { textParts: string[]; imageFilePaths: string[] } = { textParts: [], imageFilePaths: [] };
  for (const att of attachments) {
    try {
      if (isImage(att)) {
        const localPath = await downloadFile(att.url, att.filename);
        result.imageFilePaths.push(localPath);
        result.textParts.push(`[图片: ${att.filename}, 已保存到 ${localPath}]`);
      } else {
        const localPath = await downloadFile(att.url, att.filename);
        result.textParts.push(`[文件: ${att.filename}, 已保存到 ${localPath}]`);
      }
    } catch (e: any) {
      result.textParts.push(`[附件处理失败: ${att.filename}]`);
    }
  }
  return result;
}

// ═══════════════════════════════════════════
// Gateway URL
// ═══════════════════════════════════════════
async function getGatewayUrl(): Promise<string> {
  const token = await getAccessToken();
  const res = await fetch(`${QQ_API_BASE}/gateway`, {
    headers: { Authorization: `QQBot ${token}` },
  });
  const data = (await res.json()) as { url: string };
  if (!data.url) {
    throw new Error(`Failed to get gateway URL (HTTP ${res.status}): ${JSON.stringify(data)}`);
  }
  return data.url;
}

// ═══════════════════════════════════════════
// WebSocket
// ═══════════════════════════════════════════
const OP = {
  DISPATCH: 0, HEARTBEAT: 1, IDENTIFY: 2,
  RESUME: 6, RECONNECT: 7, INVALID_SESSION: 9,
  HELLO: 10, HEARTBEAT_ACK: 11,
} as const;

const INTENTS = (1 << 25) | (1 << 30) | (1 << 12) | (1 << 26);

let ws: WebSocket | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let lastSeq: number | null = null;
let sessionId: string | null = null;
let resumeGatewayUrl: string | null = null;
let reconnectAttempt = 0;
let connectionGen = 0; // 代际计数器：防止旧连接事件污染新连接状态
let isReconnecting = false; // 防止 RECONNECT/close 双重调用
const BACKOFF = [10000, 15000, 20000, 25000, 30000, 60000];

function startHeartbeat(intervalMs: number) {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(() => {
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: OP.HEARTBEAT, d: lastSeq }));
    }
  }, intervalMs);
}

function stopHeartbeat() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
}

async function sendIdentify() {
  const token = await getAccessToken();
  ws?.send(JSON.stringify({
    op: OP.IDENTIFY, d: { token: `QQBot ${token}`, intents: INTENTS, shard: [0, 1] },
  }));
}

async function sendResume() {
  const token = await getAccessToken();
  ws?.send(JSON.stringify({
    op: OP.RESUME, d: { token: `QQBot ${token}`, session_id: sessionId, seq: lastSeq },
  }));
}

async function handleMessage(data: QQMessageData, eventType: string) {
  let msgType: "c2c" | "group" = "c2c";
  let userOpenid = "";
  let groupOpenid = "";
  let content = (data.content ?? "").trim();

  if (eventType === "C2C_MESSAGE_CREATE") {
    msgType = "c2c";
    userOpenid = data.author?.user_openid ?? data.author?.id ?? "";
  } else if (eventType === "GROUP_AT_MESSAGE_CREATE") {
    msgType = "group";
    groupOpenid = data.group_openid ?? data.group_id ?? "";
    userOpenid = data.author?.member_openid ?? data.author?.id ?? "";
    content = content.replace(/^<@!\d+>\s*/, "").trim();
  }

  if (CONFIG.allowedUsers.length > 0 && !CONFIG.allowedUsers.includes(userOpenid)) {
    return;
  }

  const media = data.attachments?.length
    ? await processAttachments(data.attachments)
    : { textParts: [], imageFilePaths: [] };

  const fullContent = [content, ...media.textParts].filter(Boolean).join("\n");

  appendToQueue({
    type: msgType, user_openid: userOpenid, group_openid: groupOpenid,
    msg_id: data.id, content: fullContent,
    image_paths: media.imageFilePaths, timestamp: data.timestamp,
  });
}

async function connectGateway(resuming = false) {
  const url = resuming && resumeGatewayUrl ? resumeGatewayUrl : await getGatewayUrl();
  const myGen = ++connectionGen; // 当前连接的代际
  log(`Connecting WS (gen ${myGen}):`, url, resuming ? "(resuming)" : "(fresh)");

  ws = new WebSocket(url);

  ws.addEventListener("open", () => log(`✅ WS connected (gen ${myGen})`));

  ws.addEventListener("message", async (event) => {
    // 旧连接的滞留事件，忽略
    if (connectionGen !== myGen) return;
    let payload: any;
    try {
      payload = JSON.parse(typeof event.data === "string" ? event.data : Buffer.from(event.data as ArrayBuffer).toString());
    } catch { return; }

    switch (payload.op) {
      case OP.HELLO:
        startHeartbeat(payload.d?.heartbeat_interval ?? 45000);
        if (resuming && sessionId) await sendResume();
        else await sendIdentify();
        break;

      case OP.DISPATCH: {
        lastSeq = payload.s;
        const t = payload.t as string;
        if (t === "READY") {
          sessionId = payload.d.session_id;
          resumeGatewayUrl = payload.d.resume_gateway_url ?? null;
          // 只在首次连接或非重连时清零计数器
          if (reconnectAttempt === 0) reconnectAttempt = 0; // no-op, keep attempt
          log("✅ READY");
        } else if (t === "RESUMED") {
          // 重连成功后不清零计数器，保持退避升级趋势
          // reconnectAttempt 只在连续失败时递增，成功后保持当前值
          log("✅ RESUMED");
        } else if (t === "C2C_MESSAGE_CREATE" || t === "GROUP_AT_MESSAGE_CREATE") {
          await handleMessage(payload.d as QQMessageData, t);
        }
        break;
      }

      case OP.HEARTBEAT:
        ws?.send(JSON.stringify({ op: OP.HEARTBEAT, d: lastSeq }));
        break;

      case OP.HEARTBEAT_ACK: break;

      case OP.RECONNECT:
        log("Server requested reconnect → clearing session, will fresh IDENTIFY");
        stopHeartbeat();
        sessionId = null;  // 服务端要求重连=session已失效, 清掉做fresh identify
        lastSeq = null;
        isReconnecting = true;  // 防止 close handler 重复触发 reconnect
        ws?.close();
        await reconnect(false);  // false = fresh IDENTIFY, 不RESUME
        break;

      case OP.INVALID_SESSION: {
        const canResume = payload.d === true;
        stopHeartbeat(); ws?.close();
        if (!canResume) { sessionId = null; lastSeq = null; }
        await new Promise(r => setTimeout(r, 1000 + Math.random() * 4000));
        await reconnect(canResume && !!sessionId);
        break;
      }
    }
  });

  ws.addEventListener("close", async (event) => {
    log("WebSocket closed, code:", event.code);
    stopHeartbeat();
    // 如果已被 RECONNECT 接管，跳过（避免双重重连）
    if (isReconnecting) {
      log("Reconnect already in progress by RECONNECT handler, skipping close handler");
      return;
    }
    // 1006=异常断开（网络/超时），服务端可能已清理session，不可resume
    // 4006=无效session, 4008/4009=可resume, [1000-4000)中排除1006
    const resumable = [4008, 4009].includes(event.code) ||
      (event.code >= 1000 && event.code < 4000 && event.code !== 1006);
    const canResume = resumable && !!sessionId;
    // 非resumable：清除sessionId并重新identify
    if (!resumable) {
      const reason = event.code === 1006 ? "abnormal close (1006)" : `code ${event.code}`;
      log(`Non-resumable ${reason}, clearing session → will re-identify`);
      sessionId = null;
      lastSeq = null;
    }
    const fatal = [4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915].includes(event.code);
    if (fatal) { log("❌ Fatal close, exiting"); process.exit(1); }
    await reconnect(canResume);
  });

  ws.addEventListener("error", (event) => log("WebSocket error:", event));
}

async function reconnect(resuming: boolean) {
  const delay = BACKOFF[Math.min(reconnectAttempt, BACKOFF.length - 1)];
  reconnectAttempt++;
  // 连续重连超过阈值：放弃RESUME，模拟手动重启（fresh IDENTIFY）
  const MAX_RESUME_ATTEMPTS = 4;
  if (resuming && reconnectAttempt > MAX_RESUME_ATTEMPTS) {
    log(`Too many resumes (${reconnectAttempt}), forcing fresh IDENTIFY like manual restart`);
    sessionId = null;
    lastSeq = null;
    resuming = false;
  }
  log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempt}, resume=${resuming})`);
  await new Promise(r => setTimeout(r, delay));
  // 确保旧WebSocket完全清理后再建新连接（防止session竞态）
  if (ws) {
    try { ws.close(); } catch {}
    ws = null;
  }
  try { await getAccessToken(); } catch (e) { log("Token refresh failed:", e); }
  await connectGateway(resuming);
  isReconnecting = false; // 重连完成，复位标记
}

// ═══════════════════════════════════════════
// 启动
// ═══════════════════════════════════════════
log("╔══════════════════════════════════════╗");
log("║   QQBot Gateway (Full Duplex)        ║");
log("╠══════════════════════════════════════╣");
log(`║   📥 In:   WebSocket → queue         ║`);
log(`║   📤 Out:  replies.json → QQ API     ║`);
log("╚══════════════════════════════════════╝");

// 启动回复轮询（每 2 秒检查一次）
const replyTimer = setInterval(replyPoller, 2000);
log("Reply poller active (every 2s)");

// ── 优雅退出 ──
function gracefulShutdown() {
  log("Shutting down...");
  clearInterval(replyTimer);
  stopHeartbeat();
  if (ws?.readyState === WebSocket.OPEN) ws.close();
  process.exit(0);
}
process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);

// 连接 WebSocket
await connectGateway();
