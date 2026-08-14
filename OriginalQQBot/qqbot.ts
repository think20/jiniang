#!/usr/bin/env bun
/**
 * QQBot Channel for Claude Code
 * 基于腾讯 QQ 官方 Bot API v2 (WebSocket 模式)
 *
 * 功能:
 *  - 文字消息收发
 *  - 图片接收 (下载到本地, Claude 多模态识别)
 *  - 语音接收 (下载 → STT → 文字)
 *  - 文件接收 (下载到本地, 由 Claude 自行读取)
 *  - 富媒体回复 (文字 + 图片/语音/视频/文件, 支持本地路径和URL)
 *  - 内联媒体标签 (<qqimg>/<qqvoice>/<qqvideo>/<qqfile>)
 *  - 远程权限审批
 *
 * 用法: claude --dangerously-load-development-channels server:qqbot
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import { existsSync, mkdirSync, readFileSync } from "fs";
import { join, resolve, isAbsolute, basename } from "path";
import { $ } from "bun";

// ─────────────────────────────────────────────
// 配置
// ─────────────────────────────────────────────
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

// ── 消息队列 (供 Stop Hook 轮询) ──
const MSG_QUEUE_FILE = join(TEMP_DIR, "pending_messages.json");
interface QueuedMessage {
  type: "c2c" | "group";
  user_openid: string;
  group_openid: string;
  msg_id: string;
  content: string;
  image_paths: string[];
  timestamp: string;
}

function readQueue(): QueuedMessage[] {
  try {
    if (!existsSync(MSG_QUEUE_FILE)) return [];
    const raw = readFileSync(MSG_QUEUE_FILE, "utf-8");
    return JSON.parse(raw) as QueuedMessage[];
  } catch {
    return [];
  }
}

function writeQueue(queue: QueuedMessage[]): void {
  Bun.write(MSG_QUEUE_FILE, JSON.stringify(queue, null, 2));
}

function appendToQueue(msg: QueuedMessage): void {
  const queue = readQueue();
  queue.push(msg);
  writeQueue(queue);
  log("Message appended to queue, total pending:", queue.length);
}

export function popQueue(): QueuedMessage | null {
  const queue = readQueue();
  if (queue.length === 0) return null;
  const msg = queue.shift()!;
  writeQueue(queue);
  return msg;
}

let msgSeqCounter = 1;

// ─────────────────────────────────────────────
// 类型定义
// ─────────────────────────────────────────────
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
  message_scene?: {
    ext?: string[];
  };
}

interface QQAttachment {
  content_type: string;
  url: string;
  filename: string;
  size?: number;
}

// ─────────────────────────────────────────────
// 引用消息存储 (msgIdx → 消息内容)
// ─────────────────────────────────────────────
const REF_STORE = new Map<string, string>();
const REF_STORE_MAX = 500;

function setRefEntry(msgIdx: string, content: string) {
  if (REF_STORE.size >= REF_STORE_MAX) {
    const firstKey = REF_STORE.keys().next().value;
    if (firstKey) REF_STORE.delete(firstKey);
  }
  REF_STORE.set(msgIdx, content);
}

function parseRefIndices(ext?: string[]): { refMsgIdx?: string; msgIdx?: string } {
  if (!ext) return {};
  let refMsgIdx: string | undefined;
  let msgIdx: string | undefined;
  for (const e of ext) {
    const refMatch = /ref_msg_idx=(\S+)/.exec(e);
    if (refMatch) { refMsgIdx = refMatch[1]; continue; }
    const idxMatch = /msg_idx=(\S+)/.exec(e);
    if (idxMatch) msgIdx = idxMatch[1];
  }
  return { refMsgIdx, msgIdx };
}

// ─────────────────────────────────────────────
// Access Token 管理
// ─────────────────────────────────────────────
let accessToken = "";
let tokenExpiry = 0;

async function getAccessToken(): Promise<string> {
  if (Date.now() < tokenExpiry - 60_000) return accessToken;
  const res = await fetch("https://bots.qq.com/app/getAppAccessToken", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ appId: CONFIG.appId, clientSecret: CONFIG.appSecret }),
  });
  const data = (await res.json()) as { access_token: string; expires_in: number };
  accessToken = data.access_token;
  tokenExpiry = Date.now() + data.expires_in * 1000;
  log("Access token refreshed, expires in", data.expires_in, "s");
  return accessToken;
}

function authHeaders(): Record<string, string> {
  return {
    Authorization: `QQBot ${accessToken}`,
    "Content-Type": "application/json",
    "X-Union-Appid": CONFIG.appId,
  };
}

// ─────────────────────────────────────────────
// 日志 (输出到 stderr, 不干扰 MCP stdio)
// ─────────────────────────────────────────────
function log(...args: any[]) {
  console.error(`[qqbot ${new Date().toISOString()}]`, ...args);
}

// ─────────────────────────────────────────────
// 富媒体上传 (图片/视频/语音/文件)
// file_type: 1=图片, 2=视频, 3=语音, 4=文件
// ─────────────────────────────────────────────
async function uploadMedia(
  target: { type: "c2c"; userOpenid: string } | { type: "group"; groupOpenid: string },
  fileType: number,
  urlOrPath: string,
  srvSendMsg: boolean = false
): Promise<{ file_info: string; file_uuid?: string } | null> {
  await getAccessToken();
  const endpoint =
    target.type === "c2c"
      ? `${QQ_API_BASE}/v2/users/${target.userOpenid}/files`
      : `${QQ_API_BASE}/v2/groups/${target.groupOpenid}/files`;

  // Detect local file path vs URL
  const isLocalFile = !urlOrPath.startsWith("http://") && !urlOrPath.startsWith("https://");
  let body: any;
  if (isLocalFile) {
    const filePath = isAbsolute(urlOrPath) ? urlOrPath : resolve(import.meta.dir, urlOrPath);
    if (!existsSync(filePath)) {
      throw new Error(`Local file not found: ${filePath}`);
    }
    const fileData = readFileSync(filePath).toString("base64");
    body = { file_type: fileType, file_data: fileData, srv_send_msg: srvSendMsg };
    // 文件类型(4)需要传 file_name，否则 QQ 显示"未命名"
    if (fileType === 4) {
      body.file_name = basename(filePath);
    }
  } else {
    body = { file_type: fileType, url: urlOrPath, srv_send_msg: srvSendMsg };
    // URL 方式也提取文件名
    if (fileType === 4) {
      try {
        const name = basename(new URL(urlOrPath).pathname);
        if (name) body.file_name = name;
      } catch {}
    }
  }

  const res = await fetch(endpoint, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    log("uploadMedia error:", res.status, errText);
    throw new Error(`QQ media upload ${res.status}: ${errText}`);
  }
  const data = (await res.json()) as any;
  return { file_info: data.file_info, file_uuid: data.file_uuid };
}

// ─────────────────────────────────────────────
// 发送消息 API (文字 + 富媒体)
// ─────────────────────────────────────────────
async function sendC2CMessage(
  userOpenid: string,
  content: string,
  msgId?: string,
  mediaUrl?: string,
  mediaType?: number // 1=图片, 2=视频, 3=语音, 4=文件
): Promise<void> {
  await getAccessToken();
  const msgType = mediaUrl ? 7 : 0;
  const body: any = { content, msg_type: msgType, msg_id: msgId, msg_seq: msgSeqCounter++ };
  if (mediaUrl && mediaType) {
    const media = await uploadMedia({ type: "c2c", userOpenid }, mediaType, mediaUrl);
    if (media) body.media = { file_info: media.file_info };
  }
  const res = await fetch(`${QQ_API_BASE}/v2/users/${userOpenid}/messages`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    log("sendC2CMessage error:", res.status, errText);
    throw new Error(`QQ API ${res.status}: ${errText}`);
  }
}

async function sendGroupMessage(
  groupOpenid: string,
  content: string,
  msgId?: string,
  mediaUrl?: string,
  mediaType?: number // 1=图片, 2=视频, 3=语音, 4=文件
): Promise<void> {
  await getAccessToken();
  const msgType = mediaUrl ? 7 : 0;
  const body: any = { content, msg_type: msgType, msg_id: msgId, msg_seq: msgSeqCounter++ };
  if (mediaUrl && mediaType) {
    const media = await uploadMedia({ type: "group", groupOpenid }, mediaType, mediaUrl);
    if (media) body.media = { file_info: media.file_info };
  }
  const res = await fetch(`${QQ_API_BASE}/v2/groups/${groupOpenid}/messages`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const errText = await res.text();
    log("sendGroupMessage error:", res.status, errText);
    throw new Error(`QQ API ${res.status}: ${errText}`);
  }
}

// ─────────────────────────────────────────────
// 媒体处理: 图片
// ─────────────────────────────────────────────
async function downloadFile(url: string, filename: string): Promise<string> {
  const res = await fetch(url);
  const buf = Buffer.from(await res.arrayBuffer());
  const path = join(TEMP_DIR, `${Date.now()}_${filename}`);
  await Bun.write(path, buf);
  return path;
}

function isImage(att: QQAttachment): boolean {
  return /^image\//.test(att.content_type) ||
    /\.(jpg|jpeg|png|gif|webp|bmp)$/i.test(att.filename);
}

// ─────────────────────────────────────────────
// 媒体处理: 语音 → 文字 (STT)
// ─────────────────────────────────────────────
function isAudio(att: QQAttachment): boolean {
  return /^audio\//.test(att.content_type) ||
    /\.(mp3|wav|ogg|silk|amr|m4a|flac)$/i.test(att.filename);
}

// TODO: 语音识别 (STT) 暂未实现，需引入额外依赖 (silk-decoder + whisper 或外部 STT API)
async function speechToText(_audioPath: string): Promise<string> {
  return "[语音消息暂不支持]";
}

// ─────────────────────────────────────────────
// ─────────────────────────────────────────────
// 统一附件处理
// ─────────────────────────────────────────────
interface ProcessedMedia {
  textParts: string[];
  imageFilePaths: string[];
}

async function processAttachments(attachments: QQAttachment[]): Promise<ProcessedMedia> {
  const result: ProcessedMedia = { textParts: [], imageFilePaths: [] };

  for (const att of attachments) {
    try {
      if (isImage(att)) {
        // 保存图片到本地文件，让 Claude 通过 Read 工具以多模态方式查看
        const localPath = await downloadFile(att.url, att.filename);
        result.imageFilePaths.push(localPath);
        result.textParts.push(`[图片: ${att.filename}, 已保存到 ${localPath}]`);
      } else if (isAudio(att)) {
        const localPath = await downloadFile(att.url, att.filename);
        const transcript = await speechToText(localPath);
        result.textParts.push(`[语音转文字]: ${transcript}`);
        try { await $`rm -f ${localPath} ${localPath.replace(/\.[^.]+$/, ".wav")} ${localPath.replace(/\.[^.]+$/, ".txt")}`.quiet(); } catch {}
      } else {
        const localPath = await downloadFile(att.url, att.filename);
        result.textParts.push(`[文件: ${att.filename}, 已保存到 ${localPath}]`);
      }
    } catch (e: any) {
      result.textParts.push(`[附件处理失败: ${att.filename} - ${e.message}]`);
    }
  }

  return result;
}

// ─────────────────────────────────────────────
// MCP Server (Claude Code Channel 接口)
// ─────────────────────────────────────────────
const mcp = new Server(
  { name: "qqbot", version: "2.0.0" },
  {
    capabilities: {
      experimental: {
        "claude/channel": {},
        "claude/channel/permission": {},
      },
      tools: {},
    },
    instructions: `
你通过 QQ 官方机器人接收来自用户的消息。
消息格式: <channel source="qqbot" type="c2c|group" user_openid="..." group_openid="..." msg_id="...">
- type=c2c 私聊, type=group 群消息
- 消息可能包含 [图片], [语音转文字], [文件] 等多媒体内容
- 如果消息包含图片 [图片: xxx, 已保存到 /path/to/file], 请使用 Read 工具读取该图片文件路径来查看图片内容（Read 工具支持多模态图片识别）
- meta 中的 image_paths 字段包含逗号分隔的图片文件路径列表
- 不要尝试以文本方式解析图片数据！用 Read 工具读取图片文件即可
- 使用 reply 工具回复, 必须传回 type/user_openid/group_openid/msg_id

## 📸 富媒体发送
在回复文本中使用以下标签即可发送富媒体，系统会自动解析并处理：
| 类型 | 标签 | 示例 |
|------|------|------|
| 图片 | <qqimg>绝对路径</qqimg> | <qqimg>/home/user/photo.jpg</qqimg> |
| 语音 | <qqvoice>绝对路径</qqvoice> | <qqvoice>/home/user/voice.mp3</qqvoice> |
| 视频 | <qqvideo>绝对路径</qqvideo> | <qqvideo>/home/user/video.mp4</qqvideo> |
| 文件 | <qqfile>绝对路径</qqfile> | <qqfile>/home/user/doc.pdf</qqfile> |

规则:
- 路径必须是绝对路径
- 标签必须完整闭合
- 文件大小上限 20MB
- 不要说"无法发送图片/文件"，直接用标签，系统自动处理
- 也支持传统方式: file_url + file_type 参数 (支持公网URL或本地路径)
`.trim(),
  }
);

// ── 工具注册 ──
mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "reply",
      description: "通过 QQ 机器人回复消息 (支持文字 + 图片/视频/语音/文件)",
      inputSchema: {
        type: "object",
        properties: {
          type: { type: "string", enum: ["c2c", "group"], description: "消息类型" },
          user_openid: { type: "string", description: "私聊用户 openid" },
          group_openid: { type: "string", description: "群 openid" },
          msg_id: { type: "string", description: "原消息 ID" },
          text: { type: "string", description: "回复文字" },
          image_url: { type: "string", description: "可选: 回复图片的 URL (file_type=1 的快捷方式)" },
          file_url: { type: "string", description: "可选: 富媒体文件的 URL 或本地文件路径 (需配合 file_type)" },
          file_type: { type: "number", enum: [1, 2, 3, 4], description: "富媒体类型: 1=图片, 2=视频, 3=语音, 4=文件" },
        },
        required: ["type", "text"],
      },
    },
  ],
}));

// ── 富媒体标签解析 ──
const MEDIA_TAG_RE = /<(qqimg|qqvoice|qqvideo|qqfile)>(.*?)<\/\1>/g;
const TAG_TO_FILE_TYPE: Record<string, number> = {
  qqimg: 1, qqvideo: 2, qqvoice: 3, qqfile: 4,
};

interface ParsedMedia {
  cleanText: string;
  mediaItems: { path: string; fileType: number }[];
}

function parseMediaTags(text: string): ParsedMedia {
  const mediaItems: { path: string; fileType: number }[] = [];
  const cleanText = text.replace(MEDIA_TAG_RE, (_, tag, path) => {
    const trimmed = path.trim();
    if (trimmed) {
      mediaItems.push({ path: trimmed, fileType: TAG_TO_FILE_TYPE[tag] });
    }
    return "";
  }).trim();
  return { cleanText, mediaItems };
}

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === "reply") {
    const args = req.params.arguments as Record<string, any>;
    const { type, user_openid, group_openid, msg_id, text, image_url, file_url, file_type } = args;

    try {
      if (!type || (type === "c2c" && !user_openid) || (type === "group" && !group_openid)) {
        throw new Error("缺少 user_openid 或 group_openid");
      }

      // 解析文本中的媒体标签
      const parsed = parseMediaTags(text);

      // 传统参数方式的媒体 (file_url/image_url)
      const legacyMediaUrl = file_url || image_url;
      const legacyMediaType = file_url ? (file_type || 4) : (image_url ? 1 : undefined);

      // 合并: 标签媒体 + 传统参数媒体
      const allMedia = [...parsed.mediaItems];
      if (legacyMediaUrl && legacyMediaType) {
        allMedia.push({ path: legacyMediaUrl, fileType: legacyMediaType });
      }

      const sendFn = type === "c2c"
        ? (content: string, mid?: string, mUrl?: string, mType?: number) =>
            sendC2CMessage(user_openid, content, mid, mUrl, mType)
        : (content: string, mid?: string, mUrl?: string, mType?: number) =>
            sendGroupMessage(group_openid, content, mid, mUrl, mType);

      if (allMedia.length === 0) {
        // 纯文本
        await sendFn(parsed.cleanText, msg_id);
      } else if (allMedia.length === 1) {
        // 文字 + 单个媒体 (一条消息)
        await sendFn(parsed.cleanText, msg_id, allMedia[0].path, allMedia[0].fileType);
      } else {
        // 多个媒体: 文字单独发, 每个媒体单独发 (QQ 单条消息只支持一个媒体)
        if (parsed.cleanText) {
          await sendFn(parsed.cleanText, msg_id);
        }
        for (const item of allMedia) {
          await sendFn("", msg_id, item.path, item.fileType);
        }
      }

      return { content: [{ type: "text", text: "消息已发送" }] };
    } catch (e: any) {
      return { content: [{ type: "text", text: `发送失败: ${e.message}` }] };
    }
  }
  throw new Error(`未知工具: ${req.params.name}`);
});

// ── 权限审批中继 ──
const PermissionRequestSchema = z.object({
  method: z.literal("notifications/claude/channel/permission_request"),
  params: z.object({
    request_id: z.string(),
    tool_name: z.string(),
    description: z.string(),
    input_preview: z.string(),
  }),
});

let permissionTarget: { type: "c2c" | "group"; id: string } | null = null;
const alwaysAllowedTools = new Set<string>();
const pendingPermissions = new Map<string, string>(); // request_id -> tool_name

mcp.setNotificationHandler(PermissionRequestSchema, async ({ params }) => {
  if (!permissionTarget) return;

  // 自动批准已设为"始终允许"的工具
  if (alwaysAllowedTools.has(params.tool_name)) {
    log(`Auto-approving tool: ${params.tool_name}`);
    await mcp.notification({
      method: "notifications/claude/channel/permission",
      params: { request_id: params.request_id, behavior: "allow" },
    });
    return;
  }

  pendingPermissions.set(params.request_id, params.tool_name);

  const msg =
    `Claude 请求执行:\n` +
    `工具: ${params.tool_name}\n` +
    `说明: ${params.description}\n\n` +
    `回复 "yes ${params.request_id}" 允许\n` +
    `回复 "no ${params.request_id}" 拒绝\n` +
    `回复 "always ${params.request_id}" 始终允许该工具`;
  if (permissionTarget.type === "c2c") {
    await sendC2CMessage(permissionTarget.id, msg);
  } else {
    await sendGroupMessage(permissionTarget.id, msg);
  }
});

// ── 连接 Claude Code ──
await mcp.connect(new StdioServerTransport());
log("MCP server connected to Claude Code");

// ─────────────────────────────────────────────
// WebSocket Gateway 连接 (替代 Webhook)
// ─────────────────────────────────────────────
const PERMISSION_RE = /^\s*(y|yes|n|no|always)\s+([a-km-z]{5})\s*$/i;

// WebSocket OpCodes
const OP = {
  DISPATCH: 0,
  HEARTBEAT: 1,
  IDENTIFY: 2,
  RESUME: 6,
  RECONNECT: 7,
  INVALID_SESSION: 9,
  HELLO: 10,
  HEARTBEAT_ACK: 11,
} as const;

// Intents: 群+C2C 消息事件
const INTENTS =
  (1 << 25) |  // GROUP_AND_C2C_EVENT: C2C_MESSAGE_CREATE, GROUP_AT_MESSAGE_CREATE
  (1 << 30) |  // PUBLIC_GUILD_MESSAGES
  (1 << 12) |  // DIRECT_MESSAGE
  (1 << 26);   // INTERACTION

// WebSocket 状态
let ws: WebSocket | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let lastSeq: number | null = null;
let sessionId: string | null = null;
let resumeGatewayUrl: string | null = null;
let reconnectAttempt = 0;
const BACKOFF = [1000, 2000, 5000, 10000, 30000, 60000];

async function getGatewayUrl(): Promise<string> {
  const token = await getAccessToken();
  const res = await fetch(`${QQ_API_BASE}/gateway`, {
    headers: { Authorization: `QQBot ${token}` },
  });
  const data = (await res.json()) as { url: string };
  log("Gateway URL:", data.url);
  return data.url;
}

function startHeartbeat(intervalMs: number) {
  stopHeartbeat();
  log("Starting heartbeat, interval:", intervalMs, "ms");
  heartbeatTimer = setInterval(() => {
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ op: OP.HEARTBEAT, d: lastSeq }));
    }
  }, intervalMs);
}

function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

async function sendIdentify() {
  const token = await getAccessToken();
  const payload = {
    op: OP.IDENTIFY,
    d: {
      token: `QQBot ${token}`,
      intents: INTENTS,
      shard: [0, 1],
    },
  };
  ws?.send(JSON.stringify(payload));
  log("Sent Identify");
}

async function sendResume() {
  const token = await getAccessToken();
  const payload = {
    op: OP.RESUME,
    d: {
      token: `QQBot ${token}`,
      session_id: sessionId,
      seq: lastSeq,
    },
  };
  ws?.send(JSON.stringify(payload));
  log("Sent Resume, session:", sessionId, "seq:", lastSeq);
}

async function handleDispatch(t: string, d: any, s: number) {
  lastSeq = s;

  if (t === "READY") {
    sessionId = d.session_id;
    resumeGatewayUrl = d.resume_gateway_url ?? null;
    reconnectAttempt = 0;
    log("READY! session:", sessionId, "user:", d.user?.username);
    return;
  }

  if (t === "RESUMED") {
    reconnectAttempt = 0;
    log("RESUMED successfully");
    return;
  }

  // 只处理消息事件
  if (t !== "C2C_MESSAGE_CREATE" && t !== "GROUP_AT_MESSAGE_CREATE") return;

  const data = d as QQMessageData;
  let msgType: "c2c" | "group" = "c2c";
  let userOpenid = "";
  let groupOpenid = "";
  let content = (data.content ?? "").trim();

  if (t === "C2C_MESSAGE_CREATE") {
    msgType = "c2c";
    userOpenid = data.author?.user_openid ?? data.author?.id ?? "";
  } else if (t === "GROUP_AT_MESSAGE_CREATE") {
    msgType = "group";
    groupOpenid = data.group_openid ?? data.group_id ?? "";
    userOpenid = data.author?.member_openid ?? data.author?.id ?? "";
    content = content.replace(/^<@!\d+>\s*/, "").trim();
  }

  log(`${t}: user=${userOpenid} group=${groupOpenid} content="${content.slice(0, 50)}"`);

  // 白名单
  if (CONFIG.allowedUsers.length > 0 && !CONFIG.allowedUsers.includes(userOpenid)) {
    log("User not in allowlist, ignoring");
    return;
  }

  // 权限审批
  const permMatch = PERMISSION_RE.exec(content);
  if (permMatch) {
    const verdict = permMatch[1].toLowerCase();
    const requestId = permMatch[2].toLowerCase();

    // "always" => 记住该工具，以后自动批准
    if (verdict === "always") {
      const toolName = pendingPermissions.get(requestId);
      if (toolName) {
        alwaysAllowedTools.add(toolName);
        log(`Tool "${toolName}" added to always-allow list`);
      }
    }
    pendingPermissions.delete(requestId);

    await mcp.notification({
      method: "notifications/claude/channel/permission",
      params: {
        request_id: requestId,
        behavior: verdict.startsWith("n") ? "deny" : "allow",
      },
    });
    return;
  }

  // 更新审批目标
  permissionTarget = msgType === "group" && groupOpenid
    ? { type: "group", id: groupOpenid }
    : { type: "c2c", id: userOpenid };

  // 解析引用消息
  const { refMsgIdx, msgIdx } = parseRefIndices(data.message_scene?.ext);
  let quotedPrefix = "";
  if (refMsgIdx) {
    const quoted = REF_STORE.get(refMsgIdx);
    if (quoted) {
      quotedPrefix = `[引用消息]: ${quoted}\n`;
    } else {
      quotedPrefix = `[引用了一条消息]\n`;
    }
  }
  // 存储本条消息供后续引用
  if (msgIdx && content) {
    setRefEntry(msgIdx, content);
  }

  // 处理附件 (图片/语音/文件)
  const media = data.attachments?.length
    ? await processAttachments(data.attachments)
    : { textParts: [], imageFilePaths: [] };

  const fullContent = [quotedPrefix + content, ...media.textParts].filter(Boolean).join("\n");

  // 转发给 Claude (via channel notification)
  await mcp.notification({
    method: "notifications/claude/channel",
    params: {
      content: fullContent,
      meta: {
        type: msgType,
        user_openid: userOpenid,
        group_openid: groupOpenid,
        msg_id: data.id,
        event_type: t,
        has_image: String(media.imageFilePaths.length > 0),
        image_count: String(media.imageFilePaths.length),
        // 传递图片文件路径，Claude 可通过 Read 工具以多模态方式查看
        ...(media.imageFilePaths.length > 0 && {
          image_paths: media.imageFilePaths.join(","),
        }),
      },
    },
  });

  // 同时写入消息队列文件 (供 Stop Hook 轮询)
  appendToQueue({
    type: msgType,
    user_openid: userOpenid,
    group_openid: groupOpenid,
    msg_id: data.id,
    content: fullContent,
    image_paths: media.imageFilePaths,
    timestamp: data.timestamp,
  });
}

async function connectGateway(resuming = false) {
  const url = resuming && resumeGatewayUrl
    ? resumeGatewayUrl
    : await getGatewayUrl();

  log("Connecting to gateway:", url, resuming ? "(resuming)" : "(fresh)");

  ws = new WebSocket(url);

  ws.addEventListener("open", () => {
    log("WebSocket connected");
  });

  ws.addEventListener("message", async (event) => {
    let payload: any;
    try {
      payload = JSON.parse(typeof event.data === "string" ? event.data : Buffer.from(event.data as ArrayBuffer).toString());
    } catch {
      log("Failed to parse WS message");
      return;
    }

    switch (payload.op) {
      case OP.HELLO: {
        const interval = payload.d?.heartbeat_interval ?? 45000;
        startHeartbeat(interval);
        if (resuming && sessionId) {
          await sendResume();
        } else {
          await sendIdentify();
        }
        break;
      }

      case OP.DISPATCH:
        await handleDispatch(payload.t, payload.d, payload.s);
        break;

      case OP.HEARTBEAT:
        // Server requests immediate heartbeat
        ws?.send(JSON.stringify({ op: OP.HEARTBEAT, d: lastSeq }));
        break;

      case OP.HEARTBEAT_ACK:
        // Heartbeat acknowledged, all good
        break;

      case OP.RECONNECT:
        log("Server requested reconnect");
        stopHeartbeat();
        ws?.close();
        await reconnect(true);
        break;

      case OP.INVALID_SESSION: {
        const canResume = payload.d === true;
        log("Invalid session, canResume:", canResume);
        stopHeartbeat();
        ws?.close();
        if (!canResume) {
          sessionId = null;
          lastSeq = null;
        }
        // Wait 1-5s as per QQ docs
        await new Promise(r => setTimeout(r, 1000 + Math.random() * 4000));
        await reconnect(canResume && !!sessionId);
        break;
      }

      default:
        log("Unknown opcode:", payload.op);
    }
  });

  ws.addEventListener("close", async (event) => {
    log("WebSocket closed, code:", event.code, "reason:", event.reason);
    stopHeartbeat();

    // Close codes that allow resume
    const resumable = [4008, 4009].includes(event.code) || (event.code >= 1000 && event.code < 4000);
    const canResume = resumable && !!sessionId;

    // Don't reconnect on fatal errors
    const fatal = [4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915].includes(event.code);
    if (fatal) {
      log("Fatal close code, not reconnecting:", event.code);
      return;
    }

    await reconnect(canResume);
  });

  ws.addEventListener("error", (event) => {
    log("WebSocket error:", event);
  });
}

async function reconnect(resuming: boolean) {
  const delay = BACKOFF[Math.min(reconnectAttempt, BACKOFF.length - 1)];
  reconnectAttempt++;
  log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempt}, resume=${resuming})`);
  await new Promise(r => setTimeout(r, delay));

  // Refresh token before reconnecting
  try {
    await getAccessToken();
  } catch (e) {
    log("Failed to refresh token:", e);
  }

  await connectGateway(resuming);
}

// ─────────────────────────────────────────────
// 启动
// ─────────────────────────────────────────────
log("Starting QQBot (WebSocket mode), appId:", CONFIG.appId);
await connectGateway();
