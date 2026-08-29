#!/usr/bin/env bun
/**
 * gateway-gui/server.ts — 机娘 QQ Bot 网关统一图形界面（单一窗口）
 *
 * 职责（纯运维观察，不改动任何业务逻辑）：
 *  - 以子进程方式启动 qqbot-gateway.ts（网关）与 check-pending.ts（外部时钟 / 检测器）
 *  - 捕获两者 stdout/stderr，经 WebSocket 实时推送到浏览器
 *  - 每秒读取 .tmp/*.json，推送待处理消息 / 待发送回复 / 定时任务 / 用户别名等统计
 *  - 提供 启动 / 停止 / 重启 控制接口
 *
 * 启动方式：bun gateway-gui/server.ts
 *   start-gateway.bat 会调用本脚本并自动打开浏览器控制台。
 *   关闭本进程（Ctrl+C / 关闭窗口）会同时结束网关与外部时钟。
 *
 * 环境变量：
 *   GUI_PORT       控制台端口（默认 8989）
 *   GUI_AUTOSTART  设为 0 时不自动启动子进程（默认自动启动）
 *   GUI_NO_OPEN    设为 1 时不自动打开浏览器
 */

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

// ── 配置 ──
const PORT = Number.parseInt(process.env.GUI_PORT ?? "8989", 10);
const HOST = "127.0.0.1";
const ROOT = import.meta.dir; // gateway-gui/
const APP_ROOT = join(ROOT, ".."); // 项目根目录
const TEMP_DIR = join(APP_ROOT, ".tmp");
const INDEX_FILE = join(ROOT, "index.html");

const AUTOSTART = process.env.GUI_AUTOSTART !== "0";
const NO_OPEN = process.env.GUI_NO_OPEN === "1";

const APP_ID = process.env.QQ_APPID ?? "";
const APP_SECRET = process.env.QQ_APP_SECRET ?? "";
const CONFIGURED =
  APP_ID !== "" && APP_SECRET !== "" &&
  APP_ID !== "你的AppID" && APP_SECRET !== "你的AppSecret";

// ── 实际生效的会话 ID：优先环境变量 CLAUDE_SESSION_ID，
//    其次 check-pending.ts 中内置的回退默认值（部署版往往直接内置真实会话 ID）──
function readFallbackSessionId(): string {
  try {
    const src = readFileSync(join(APP_ROOT, "check-pending.ts"), "utf-8");
    const m = src.match(/CLAUDE_SESSION_ID\s*\?\?\s*["']([^"']+)["']/);
    return m ? m[1] : "";
  } catch { return ""; }
}
const SESSION_ID = process.env.CLAUDE_SESSION_ID ?? "";
const EFFECTIVE_SESSION_ID = SESSION_ID || readFallbackSessionId();
const SESSION_CONFIGURED =
  EFFECTIVE_SESSION_ID !== "" && EFFECTIVE_SESSION_ID !== "你的SessionID";
const SESSION_MASK = SESSION_CONFIGURED
  ? EFFECTIVE_SESSION_ID.slice(0, 4) + "****" + EFFECTIVE_SESSION_ID.slice(-4)
  : "";

// ── 受管进程 ──
interface ManagedProc {
  name: string;
  script: string;
  label: string;
  desc: string;
  proc: any; // Subprocess
  lines: string[];
  running: boolean;
  exitCode: number | null;
  startedAt: number | null;
  lastActivity: number;
}

function makeProc(name: string, script: string, label: string, desc: string): ManagedProc {
  return { name, script, label, desc, proc: null, lines: [], running: false, exitCode: null, startedAt: null, lastActivity: 0 };
}

const PROCS: Record<string, ManagedProc> = {
  gateway: makeProc("gateway", "qqbot-gateway.ts", "网关", "WebSocket 收消息 → 队列；replies.json → QQ API 发送"),
  detector: makeProc("detector", "check-pending.ts", "外部时钟 / 检测器", "每 5s 轮询 → 唤醒 Claude Code → 解析回复 → 写 replies.json"),
};

const clients = new Set<any>();

function log(...args: any[]) {
  console.log("[gui " + new Date().toISOString() + "]", ...args);
}

// ── 日志环形缓冲 + 广播 ──
const MAX_LINES = 800;

function broadcast(obj: unknown) {
  const msg = JSON.stringify(obj);
  for (const c of clients) {
    try { c.send(msg); } catch { /* ignore dead socket */ }
  }
}

function appendLine(p: ManagedProc, line: string, isErr: boolean) {
  const entry = (isErr ? "[stderr] " : "") + line;
  p.lines.push(entry);
  if (p.lines.length > MAX_LINES) p.lines.splice(0, p.lines.length - MAX_LINES);
  p.lastActivity = Date.now();
  broadcast({ type: "line", name: p.name, line: entry });
}

// ── 子进程生命周期 ──
async function pumpStream(stream: ReadableStream<Uint8Array>, p: ManagedProc, isErr: boolean) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        let line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (line.endsWith("\r")) line = line.slice(0, -1);
        appendLine(p, line, isErr);
      }
    }
    buf += decoder.decode();
    if (buf.trim()) appendLine(p, buf, isErr);
  } catch (e: any) {
    appendLine(p, "[读取进程输出失败] " + (e?.message ?? e), true);
  }
}

function spawnProc(p: ManagedProc) {
  if (p.proc || p.running) return;
  p.lines = [];
  p.exitCode = null;
  p.startedAt = Date.now();
  p.lastActivity = Date.now();
  p.running = true;
  broadcast({ type: "proc", name: p.name, running: true, exitCode: null, startedAt: p.startedAt });
  appendLine(p, "════ 启动 " + p.script + " ════", false);
  try {
    const proc = Bun.spawn({
      cmd: [process.execPath, p.script],
      cwd: APP_ROOT,
      env: process.env,
      stdout: "pipe",
      stderr: "pipe",
      onExit(_proc, code, signal) {
        p.running = false;
        p.exitCode = code;
        p.proc = null;
        appendLine(p, "════ 进程退出 (exit=" + code + ", signal=" + (signal ?? "none") + ") ════", code !== 0);
        broadcast({ type: "proc", name: p.name, running: false, exitCode: code, startedAt: p.startedAt });
      },
    });
    p.proc = proc;
    pumpStream(proc.stdout, p, false);
    pumpStream(proc.stderr, p, true);
  } catch (e: any) {
    p.running = false;
    p.proc = null;
    appendLine(p, "[启动失败] " + (e?.message ?? e), true);
    broadcast({ type: "proc", name: p.name, running: false, exitCode: -1, startedAt: p.startedAt });
  }
}

function stopProc(p: ManagedProc) {
  if (!p.proc && !p.running) return;
  appendLine(p, "════ 收到停止请求 ════", false);
  try { p.proc?.kill(); } catch { /* ignore */ }
  // 兜底：2 秒后仍存活则强制结束（Windows 信号支持有限）
  setTimeout(() => {
    if (p.proc && p.running) {
      try { p.proc.kill("SIGKILL"); } catch { /* ignore */ }
    }
  }, 2000);
}

function restartProc(p: ManagedProc) {
  stopProc(p);
  const deadline = Date.now() + 5000;
  const timer = setInterval(() => {
    if (!p.running) {
      clearInterval(timer);
      spawnProc(p);
    } else if (Date.now() > deadline) {
      clearInterval(timer);
      try { p.proc?.kill("SIGKILL"); } catch { /* ignore */ }
      setTimeout(() => spawnProc(p), 300);
    }
  }, 200);
}

// ── 实时统计（读取 .tmp/*.json）──
function readJsonSafe(file: string): any {
  try {
    if (!existsSync(file)) return null;
    const raw = readFileSync(file, "utf-8").trim();
    if (!raw) return null;
    return JSON.parse(raw);
  } catch { return null; }
}

function currentStats() {
  const queue = readJsonSafe(join(TEMP_DIR, "pending_messages.json"));
  const replies = readJsonSafe(join(TEMP_DIR, "replies.json"));
  const schedule = readJsonSafe(join(TEMP_DIR, "scheduled_tasks.json"));
  const aliases = readJsonSafe(join(TEMP_DIR, "user_aliases.json"));
  let lockRaw = "";
  try {
    if (existsSync(join(TEMP_DIR, "processing.lock")))
      lockRaw = readFileSync(join(TEMP_DIR, "processing.lock"), "utf-8").trim();
  } catch { /* ignore */ }

  const pending = Array.isArray(queue) ? queue : [];
  const latest = pending.length ? pending[pending.length - 1] : null;

  return {
    ts: Date.now(),
    pendingCount: pending.length,
    pendingPreview: latest ? String(latest?.content ?? "").slice(0, 60) : null,
    pendingType: latest?.type ?? null,
    repliesCount: Array.isArray(replies) ? replies.length : 0,
    scheduled: Array.isArray(schedule)
      ? schedule.map((t: any) => ({ time: t?.time ?? "?", content: String(t?.content ?? "") }))
      : [],
    aliasCount: aliases && typeof aliases === "object" && !Array.isArray(aliases)
      ? Object.keys(aliases).length : 0,
    locked: lockRaw !== "" && Number.isFinite(Number(lockRaw)),
    configured: CONFIGURED,
    sessionConfigured: SESSION_CONFIGURED,
    tempDir: TEMP_DIR,
  };
}

// ── 客户端消息处理 ──
function handleClientMessage(ws: any, raw: string) {
  let msg: any;
  try { msg = JSON.parse(raw); } catch { return; }
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "control") {
    const p = PROCS[msg.name];
    if (!p) return;
    if (msg.action === "start") spawnProc(p);
    else if (msg.action === "stop") stopProc(p);
    else if (msg.action === "restart") restartProc(p);
  } else if (msg.type === "clear") {
    const p = PROCS[msg.name];
    if (!p) return;
    p.lines = [];
    broadcast({ type: "clear", name: p.name });
  }
}

function initMessage() {
  return JSON.stringify({
    type: "init",
    port: PORT,
    procs: Object.values(PROCS).map((p) => ({
      name: p.name, label: p.label, desc: p.desc, script: p.script,
      running: p.running, exitCode: p.exitCode, startedAt: p.startedAt,
      lines: p.lines,
    })),
    stats: currentStats(),
  });
}

// ── HTTP + WebSocket 服务 ──
const server = Bun.serve({
  hostname: HOST,
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);

    if (url.pathname === "/ws") {
      if (server.upgrade(req)) return;
      return new Response("WebSocket upgrade failed", { status: 400 });
    }
    if (url.pathname === "/api/stats") {
      return Response.json(currentStats());
    }
    if (url.pathname === "/api/control" && req.method === "POST") {
      try {
        const body = await req.json();
        const p = PROCS[body?.name];
        if (!p) return Response.json({ ok: false, error: "unknown process" }, { status: 400 });
        if (body.action === "start") spawnProc(p);
        else if (body.action === "stop") stopProc(p);
        else if (body.action === "restart") restartProc(p);
        else return Response.json({ ok: false, error: "unknown action" }, { status: 400 });
        return Response.json({ ok: true });
      } catch (e: any) {
        return Response.json({ ok: false, error: String(e) }, { status: 400 });
      }
    }
    if (url.pathname === "/" || url.pathname === "/index.html") {
      if (!existsSync(INDEX_FILE)) return new Response("gateway-gui/index.html 缺失", { status: 500 });
      return new Response(Bun.file(INDEX_FILE));
    }
    return new Response("Not found", { status: 404 });
  },
  websocket: {
    open(ws: any) {
      clients.add(ws);
      ws.send(initMessage());
    },
    message(ws: any, raw: any) {
      const text = typeof raw === "string" ? raw : Buffer.from(raw as ArrayBuffer).toString();
      handleClientMessage(ws, text);
    },
    close(ws: any) {
      clients.delete(ws);
    },
  },
});

// ── 每秒推送一次实时统计（待处理 / 待发送 / 定时任务 / 别名等）──
setInterval(() => {
  broadcast({ type: "stats", stats: currentStats() });
}, 1000);

// ── 优雅退出：结束子进程 ──
let shuttingDown = false;
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  log("正在关闭，结束子进程…");
  for (const p of Object.values(PROCS)) {
    try { p.proc?.kill(); } catch { /* ignore */ }
  }
  setTimeout(() => process.exit(0), 800);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("exit", () => {
  for (const p of Object.values(PROCS)) {
    try { p.proc?.kill("SIGKILL"); } catch { /* ignore */ }
  }
});

// ── 父进程看门狗：控制台窗口被关闭（点 X / 宿主进程消失）时自动结束子进程 ──
// start-gateway.bat 用 start + cmd /c 启动本服务，cmd 是承载控制台窗口的父进程；
// 窗口关闭时 cmd 先退出，这里探测到父进程消失后自动 shutdown。
const PARENT_PID = process.ppid;
if (process.platform === "win32") {
  setInterval(() => {
    if (shuttingDown) return;
    try {
      process.kill(PARENT_PID, 0); // 0 = 仅探测进程是否存在
    } catch {
      log("检测到控制台窗口已关闭（父进程消失），自动结束网关与外部时钟…");
      shutdown();
    }
  }, 2000);
}

// ── 启动 ──
log("╔══════════════════════════════════════════════════╗");
log("║   机娘 QQ Bot 网关图形界面（单一窗口）           ║");
log("╚══════════════════════════════════════════════════╝");
log("控制台地址: http://" + HOST + ":" + PORT);
log("项目目录  : " + APP_ROOT);
log("凭证状态  : " + (CONFIGURED ? "已配置" : "未配置 —— 请先在 start-gateway.bat 填写 QQ_APPID / QQ_APP_SECRET"));
log("会话状态  : " + (SESSION_CONFIGURED ? "已配置 (" + SESSION_MASK + ")" : "CLAUDE_SESSION_ID 未配置（外部时钟将使用占位会话）"));
log("关闭本窗口（Ctrl+C）将同时结束网关与外部时钟进程。");

if (AUTOSTART) {
  log("自动启动 网关 + 外部时钟 …");
  spawnProc(PROCS.gateway);
  spawnProc(PROCS.detector);
} else {
  log("GUI_AUTOSTART=0，请在浏览器控制台手动启动。");
}

if (!NO_OPEN) {
  setTimeout(() => {
    try {
      Bun.spawn(["cmd", "/c", "start", "", "http://" + HOST + ":" + PORT], { stdout: "ignore", stderr: "ignore" });
      log("已在默认浏览器打开控制台；如未弹出请手动访问 http://" + HOST + ":" + PORT);
    } catch { /* 打开浏览器失败不影响使用 */ }
  }, 500);
}
