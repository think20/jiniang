#!/usr/bin/env bun
/**
 * check-pending.ts — 外部消息检测 + 唤醒脚本
 *
 * 每 30 秒检查 pending_messages.json + scheduled_tasks.json
 * - 有QQ消息 → claude --resume <session-id> -p 唤醒处理
 * - 有定时任务到点 → 视作额外消息，提示词中标注「定时任务」
 * - 无消息/无到期任务 → 静默跳过
 *
 * scheduled_tasks.json 格式:
 * [{ "time": "HH:MM", "user_openid": "...", "content": "任务描述" }]
 * 同一任务同一天只触发一次（通过 last_date 去重）
 */

import { existsSync, readFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";

// ⚠️ 请填写你的 Claude Code 会话 ID（通过 claude --resume list 查看）
const SESSION_ID = process.env.CLAUDE_SESSION_ID ?? "你的SessionID";
const TEMP_DIR = join(import.meta.dir, ".tmp");
const QUEUE_FILE = join(TEMP_DIR, "pending_messages.json");
const REPLIES_FILE = join(TEMP_DIR, "replies.json");
const SCHEDULE_FILE = join(TEMP_DIR, "scheduled_tasks.json");
const LOCK_FILE = join(TEMP_DIR, "processing.lock");
const USER_ALIASES_FILE = join(TEMP_DIR, "user_aliases.json");

if (!existsSync(TEMP_DIR)) mkdirSync(TEMP_DIR, { recursive: true });

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
  file_url?: string;
  file_type?: 1 | 2 | 3 | 4;
}

interface UserAliases {
  [openid: string]: string;
}

function readQueue(): QueuedMessage[] {
  try {
    if (!existsSync(QUEUE_FILE)) return [];
    const raw = readFileSync(QUEUE_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

// ── 用户别名管理 ──
function readAliases(): UserAliases {
  try {
    if (!existsSync(USER_ALIASES_FILE)) return {};
    const raw = readFileSync(USER_ALIASES_FILE, "utf-8");
    return JSON.parse(raw) as UserAliases;
  } catch {
    return {};
  }
}

function writeAliases(aliases: UserAliases): void {
  Bun.write(USER_ALIASES_FILE, JSON.stringify(aliases, null, 2));
}

function getOrCreateAlias(openid: string, aliases: UserAliases): string {
  if (aliases[openid]) return aliases[openid];
  const idx = Object.keys(aliases).length;
  const letter = String.fromCharCode(65 + idx); // A, B, C, ...
  const alias = `用户${letter}`;
  aliases[openid] = alias;
  return alias;
}

// ── 定时任务 ──
interface ScheduledTask {
  time: string;         // "HH:MM" 格式
  user_openid?: string;  // 可选：任务关联的用户
  content: string;       // 任务描述
  last_date?: string;    // 上次触发的日期 "YYYY-MM-DD"，用于去重
}

function readSchedule(): ScheduledTask[] {
  try {
    if (!existsSync(SCHEDULE_FILE)) return [];
    const raw = readFileSync(SCHEDULE_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeSchedule(tasks: ScheduledTask[]): void {
  Bun.write(SCHEDULE_FILE, JSON.stringify(tasks, null, 2));
}

// 检查是否有到期的定时任务（当前分钟匹配，且今天未触发过）
function checkScheduledTasks(): ScheduledTask | null {
  const tasks = readSchedule();
  if (tasks.length === 0) return null;

  const now = new Date();
  const hhmm = `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
  const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;

  for (const task of tasks) {
    if (task.time === hhmm && task.last_date !== today) {
      // 标记为今天已触发
      task.last_date = today;
      writeSchedule(tasks);
      return task;
    }
  }
  return null;
}

const LOCK_TTL_MS = 90_000; // 锁过期时间 90 秒（一次 Claude 处理的最长等待）

function isLocked(): boolean {
  try {
    if (!existsSync(LOCK_FILE)) return false;
    const raw = readFileSync(LOCK_FILE, "utf-8").trim();
    if (!raw) return false;
    const lockTime = parseInt(raw);
    if (isNaN(lockTime)) { releaseLock(); return false; }
    // 锁过期则清除（防止死锁）
    if (Date.now() - lockTime > LOCK_TTL_MS) {
      releaseLock();
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

function acquireLock(): boolean {
  if (isLocked()) return false;
  Bun.write(LOCK_FILE, String(Date.now()));
  return true;
}

function releaseLock(): void {
  try { if (existsSync(LOCK_FILE)) Bun.write(LOCK_FILE, ""); } catch {}
}

function cleanupAndExit(code: number = 0) {
  console.log(`\n[check-pending] Shutting down...`);
  releaseLock();
  process.exit(code);
}

// Ctrl+C / SIGTERM 时清理锁文件
process.on("SIGINT", () => cleanupAndExit(0));
process.on("SIGTERM", () => cleanupAndExit(0));

function log(...args: any[]) {
  console.error(`[check-pending ${new Date().toISOString()}]`, ...args);
}

// ── 状态常量 ──
const STATUS = {
  IDLE:      "[STATUS:IDLE]",
  DETECTED:  "[STATUS:DETECTED]",
  WAKING:    "[STATUS:WAKING]",
  SUCCESS:   "[STATUS:SUCCESS]",
  FAILED:    "[STATUS:FAILED]",
  LOCKED:    "[STATUS:LOCKED]",
  COOLDOWN:  "[STATUS:COOLDOWN]",
} as const;

const COOLDOWN_MS = 60_000; // 处理完成后冷却 60 秒再恢复轮询
const SESSION_MASK = SESSION_ID.slice(0, 4) + "****" + SESSION_ID.slice(-4);

// ── 解析 Claude 回复中的 @用户XX:、[IMG:路径]、[SCHEDULE:...] 标记 ──
interface ParsedReply {
  alias: string;
  text: string;
  imagePath?: string;
  schedule?: { time: string; content: string };
  scheduleList?: boolean;
  scheduleDelete?: string; // 要删除的任务匹配串（HH:MM 或内容子串）
}

function parseReplies(text: string): ParsedReply[] {
  const replies: ParsedReply[] = [];
  const lines = text.split("\n");
  let currentAlias: string | null = null;
  let currentLines: string[] = [];

  for (const line of lines) {
    const match = line.match(/^@(用户[A-Z]):\s*(.*)/);
    if (match) {
      if (currentAlias && currentLines.length > 0) {
        replies.push({ alias: currentAlias, text: currentLines.join("\n").trim() });
      }
      currentAlias = match[1];
      currentLines = match[2] ? [match[2]] : [];
    } else if (currentAlias) {
      currentLines.push(line);
    }
  }
  if (currentAlias && currentLines.length > 0) {
    replies.push({ alias: currentAlias, text: currentLines.join("\n").trim() });
  }
  for (const r of replies) {
    // [SCHEDULE:LIST] — 列出所有定时任务
    if (/\[SCHEDULE:LIST\]/.test(r.text)) {
      r.scheduleList = true;
      r.text = r.text.replace(/\[SCHEDULE:LIST\]/, "").trim();
    }
    // [SCHEDULE:DELETE:匹配] — 删除匹配的定时任务
    const delMatch = r.text.match(/\[SCHEDULE:DELETE:(.+?)\]/);
    if (delMatch) {
      r.scheduleDelete = delMatch[1].trim();
      r.text = r.text.replace(/\[SCHEDULE:DELETE:.+?\]/, "").trim();
    }
    // [SCHEDULE:HH:MM:描述] — 创建定时任务
    const schMatch = r.text.match(/\[SCHEDULE:(\d{2}:\d{2}):(.+?)\]/);
    if (schMatch) {
      r.schedule = { time: schMatch[1], content: schMatch[2].trim() };
      r.text = r.text.replace(/\[SCHEDULE:\d{2}:\d{2}:.+?\]/, "").trim();
    }
    // [IMG:路径]
    const imgMatch = r.text.match(/\[IMG:(.+?)\]/);
    if (imgMatch) {
      r.imagePath = imgMatch[1].trim();
      r.text = r.text.replace(/\[IMG:.+?\]/, "").trim();
    }
  }
  return replies;
}

// ── 处理消息：通过 claude --resume -p 唤醒 ──
async function processMessages(messages: QueuedMessage[], scheduledTask: ScheduledTask | null = null): Promise<void> {
  const trigger = scheduledTask ? "⏰定时任务" : "📥QQ消息";
  console.log(`\n${STATUS.DETECTED} ${trigger} ${new Date().toISOString()}`);
  if (scheduledTask) {
    console.log(`   Task: ${scheduledTask.content}`);
    if (scheduledTask.user_openid) console.log(`   Target: ${scheduledTask.user_openid.slice(0, 12)}...`);
  }
  console.log(`   Queue: ${messages.length} message(s) pending`);
  console.log(`   Session: ${SESSION_MASK}`);

  if (!acquireLock()) {
    console.log(`${STATUS.LOCKED} Another processing is in progress — skipping\n`);
    return;
  }

  // 加载并解析用户别名
  const aliases = readAliases();
  for (const msg of messages) {
    getOrCreateAlias(msg.user_openid, aliases);
  }
  writeAliases(aliases);

  // 构建干净的文本提示词（不含 JSON 结构）
  let prompt: string;
  if (scheduledTask) {
    prompt = `⏰ 定时任务提醒：${scheduledTask.content}`;
    if (scheduledTask.user_openid) {
      const sa = getOrCreateAlias(scheduledTask.user_openid, aliases);
      writeAliases(aliases);
      prompt += `，请向 ${sa} 发送消息`;
    }
    prompt += "。\n\n";
  } else {
    prompt = "";
  }

  if (messages.length > 0) {
    prompt += "收到新的QQ消息：";
    for (const msg of messages) {
      const alias = aliases[msg.user_openid];
      prompt += ` [${alias}] ${msg.content}`;
      if (msg.image_paths.length > 0) {
        prompt += ` (附${msg.image_paths.length}张图片: ${msg.image_paths.join(", ")}，请用 bl vision describe --image 路径 先识别再回复)`;
      }
    }
    prompt += " 用 @用户A: 内容 回复。标记: [IMG:路径]发图 [SCHEDULE:HH:MM:描述]创建提醒 [SCHEDULE:LIST]列任务 [SCHEDULE:DELETE:匹配]删任务。不回复则跳过。";
  } else {
    prompt += "请检查是否有需要处理的事项。";
  }

  console.log(`${STATUS.WAKING} Invoking claude --resume ${SESSION_MASK} -p ...`);
  console.log(`   (This call blocks until Claude finishes processing)`);

  const startTime = Date.now();

  try {
    const result =
      await $`claude --resume ${SESSION_ID} -p ${prompt}`.nothrow();

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    if (result.exitCode === 0) {
      // 解析 Claude 的文本回复
      const stdout = result.stdout?.toString() || "";
      const parsed = parseReplies(stdout);

      if (parsed.length > 0) {
        // 别名 → openid 反向映射
        const reverse: { [alias: string]: string } = {};
        for (const [openid, alias] of Object.entries(aliases)) {
          reverse[alias] = openid;
        }

        // 构造 replies.json（脚本负责 JSON 序列化）
        const replyEntries: ReplyEntry[] = [];
        for (const r of parsed) {
          const openid = reverse[r.alias];
          const originalMsg = messages.find(m => m.user_openid === openid);

          // 处理 [SCHEDULE:LIST] — 列出所有定时任务
          if (r.scheduleList) {
            const tasks = readSchedule();
            if (tasks.length === 0) {
              r.text = r.text || "当前没有定时任务。";
            } else {
              const lines: string[] = ["当前定时任务："];
              tasks.forEach((t, i) => {
                const taskAlias = aliases[t.user_openid || ""] || "未知用户";
                lines.push(`${i + 1}. ${t.time} → ${taskAlias}: ${t.content}`);
              });
              r.text = (r.text ? r.text + "\n" : "") + lines.join("\n");
            }
          }

          // 处理 [SCHEDULE:DELETE:匹配] — 删除匹配的任务
          if (r.scheduleDelete) {
            const tasks = readSchedule();
            const pattern = r.scheduleDelete;
            // 先尝试按 HH:MM 精确匹配，再按内容子串匹配
            const idx = tasks.findIndex(t =>
              t.time === pattern || t.content.includes(pattern)
            );
            if (idx >= 0) {
              const removed = tasks.splice(idx, 1)[0];
              writeSchedule(tasks);
              r.text = (r.text ? r.text + " " : "") + `已删除定时任务: ${removed.time} "${removed.content}"`;
              console.log(`   Deleted scheduled task: ${removed.time} "${removed.content}"`);
            } else {
              r.text = (r.text ? r.text + " " : "") + `未找到匹配"${pattern}"的定时任务。`;
            }
          }

          // 跳过既无文字也无图片的空回复
          if (!r.text && !r.imagePath) continue;
          const entry: ReplyEntry = {
            type: originalMsg?.type || "c2c",
            user_openid: openid,
            msg_id: originalMsg?.msg_id,
            text: r.text,
          };
          if (r.imagePath) {
            entry.file_url = r.imagePath;
            entry.file_type = 1; // 1=图片
          }
          replyEntries.push(entry);
        }
        if (replyEntries.length > 0) {
          Bun.write(REPLIES_FILE, JSON.stringify(replyEntries, null, 2));
          console.log(`   Wrote ${replyEntries.length} reply(s) to replies.json`);
        }

        // 处理定时任务创建：写入 scheduled_tasks.json
        for (const r of parsed) {
          if (r.schedule) {
            const openid = reverse[r.alias];
            const tasks = readSchedule();
            tasks.push({ time: r.schedule.time, user_openid: openid, content: r.schedule.content });
            writeSchedule(tasks);
            console.log(`   Scheduled task: ${r.schedule.time} → ${r.alias} "${r.schedule.content}"`);
          }
        }
      } else {
        console.log(`   No @-replies detected in Claude response`);
      }

      // 清空消息队列
      Bun.write(QUEUE_FILE, "[]");
      console.log(`${STATUS.SUCCESS} All messages processed in ${elapsed}s`);
      console.log(`   Queue cleared, ${parsed.length} reply(s) sent\n`);
    } else {
      console.log(`${STATUS.FAILED} Claude exited with code ${result.exitCode} after ${elapsed}s`);
      const stderr = result.stderr?.toString() || "";
      if (stderr) console.log(`   stderr: ${stderr.slice(0, 300)}`);
      console.log("");
    }
  } catch (e: any) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`${STATUS.FAILED} Exception after ${elapsed}s: ${e.message}\n`);
  } finally {
    releaseLock();
  }
}

// ── 主循环 ──
console.log("╔══════════════════════════════════════════════╗");
console.log("║   QQBot Message Detector + Wake Engine       ║");
console.log("╠══════════════════════════════════════════════╣");
console.log(`║   Session:  ${SESSION_MASK.padEnd(19)}       ║`);
console.log(`║   Polling:  every 5 seconds                 ║`);
console.log(`║   Schedule: scheduled_tasks.json (HH:MM)     ║`);
console.log(`║   Wake:     claude --resume <id> -p          ║`);
//console.log(`║   Cooldown: ${COOLDOWN_MS / 1000}s after each wake          ║`);
console.log("╚══════════════════════════════════════════════╝");
console.log("");

let lastMessageCount = 0;
let cooldownUntil = 0;

setInterval(async () => {

  const queue = readQueue();
  const count = queue.length;

  if (count > 0) {
    if (count !== lastMessageCount) {
      lastMessageCount = count;
      await processMessages(queue);
    }
  } else {
    lastMessageCount = 0;
    // 没有QQ消息时，检查定时任务
    const task = checkScheduledTasks();
    if (task) {
      console.log(`\n⏰ Scheduled task triggered: ${task.content}`);
      await processMessages([], task);
    }
  }
}, 5_000);

// 启动时立即检查一次
(async () => {
  const initialQueue = readQueue();
  if (initialQueue.length > 0) {
    lastMessageCount = initialQueue.length;
    await processMessages(initialQueue);
    cooldownUntil = Date.now() + COOLDOWN_MS;
  } else {
    const task = checkScheduledTasks();
    if (task) {
      console.log(`\n⏰ Scheduled task triggered at startup: ${task.content}`);
      await processMessages([], task);
    }
  }
})();

console.log(`${STATUS.IDLE} Detector active — waiting for messages + scheduled tasks...\n`);
