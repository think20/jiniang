# qqbot-claude-channel

通过 QQ 官方机器人 API 让 [Claude Code](https://claude.ai/code) 收发消息的通道：在手机 QQ 上给机器人发消息，Claude 在本机处理并回复，支持图片识别、文件处理和富媒体回复。

> 本项目参考 [OpenClaw QQBot](https://github.com/tencent-connect/openclaw-qqbot)。

## 架构

三层独立、事件驱动，Claude 离线时也能收消息：

```
qqbot-gateway.ts   ←→ QQ WebSocket（收）+ QQ API（发）
        ↓ pending_messages.json      ↑ replies.json
check-pending.ts   30s 轮询 → 读队列 → 别名映射 → claude --resume -p 唤醒
        ↓ 纯文本 prompt
Claude Code         按需唤醒，输出 @用户XX: 格式回复
```

JSON 序列化/反序列化由脚本负责，Claude 只处理干净的对话文本。

## 主要文件

| 文件 | 作用 |
|---|---|
| `qqbot-gateway.ts` | 全双工网关：WebSocket 收 + HTTP 发，独立进程 |
| `check-pending.ts` | 唤醒脚本：轮询 → 读队列/别名 → 唤醒 Claude → 解析回复 → 写回复队列 |
| `OriginalQQBot/qqbot.ts` | MCP 服务器：Claude 在线时替代 gateway |

## 前置条件

- [Bun](https://bun.sh) >= 1.0
- [Claude Code](https://claude.ai/code) CLI >= 2.1.87
- 在 [QQ 开放平台](https://q.qq.com) 申请的机器人账号（AppID / AppSecret）

## 配置

复制 `.env.example` 为 `.env`（或直接设置环境变量）：

| 变量 | 必填 | 说明 |
|---|---|---|
| `QQ_APPID` | 是 | QQ 机器人 AppID |
| `QQ_APP_SECRET` | 是 | QQ 机器人 AppSecret |
| `QQ_ALLOWED_USERS` | 否 | 用户 openid 白名单，逗号分隔，空则允许所有人 |
| `QQ_SANDBOX` | 否 | `"true"` 使用沙箱 API，默认 `"false"` |
| `CLAUDE_SESSION_ID` | 否 | `check-pending.ts` 唤醒的会话 ID（`claude --resume list` 查看） |

MCP 方式：复制 `.mcp.json.example` 为 `.mcp.json`，或在任意目录运行：

```bash
claude mcp add qqbot \
  --scope user \
  --env QQ_APPID=你的AppID \
  --env QQ_APP_SECRET=你的AppSecret \
  -- bun ./OriginalQQBot/qqbot.ts
```

## 启动

```bash
# Windows：独立网关 + 检测器
start-gateway.bat

# 或手动
bun qqbot-gateway.ts
bun check-pending.ts
```

## 使用场景

- **远程执行任务**：手机 QQ 上发指令，Claude 在本机写代码、改文件、跑脚本
- **图片分析**：发截图或照片，Claude 识别后回复
- **文件处理**：发 PDF / DOCX / 代码文件，Claude 读取后回复
- **工具权限审批**：敏感操作通过 QQ 消息询问，回复 `yes {id}` / `no {id}` / `always {id}`

## 许可

[MIT](LICENSE)
