# 机娘（jiniang）

通过 QQ 官方机器人 API 让 [Claude Code](https://claude.ai/code)（或任何能响应 prompt 的智能体）收发消息的通道：在手机 QQ 上给机器人发消息，智能体在本机处理并回复，支持图片识别、文件处理和富媒体回复。

> 本项目源自 [tencent-connect/openclaw-qqbot](https://github.com/tencent-connect/openclaw-qqbot)（MIT，作者 sliverp / Tencent Connect）与 [ZhongtaoWang/qqbot-claude-channel](https://github.com/ZhongtaoWang/qqbot-claude-channel)（MIT），在此致谢上游。

## 架构

三层独立、事件驱动，智能体离线时也能收消息：

```
qqbot-gateway.ts   ←→ QQ WebSocket（收）+ QQ API（发）
        ↓ pending_messages.json      ↑ replies.json
check-pending.ts   30s 轮询 → 读队列 → 别名映射 → 唤醒智能体
        ↓ 纯文本 prompt
智能体（Claude Code） 按需唤醒，输出 @用户XX: 格式回复
```

JSON 序列化/反序列化由脚本负责，智能体只处理干净的对话文本。

## 主要文件

| 文件 | 作用 |
|---|---|
| `qqbot-gateway.ts` | 全双工网关：WebSocket 收 + HTTP 发，独立进程 |
| `check-pending.ts` | 唤醒脚本：轮询 → 读队列/别名 → 唤醒智能体 → 解析回复 → 写回复队列 |
| `OriginalQQBot/qqbot.ts` | MCP 服务器：智能体在线时替代 gateway |

## 部署

### 手动部署

1. 安装 [Bun](https://bun.sh) >= 1.0 与 [Claude Code](https://claude.ai/code) CLI >= 2.1.87。
2. 在 [QQ 开放平台](https://q.qq.com) 申请机器人账号，取得 AppID / AppSecret。
3. 克隆并安装依赖：

```bash
git clone https://github.com/think20/jiniang.git
cd jiniang
bun install
```

4. 配置环境变量（复制 `.env.example` 为 `.env` 并填入真实凭证，见下表）。
5. 启动网关与检测器：

```bash
start-gateway.bat        # Windows 一键启动
# 或手动分别启动：
bun qqbot-gateway.ts
bun check-pending.ts
```

### 智能体部署（供智能体自主执行）

本项目可被任意智能体（Claude Code、DSH 等）自主部署。全部命令非交互、可验证，按序执行：

1. **校验工具链**：`bun --version` 与 `claude --version` 均返回版本号（退出码 0）。
2. **准备凭证**：把 QQ AppID / AppSecret 写入环境变量或 `.env`（见下表）。
3. **安装依赖**：`bun install`，退出码 0。
4. **准备会话**：用 `claude --resume list` 取得要长期使用的会话 ID，写入 `CLAUDE_SESSION_ID`。
5. **启动网关**：后台常驻运行 `bun qqbot-gateway.ts`，日志出现 access token 刷新即为连上。
6. **启动检测器**：后台常驻运行 `bun check-pending.ts`。
7. **验收**：用手机 QQ 向机器人发一条消息，确认检测器日志出现 `[STATUS:DETECTED]`，且智能体回复后队列清空。

> 唤醒机制：检测器每 30 秒轮询一次 `pending_messages.json`；有新消息时通过 `claude --resume <CLAUDE_SESSION_ID> -p <prompt>` 唤醒智能体，解析其文本回复后写入 `replies.json`，由网关发送。

## 智能体接口协议（被唤醒后如何回复）

`check-pending.ts` 会把消息提取成纯文本 prompt（`[用户XX] 消息内容`）交给智能体。智能体按以下纯文本格式回复，脚本负责解析与 JSON 序列化：

- **普通回复**：`@用户A: 回复内容`，多行直接续写。
- **发送图片**：在文字末尾追加 `[IMG:完整绝对路径]`。
- **创建定时任务**：`@用户A: 好的 [SCHEDULE:08:00:提醒内容]`。
- **列出任务**：`@用户A: [SCHEDULE:LIST]`。
- **删除任务**：`@用户A: 已删除 [SCHEDULE:DELETE:08:00]`。

不需要回复的消息直接跳过，不输出对应行；不要直接读写 `pending_messages.json` / `replies.json`。

## 配置

环境变量（复制 `.env.example` 为 `.env`）：

| 变量 | 必填 | 说明 |
|---|---|---|
| `QQ_APPID` | 是 | QQ 机器人 AppID |
| `QQ_APP_SECRET` | 是 | QQ 机器人 AppSecret |
| `QQ_ALLOWED_USERS` | 否 | 用户 openid 白名单，逗号分隔，空则允许所有人 |
| `QQ_SANDBOX` | 否 | `"true"` 使用沙箱 API，默认 `"false"` |
| `CLAUDE_SESSION_ID` | 否 | 被唤醒的 Claude Code 会话 ID |

MCP 方式：复制 `.mcp.json.example` 为 `.mcp.json`，或在任意目录运行：

```bash
claude mcp add qqbot \
  --scope user \
  --env QQ_APPID=你的AppID \
  --env QQ_APP_SECRET=你的AppSecret \
  -- bun ./OriginalQQBot/qqbot.ts
```

## 使用场景

- **远程执行任务**：手机 QQ 上发指令，智能体在本机写代码、改文件、跑脚本
- **图片分析**：发截图或照片，智能体识别后回复
- **文件处理**：发 PDF / DOCX / 代码文件，智能体读取后回复
- **工具权限审批**：敏感操作通过 QQ 消息询问，回复 `yes {id}` / `no {id}` / `always {id}`

## 许可

[MIT](LICENSE)。本项目继承上游 [openclaw-qqbot](https://github.com/tencent-connect/openclaw-qqbot)（sliverp / Tencent Connect）与 [qqbot-claude-channel](https://github.com/ZhongtaoWang/qqbot-claude-channel)（ZhongtaoWang）的 MIT 许可，版权声明见 `LICENSE`。
