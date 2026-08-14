@echo off
REM QQ Bot Gateway 启动脚本
REM 设置 QQ API 凭证后启动独立网关 + 消息检测
cd /d "%~dp0"

REM ⚠️ 请填写你的 QQ Bot 凭证（从 QQ 开放平台 https://q.qq.com 获取）
set QQ_APPID=你的AppID
set QQ_APP_SECRET=你的AppSecret

echo Starting QQBot Gateway...
start "qqbot-gateway" cmd /c "bun qqbot-gateway.ts || pause"

echo Starting Message Detector...
start "check-pending" cmd /c "bun check-pending.ts || pause"

echo.
echo Both windows should now be open.
echo If a window flashes and closes, check the error above.
