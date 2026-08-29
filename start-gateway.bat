@echo off
REM QQ Bot Gateway 启动脚本（图形界面版，单一窗口）
REM 启动网关 + 外部时钟（消息检测器），并在浏览器打开统一监控控制台
REM 原双窗口模式请使用 start-gateway-console.bat
cd /d "%~dp0"

REM ⚠️ 请填写你的 QQ Bot 凭证（从 QQ 开放平台 https://q.qq.com 获取）
set QQ_APPID=你的AppID
set QQ_APP_SECRET=你的AppSecret

echo Starting QQBot Gateway GUI (single window)...
start "qqbot-gateway-gui" cmd /c "bun gateway-gui/server.ts || pause"

echo.
echo GUI console opened at http://127.0.0.1:8989 (browser should open automatically).
echo Gateway + external clock run inside the GUI window.
echo Close the "qqbot-gateway-gui" window or press Ctrl+C there to stop everything.
