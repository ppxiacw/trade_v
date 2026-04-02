@echo off
setlocal

echo Stopping backend/frontend windows...
taskkill /FI "WINDOWTITLE eq trade_v backend*" /T /F >nul 2>nul
taskkill /FI "WINDOWTITLE eq trader_front frontend*" /T /F >nul 2>nul

echo Releasing common dev ports (5000, 5173) if still occupied...
for %%P in (5000 5173) do (
  for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%%P"') do (
    taskkill /PID %%a /T /F >nul 2>nul
  )
)

echo Done.
endlocal
