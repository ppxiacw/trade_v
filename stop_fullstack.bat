@echo off
setlocal

echo Stopping backend/frontend windows...
powershell -NoProfile -Command "$ErrorActionPreference='SilentlyContinue'; $procs = Get-Process; foreach($p in $procs){ if($p.MainWindowTitle -like 'trade_v backend*' -or $p.MainWindowTitle -like 'trader_front frontend*'){ Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } }" >nul 2>nul

echo Releasing common dev ports (5000, 5173) if still occupied...
for %%P in (5000 5173) do (
  for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%%P"') do (
    taskkill /PID %%a /T /F >nul 2>nul
  )
)

echo Done.
endlocal
