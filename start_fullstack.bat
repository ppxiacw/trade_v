@echo off
setlocal EnableExtensions
chcp 65001 >nul

REM Script location (backend repo: trade_v)
set "BACKEND_DIR=%~dp0"
if "%BACKEND_DIR:~-1%"=="\" set "BACKEND_DIR=%BACKEND_DIR:~0,-1%"
for %%I in ("%BACKEND_DIR%\..") do set "WORKSPACE_DIR=%%~fI"
set "FRONTEND_DIR=%WORKSPACE_DIR%\trader_front"
set "VENV_PY=%BACKEND_DIR%\.venv\Scripts\python.exe"
set "PYTHON_EXE="
set "FRONTEND_VITE_BIN=%FRONTEND_DIR%\node_modules\.bin\vite.cmd"
set "FRONTEND_PORT=5173"
set "LOG_DIR=%BACKEND_DIR%\logs"
set "BACKEND_LOG=%LOG_DIR%\backend.log"
set "BACKEND_ERR_LOG=%LOG_DIR%\backend.err.log"
set "FRONTEND_LOG=%LOG_DIR%\frontend.log"
set "FRONTEND_ERR_LOG=%LOG_DIR%\frontend.err.log"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if not exist "%FRONTEND_DIR%\package.json" (
  echo [ERROR] Frontend directory not found. Expected sibling folder: trader_front
  pause
  exit /b 1
)

if not exist "%BACKEND_DIR%\app.py" (
  echo [ERROR] Backend app.py not found.
  pause
  exit /b 1
)

where npm >nul 2>nul
if errorlevel 1 (
  echo [ERROR] npm not found in PATH.
  echo Please install Node.js 20+ and reopen the terminal.
  pause
  exit /b 1
)

REM Always use project virtualenv (.venv); never fallback to system/conda python.
REM requirements currently pins numpy 1.26.x, which is not compatible with Python 3.13.
REM Therefore we force .venv to use Python 3.12/3.11 when possible.

if exist "%VENV_PY%" (
  "%VENV_PY%" -c "import sys; raise SystemExit(0 if sys.version_info < (3, 13) else 1)" >nul 2>nul
  if errorlevel 1 (
    echo [WARN] Existing .venv is Python 3.13+, recreating with Python 3.12/3.11...
    rmdir /s /q "%BACKEND_DIR%\.venv"
  )
)

if not exist "%VENV_PY%" (
  echo [INFO] Creating backend virtual environment: .venv
  py -3.12 -m venv "%BACKEND_DIR%\.venv" >nul 2>nul
  if errorlevel 1 (
    py -3.11 -m venv "%BACKEND_DIR%\.venv" >nul 2>nul
  )
  if errorlevel 1 (
    py -3.10 -m venv "%BACKEND_DIR%\.venv" >nul 2>nul
  )
  if errorlevel 1 (
    python -m venv "%BACKEND_DIR%\.venv"
  )
)

if not exist "%VENV_PY%" (
  echo [ERROR] Failed to create virtual environment.
  echo Please run manually:
  echo   python -m venv .venv
  echo   .venv\Scripts\python -m pip install -r requirements.txt
  pause
  exit /b 1
)

set "PYTHON_EXE=%VENV_PY%"

REM Check backend dependency. If missing, install requirements in .venv.
"%PYTHON_EXE%" -c "import flask, mysql.connector, sqlalchemy" >nul 2>nul
if errorlevel 1 (
  echo [INFO] Installing backend dependencies from requirements.txt...
  set "HTTP_PROXY="
  set "HTTPS_PROXY="
  set "ALL_PROXY="
  set "NO_PROXY=*"
  set "PIP_NO_PROXY=*"
  "%PYTHON_EXE%" -m pip install -r "%BACKEND_DIR%\requirements.txt" ^
    --index-url https://pypi.tuna.tsinghua.edu.cn/simple ^
    --trusted-host pypi.tuna.tsinghua.edu.cn ^
    --trusted-host pypi.org ^
    --trusted-host files.pythonhosted.org
  if errorlevel 1 (
    echo [ERROR] Dependency installation failed.
    echo [TIP] Please check network/SSL settings and retry.
    pause
    exit /b 1
  )
)

if not exist "%FRONTEND_VITE_BIN%" (
  echo [INFO] Installing frontend dependencies...
  pushd "%FRONTEND_DIR%"
  call npm install
  set "NPM_INSTALL_EXIT=%ERRORLEVEL%"
  popd
  if not "%NPM_INSTALL_EXIT%"=="0" (
    echo [ERROR] Frontend dependency installation failed.
    pause
    exit /b 1
  )
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo [INFO] Checking and stopping existing backend/frontend services...
call :stop_existing_services
ping -n 2 127.0.0.1 >nul
call :stop_port_listener 5000
call :stop_port_listener 5173
call :stop_port_listener 5174
call :stop_port_listener 5175

echo Starting backend...
call :write_log_header "%BACKEND_LOG%" "trade_v backend stdout"
call :write_log_header "%BACKEND_ERR_LOG%" "trade_v backend stderr"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$p = Start-Process -FilePath '%PYTHON_EXE%' -ArgumentList @('%BACKEND_DIR%\app.py') -WorkingDirectory '%BACKEND_DIR%' -WindowStyle Hidden -RedirectStandardOutput '%BACKEND_LOG%' -RedirectStandardError '%BACKEND_ERR_LOG%' -PassThru;" ^
  "Write-Host ('[INFO] Backend PID ' + $p.Id + ' logging to logs\backend.log')"

echo Starting frontend...
call :write_log_header "%FRONTEND_LOG%" "trader_front frontend stdout"
call :write_log_header "%FRONTEND_ERR_LOG%" "trader_front frontend stderr"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$p = Start-Process -FilePath 'npm.cmd' -ArgumentList @('run','dev','--','--port','%FRONTEND_PORT%','--strictPort') -WorkingDirectory '%FRONTEND_DIR%' -WindowStyle Hidden -RedirectStandardOutput '%FRONTEND_LOG%' -RedirectStandardError '%FRONTEND_ERR_LOG%' -PassThru;" ^
  "Write-Host ('[INFO] Frontend PID ' + $p.Id + ' logging to logs\frontend.log')"

echo Done. Services are running in background:
echo - Backend:  http://127.0.0.1:5000
echo - Frontend: http://127.0.0.1:%FRONTEND_PORT%
echo [TIP] Script auto-clears listeners on ports 5000/5173-5175 before launch.
echo [TIP] Logs:
echo   Backend stdout:  logs\backend.log
echo   Backend stderr:  logs\backend.err.log
echo   Frontend stdout: logs\frontend.log
echo   Frontend stderr: logs\frontend.err.log

endlocal
exit /b 0

:write_log_header
set "HEADER_FILE=%~1"
set "HEADER_NAME=%~2"
> "%HEADER_FILE%" echo ===== %HEADER_NAME% =====
>> "%HEADER_FILE%" echo Started at %DATE% %TIME%
>> "%HEADER_FILE%" echo.
exit /b 0

:stop_existing_services
REM 仅按端口/窗口标题清理，避免误杀当前正在执行本脚本的 cmd（命令行也含 trade_v 路径）
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='SilentlyContinue';" ^
  "$ids = New-Object 'System.Collections.Generic.HashSet[int]';" ^
  "foreach($port in @(5000,5173,5174,5175)){" ^
  "  Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue | ForEach-Object {" ^
  "    if($_.OwningProcess -gt 0){ [void]$ids.Add([int]$_.OwningProcess) }" ^
  "  }" ^
  "};" ^
  "Get-Process -ErrorAction SilentlyContinue | Where-Object {" ^
  "  $_.MainWindowTitle -like 'trade_v backend*' -or $_.MainWindowTitle -like 'trader_front frontend*'" ^
  "} | ForEach-Object { [void]$ids.Add([int]$_.Id) };" ^
  "$self = $PID;" ^
  "try { $parent = (Get-CimInstance Win32_Process -Filter ('ProcessId=' + $self)).ParentProcessId; if($parent){ [void]$ids.Remove($parent) } } catch {};" ^
  "foreach($id in $ids){ if($id -ne $self){ Write-Host ('[INFO] Stopping PID ' + $id); Stop-Process -Id $id -Force -ErrorAction SilentlyContinue } }"
exit /b 0

:stop_port_listener
set "TARGET_PORT=%~1"
set "FOUND_PID=0"

REM Preferred: powershell query is locale-independent and handles IPv4/IPv6.
for /f %%P in ('powershell -NoProfile -Command "$ErrorActionPreference='SilentlyContinue'; Get-NetTCPConnection -State Listen -LocalPort %TARGET_PORT% | Select-Object -ExpandProperty OwningProcess -Unique" 2^>nul') do (
  call :kill_pid_if_needed %%P %TARGET_PORT%
)

REM Fallback: netstat text matching (for environments without Get-NetTCPConnection).
for /f "tokens=5" %%P in ('netstat -ano -p tcp ^| findstr /R /C:":%TARGET_PORT% .*LISTENING" /C:":%TARGET_PORT% .*LISTEN" /C:":%TARGET_PORT% .*侦听"') do (
  call :kill_pid_if_needed %%P %TARGET_PORT%
)

call :wait_port_release %TARGET_PORT%
exit /b 0

:kill_pid_if_needed
set "KILL_PID=%~1"
set "KILL_PORT=%~2"
if "%KILL_PID%"=="" exit /b 0
if "%KILL_PID%"=="0" exit /b 0
echo [INFO] Stopping PID %KILL_PID% on port %KILL_PORT%...
set "FOUND_PID=1"
taskkill /F /T /PID %KILL_PID% >nul 2>nul
exit /b 0

:wait_port_release
set "WAIT_PORT=%~1"
set /a "WAIT_TRIES=0"
:wait_port_release_loop
set /a "WAIT_TRIES+=1"
powershell -NoProfile -Command "$ErrorActionPreference='SilentlyContinue'; $c=Get-NetTCPConnection -State Listen -LocalPort %WAIT_PORT% | Select-Object -First 1; if($c){exit 1}else{exit 0}" >nul 2>nul
if not errorlevel 1 exit /b 0
if %WAIT_TRIES% GEQ 10 (
  echo [WARN] Port %WAIT_PORT% still occupied after retries.
  exit /b 0
)
ping -n 2 127.0.0.1 >nul
goto :wait_port_release_loop
