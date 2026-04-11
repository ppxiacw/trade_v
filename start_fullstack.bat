@echo off
setlocal EnableExtensions

REM Script location (backend repo: trade_v)
set "BACKEND_DIR=%~dp0"
if "%BACKEND_DIR:~-1%"=="\" set "BACKEND_DIR=%BACKEND_DIR:~0,-1%"
for %%I in ("%BACKEND_DIR%\..") do set "WORKSPACE_DIR=%%~fI"
set "FRONTEND_DIR=%WORKSPACE_DIR%\trader_front"
set "VENV_PY=%BACKEND_DIR%\.venv\Scripts\python.exe"
set "PYTHON_EXE="
set "FRONTEND_VITE_BIN=%FRONTEND_DIR%\node_modules\.bin\vite.cmd"
set "FRONTEND_PORT=5173"

if not exist "%FRONTEND_DIR%\package.json" (
  echo [ERROR] Frontend directory not found: "%FRONTEND_DIR%"
  pause
  exit /b 1
)

if not exist "%BACKEND_DIR%\app.py" (
  echo [ERROR] Backend app.py not found: "%BACKEND_DIR%\app.py"
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
  echo [INFO] Creating backend virtual environment: %BACKEND_DIR%\.venv
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

echo [INFO] Cleaning previous local dev servers...
call :stop_port_listener 5000
call :stop_port_listener 5173
call :stop_port_listener 5174
call :stop_port_listener 5175

echo Starting backend...
start "trade_v backend" cmd /k "cd /d ""%BACKEND_DIR%"" && title trade_v backend && ""%PYTHON_EXE%"" app.py"

echo Starting frontend...
start "trader_front frontend" cmd /k "cd /d ""%FRONTEND_DIR%"" && title trader_front frontend && npm run dev -- --port %FRONTEND_PORT% --strictPort"

echo Done. Two windows opened:
echo - Backend:  http://127.0.0.1:5000
echo - Frontend: http://127.0.0.1:%FRONTEND_PORT%
echo [TIP] Script will stop old listeners on ports 5000/5173-5175 before each start.

endlocal
exit /b 0

:stop_port_listener
set "TARGET_PORT=%~1"
for /f "tokens=5" %%P in ('netstat -ano -p tcp ^| findstr /R /C:":%TARGET_PORT% .*LISTENING"') do (
  if not "%%P"=="0" (
    echo [INFO] Stopping PID %%P on port %TARGET_PORT%...
    taskkill /F /PID %%P >nul 2>nul
  )
)
exit /b 0
