@echo off
setlocal

REM Script location (backend repo: trade_v)
set "BACKEND_DIR=%~dp0"
for %%I in ("%BACKEND_DIR%..") do set "WORKSPACE_DIR=%%~fI"
set "FRONTEND_DIR=%WORKSPACE_DIR%\trader_front"
set "VENV_PY=%BACKEND_DIR%.venv\Scripts\python.exe"
set "PYTHON_EXE="

if not exist "%FRONTEND_DIR%\package.json" (
  echo [ERROR] Frontend directory not found: "%FRONTEND_DIR%"
  pause
  exit /b 1
)

if not exist "%BACKEND_DIR%app.py" (
  echo [ERROR] Backend app.py not found: "%BACKEND_DIR%app.py"
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
    rmdir /s /q "%BACKEND_DIR%.venv"
  )
)

if not exist "%VENV_PY%" (
  echo [INFO] Creating backend virtual environment: %BACKEND_DIR%.venv
  py -3.12 -m venv "%BACKEND_DIR%.venv" >nul 2>nul
  if errorlevel 1 (
    py -3.11 -m venv "%BACKEND_DIR%.venv" >nul 2>nul
  )
  if errorlevel 1 (
    py -3.10 -m venv "%BACKEND_DIR%.venv" >nul 2>nul
  )
  if errorlevel 1 (
    python -m venv "%BACKEND_DIR%.venv"
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

REM Check backend dependency (Flask). If missing, install requirements in .venv.
"%PYTHON_EXE%" -c "import flask" >nul 2>nul
if errorlevel 1 (
  echo [INFO] Installing backend dependencies from requirements.txt...
  set "HTTP_PROXY="
  set "HTTPS_PROXY="
  set "ALL_PROXY="
  set "NO_PROXY=*"
  set "PIP_NO_PROXY=*"
  "%PYTHON_EXE%" -m pip install -r "%BACKEND_DIR%requirements.txt" ^
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

echo Starting backend...
start "trade_v backend" cmd /k "cd /d ""%BACKEND_DIR%"" && ""%PYTHON_EXE%"" app.py"

echo Starting frontend...
start "trader_front frontend" cmd /k "cd /d ""%FRONTEND_DIR%"" && npm run dev"

echo Done. Two windows opened:
echo - trade_v backend
echo - trader_front frontend

endlocal
