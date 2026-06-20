@echo off
setlocal EnableExtensions

cd /d "%~dp0"
set "PY=%~dp0.venv\Scripts\python.exe"
set "APP=%~dp0app.py"

if not exist "%PY%" (
  echo [ERROR] 未找到虚拟环境 Python:
  echo   %PY%
  echo 请先运行 start_fullstack.bat 或: py -3.12 -m venv .venv
  pause
  exit /b 1
)

if not exist "%APP%" (
  echo [ERROR] 未找到 app.py:
  echo   %APP%
  pause
  exit /b 1
)

title trade_v backend
echo [INFO] Backend: %PY%
echo [INFO] App:     %APP%
echo.

"%PY%" "%APP%"
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] Backend exited with code %EXIT_CODE%
  pause
)
exit /b %EXIT_CODE%
