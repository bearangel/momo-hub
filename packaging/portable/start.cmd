@echo off
rem momo-hub portable launcher (Windows x64, bundled Node runtime).
rem
rem Usage:
rem   start.cmd                 listen on 127.0.0.1 (local machine only)
rem   start.cmd lan             auto-detect this machine's LAN IPv4 and bind it
rem   start.cmd 0.0.0.0         bind all interfaces (startup log prints a WARN)
rem   start.cmd 192.168.1.10    bind the given address
rem
rem Env vars win over defaults: an already-set MOMO_HUB_HOST / MOMO_HUB_PORT /
rem MOMO_HUB_DATA_DIR is never overwritten.
rem PowerShell note: `set NAME=value` creates a PowerShell variable, NOT an
rem environment variable. Use $env:NAME="value", or pass the address as an
rem argument to this script instead.
setlocal EnableExtensions
cd /d "%~dp0"

if not exist "data" mkdir "data"
if not defined MOMO_HUB_DATA_DIR set "MOMO_HUB_DATA_DIR=%~dp0data"

if not "%~1"=="" (
  if /i "%~1"=="lan" (
    "%~dp0node.exe" "%~dp0lan-ip.js" > "%~dp0data\lan-ip.txt"
    set /p MOMO_HUB_HOST=<"%~dp0data\lan-ip.txt"
    del "%~dp0data\lan-ip.txt" >nul 2>nul
    if not defined MOMO_HUB_HOST (
      echo [momo-hub] LAN address not detected, falling back to 0.0.0.0
      set "MOMO_HUB_HOST=0.0.0.0"
    )
  ) else (
    set "MOMO_HUB_HOST=%~1"
  )
)

if not defined MOMO_HUB_HOST set "MOMO_HUB_HOST=127.0.0.1"
if not defined MOMO_HUB_PORT set "MOMO_HUB_PORT=8787"
rem Behind a TLS reverse proxy (Caddy/Nginx), uncomment the next line:
rem set "MOMO_HUB_PUBLIC_ORIGIN=https://hub.example.com"

echo [momo-hub] listen: %MOMO_HUB_HOST%:%MOMO_HUB_PORT%
echo [momo-hub] data  : %MOMO_HUB_DATA_DIR%
if "%MOMO_HUB_HOST%"=="127.0.0.1" (
  echo [momo-hub] local only. For other nodes: start.cmd lan  ^|  start.cmd ^<LAN-IP^>
) else (
  echo [momo-hub] other nodes should use: http://%MOMO_HUB_HOST%:%MOMO_HUB_PORT%
)
echo.

"%~dp0node.exe" "%~dp0app\dist\index.js"
echo.
echo [momo-hub] exited with code %ERRORLEVEL%
pause
