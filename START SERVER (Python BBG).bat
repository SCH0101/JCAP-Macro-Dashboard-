@echo off
cd /d "%~dp0"
REM Self-contained: uses this project's own venv (.venv) which has
REM xbbg + blpapi + httpx installed. The local data agent lives in
REM tools\bloomberg_api.py. Requires a logged-in Bloomberg Terminal (DAPI 8194).
set "PORT=8083"
".venv\Scripts\python.exe" server_bbg.py
pause
