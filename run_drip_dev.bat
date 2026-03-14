@echo off
rem Keepa Monthly Sales - drip mode dev launcher for cmd.exe
rem Run this .bat directly from cmd.exe or PowerShell (do not run via "python ...")
cd /d "%~dp0"

set "PYTHON_CMD=python"
if exist ".venv\Scripts\python.exe" (
  set "PYTHON_CMD=.venv\Scripts\python.exe"
)

"%PYTHON_CMD%" keepa_enrich.py --mode drip --tokens-per-minute 5 --interval-seconds 60 --max-minutes 480 --stop-when-tokens-below 10 --max-zero-budget-cycles 3 --max-token-status-failures 3
if errorlevel 1 (
  echo エラーで終了しました。keepa_enrich.log を確認してください。
) else (
  echo 正常終了しました。
)

pause
