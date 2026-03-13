@echo off
setlocal

cd /d "%~dp0"

set "APP_NAME=KeepaMonthlySales"
set "VENV_PY=.venv\Scripts\python.exe"
set "DIST_DIR=dist\%APP_NAME%"

echo === Build start ===

if not exist "%VENV_PY%" (
    echo [ERROR] 仮想環境の Python が見つかりません: %VENV_PY%
    echo 先に py -m venv .venv を実行してください。
    goto :error
)

echo [INFO] Python: %VENV_PY%
"%VENV_PY%" -c "import sys; print(sys.executable)"
if errorlevel 1 goto :error

echo [INFO] Installing requirements...
"%VENV_PY%" -m pip install -r requirements.txt
if errorlevel 1 goto :error

echo [INFO] Building exe with PyInstaller...
"%VENV_PY%" -m PyInstaller --noconfirm --clean --onedir --name "%APP_NAME%" keepa_enrich.py
if errorlevel 1 goto :error

if not exist "%DIST_DIR%\%APP_NAME%.exe" (
    echo [ERROR] ビルド成果物が見つかりません: %DIST_DIR%\%APP_NAME%.exe
    goto :error
)

echo [INFO] Copying launcher batch files...
copy /Y "run.bat" "%DIST_DIR%\" >nul
if errorlevel 1 goto :error
copy /Y "run_burst.bat" "%DIST_DIR%\" >nul
if errorlevel 1 goto :error
copy /Y "run_drip.bat" "%DIST_DIR%\" >nul
if errorlevel 1 goto :error

echo [INFO] Copying README...
if exist "README_keepa.md" (
    copy /Y "README_keepa.md" "%DIST_DIR%\" >nul
    if errorlevel 1 goto :error
)

echo [INFO] Copying config...
if exist "config.ini" (
    copy /Y "config.ini" "%DIST_DIR%\" >nul
    if errorlevel 1 goto :error
) else (
    copy /Y "config.ini.example" "%DIST_DIR%\config.ini" >nul
    if errorlevel 1 goto :error
)

echo [INFO] Copying input/output support files if present...
if exist "output.xlsx" (
    copy /Y "output.xlsx" "%DIST_DIR%\" >nul
    if errorlevel 1 goto :error
)

if exist "asin_cache.csv" (
    copy /Y "asin_cache.csv" "%DIST_DIR%\" >nul
    if errorlevel 1 goto :error
)

echo.
echo [OK] Build completed.
echo [INFO] Release folder: %CD%\%DIST_DIR%
echo.
echo 配布内容:
echo   %DIST_DIR%\%APP_NAME%.exe
echo   %DIST_DIR%\run.bat
echo   %DIST_DIR%\run_burst.bat
echo   %DIST_DIR%\run_drip.bat
echo   %DIST_DIR%\config.ini
echo   %DIST_DIR%\README_keepa.md
echo.
pause
exit /b 0

:error
echo.
echo [ERROR] Build failed.
pause
exit /b 1