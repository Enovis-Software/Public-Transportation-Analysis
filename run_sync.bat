@echo off
REM Database Sync Script Runner for Windows
REM This batch file runs the database sync script

echo Starting Database Sync...
echo.

REM Change to script directory
cd /d "%~dp0"

REM Run Python script
python main.py

REM Check exit code
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Sync failed with exit code %ERRORLEVEL%
    pause
    exit /b %ERRORLEVEL%
) else (
    echo.
    echo Sync completed successfully!
)

pause

