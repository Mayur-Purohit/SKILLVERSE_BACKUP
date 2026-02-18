@echo off
REM SkillVerse Quick Start Script for Windows
REM This script sets up and runs the SkillVerse application

echo ========================================
echo SkillVerse - Quick Start
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8 or higher from python.org
    pause
    exit /b 1
)

echo [1/4] Installing dependencies...
pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo [2/4] Setting up environment...
if not exist .env (
    copy .env.example .env
    echo Created .env file from template
)

echo.
echo [3/4] Initializing database...
python init_db.py
if errorlevel 1 (
    echo ERROR: Failed to initialize database
    pause
    exit /b 1
)

echo.
echo [4/4] Starting application...
echo.
echo ========================================
echo Application is starting...
echo.
echo Access the application at: http://localhost:5000
echo.
echo Default Admin Login:
echo   Email: admin@skillverse.com
echo   Password: admin123
echo.
echo Press Ctrl+C to stop the server
echo ========================================
echo.

python app.py
