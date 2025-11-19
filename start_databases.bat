@echo off
echo ========================================
echo Starting Text2SQL Databases
echo ========================================

echo.
echo Checking Docker...
docker --version
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Docker is not running!
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo.
echo Starting Docker containers...
docker-compose up -d

echo.
echo Waiting for databases to initialize (30 seconds)...
timeout /t 30 /nobreak

echo.
echo Checking container status...
docker-compose ps

echo.
echo ========================================
echo Databases started successfully!
echo ========================================
echo.
echo MySQL:      localhost:3306
echo MariaDB:    localhost:3307
echo phpMyAdmin: http://localhost:8080
echo.
echo Press any key to exit...
pause > nul