@echo off
echo ========================================
echo Database Logs
echo ========================================
echo.
echo 1. View MySQL logs
echo 2. View MariaDB logs
echo 3. View all logs
echo.
set /p choice="Select option (1-3): "

if "%choice%"=="1" (
    docker-compose logs mysql
) else if "%choice%"=="2" (
    docker-compose logs mariadb
) else if "%choice%"=="3" (
    docker-compose logs
) else (
    echo Invalid choice
)

pause