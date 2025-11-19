@echo off
echo ========================================
echo WARNING: Reset Databases
echo ========================================
echo.
echo This will DELETE ALL DATA in the databases!
echo.
set /p confirm="Are you sure? Type 'yes' to continue: "

if /i "%confirm%"=="yes" (
    echo.
    echo Stopping and removing containers...
    docker-compose down -v
    
    echo.
    echo Starting fresh databases...
    docker-compose up -d
    
    echo.
    echo Waiting for initialization...
    timeout /t 30 /nobreak
    
    echo.
    echo Databases reset complete!
) else (
    echo.
    echo Reset cancelled.
)

pause