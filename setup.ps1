# setup.ps1 - Complete Windows setup script
# Run as: .\setup.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Text2SQL Project Setup (Windows)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check prerequisites
Write-Host "Step 1: Checking prerequisites..." -ForegroundColor Yellow

# Check Docker
try {
    $dockerVersion = docker --version
    Write-Host "   ✓ Docker installed: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Docker not found! Please install Docker Desktop." -ForegroundColor Red
    exit 1
}

# Check Docker Compose
try {
    $composeVersion = docker-compose --version
    Write-Host "   ✓ Docker Compose: $composeVersion" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Docker Compose not found!" -ForegroundColor Red
    exit 1
}

# Check Python
try {
    $pythonVersion = python --version
    Write-Host "   ✓ Python: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "   ✗ Python not found! Please install Python 3.8+." -ForegroundColor Red
    exit 1
}

# Step 2: Create directory structure
Write-Host ""
Write-Host "Step 2: Creating directory structure..." -ForegroundColor Yellow

$directories = @(
    "data\mysql",
    "data\mariadb",
    "data\processed",
    "datasets_source\data",
    "database",
    "models",
    "scripts",
    "results",
    "notebooks",
    "docs\figures"
)

foreach ($dir in $directories) {
    if (!(Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "   ✓ Created: $dir" -ForegroundColor Green
    } else{
        Write-Host "   ○ Exists: $dir" -ForegroundColor Gray
    }
}

# Step 3: Create .env file if not exists
Write-Host ""
Write-Host "Step 3: Checking .env file..." -ForegroundColor Yellow

if (!(Test-Path ".env")) {
    $envContent = @"
# Database credentials
MYSQL_ROOT_PASSWORD=root123
MYSQL_DATABASE=text2sql_db
MYSQL_USER=text2sql_user
MYSQL_PASSWORD=text2sql_pass

MARIADB_ROOT_PASSWORD=root123
MARIADB_DATABASE=text2sql_db
MARIADB_USER=text2sql_user
MARIADB_PASSWORD=text2sql_pass
"@
    $envContent | Out-File -FilePath ".env" -Encoding UTF8
    Write-Host "   ✓ Created .env file" -ForegroundColor Green
} else {
    Write-Host "   ○ .env file exists" -ForegroundColor Gray
}

# Step 4: Create virtual environment
Write-Host ""
Write-Host "Step 4: Setting up Python virtual environment..." -ForegroundColor Yellow

if (!(Test-Path "venv")) {
    python -m venv venv
    Write-Host "   ✓ Virtual environment created" -ForegroundColor Green
} else {
    Write-Host "   ○ Virtual environment exists" -ForegroundColor Gray
}

# Activate and install packages
Write-Host "   Installing Python packages..." -ForegroundColor Yellow

# Ensure pip is upgraded first
& .\venv\Scripts\python.exe -m pip install --upgrade pip | Out-Null

$packages = @(
    "pymysql",
    "sqlalchemy",
    "pandas",
    "python-dotenv",
    "requests",
    "tqdm"
)

foreach ($package in $packages) {
    Write-Host "   ...installing $package" -NoNewline
    & .\venv\Scripts\python.exe -m pip install -q $package
    Write-Host " Done" -ForegroundColor Green
}
Write-Host "   ✓ Python packages installed" -ForegroundColor Green

# Step 5: Download datasets
Write-Host ""
Write-Host "Step 5: Downloading text2sql datasets..." -ForegroundColor Yellow

if (Test-Path "scripts\download_datasets.py") {
    & .\venv\Scripts\python.exe scripts\download_datasets.py
} else {
    Write-Host "   ⚠ download_datasets.py not found, skipping..." -ForegroundColor Yellow
}

# Step 6: Extract schemas
Write-Host ""
Write-Host "Step 6: Extracting database schemas..." -ForegroundColor Yellow

if (Test-Path "scripts\extract_schemas.py") {
    & .\venv\Scripts\python.exe scripts\extract_schemas.py
} else {
    Write-Host "   ⚠ extract_schemas.py not found, skipping..." -ForegroundColor Yellow
}

# Step 7: Start Docker containers
Write-Host ""
Write-Host "Step 7: Starting Docker containers..." -ForegroundColor Yellow

docker-compose up -d

Write-Host "   Waiting for database initialization (30 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Check status
docker-compose ps

# Step 8: Test connections
Write-Host ""
Write-Host "Step 8: Testing database connections..." -ForegroundColor Yellow

if (Test-Path "scripts\test_connections.py") {
    & .\venv\Scripts\python.exe scripts\test_connections.py
} else {
    Write-Host "   ⚠ test_connections.py not found, skipping..." -ForegroundColor Yellow
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Services:" -ForegroundColor White
Write-Host "   MySQL:      localhost:3306" -ForegroundColor Gray
Write-Host "   MariaDB:    localhost:3307" -ForegroundColor Gray
Write-Host "   phpMyAdmin: http://localhost:8080" -ForegroundColor Gray
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "   1. Open phpMyAdmin to verify databases" -ForegroundColor Gray
Write-Host "   2. Run: python scripts\test_connections.py" -ForegroundColor Gray
Write-Host "   3. Start working on LLM integration" -ForegroundColor Gray
Write-Host ""