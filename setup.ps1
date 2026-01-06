# setup.ps1 - Complete Windows setup script
# Run as: .\setup.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($msg)  { Write-Host "[Info]   $msg" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "[OK]   $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "[Warn]   $msg" -ForegroundColor Yellow }
function Fail($msg)  { Write-Host "[Fail]   $msg" -ForegroundColor Red }

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Text2SQL Project Setup (Windows)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# ----------------------------
# Step 1: Check prerequisites
# ----------------------------
Write-Host "Step 1: Checking prerequisites..." -ForegroundColor Yellow

# Check Docker
try {
    $dockerVersion = docker --version
    Ok "Docker found: $dockerVersion"
} catch {
    Fail "Docker not found! Please install Docker Desktop."
    exit 1
}

# Check Docker Compose, fallback to docker-compose if needed
$ComposeCmd = $null
try {
    docker compose version | Out-Null
    $ComposeCmd = @("docker", "compose")
    Ok "Docker Compose plugin available: docker compose"
} catch {
    try {
        docker-compose --version | Out-Null
        $ComposeCmd = @("docker-compose")
        Ok "Legacy docker-compose available"
    } catch {
        Fail "Neither 'docker compose' nor 'docker-compose' found."
        exit 1
    }
}

# Check Python
try {
    $pythonVersion = python3 --version
    Ok "Python: $pythonVersion" -ForegroundColor Green
} catch {
    Fail "Python not found! Please install Python 3.8+."
    exit 1
}

# ----------------------------
# Step 2: Create directory structure
# ----------------------------
Write-Host ""
Write-Host "Step 2: Creating directory structure..." -ForegroundColor Yellow

# Create runtime directories
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
        Ok "Created: $dir"
    } else{
        Info "Exists: $dir"
    }
}

# ----------------------------
# Step 3: Create/validate .env
# ----------------------------
Write-Host ""
Write-Host "Step 3: Checking .env file..." -ForegroundColor Yellow

if (!(Test-Path ".env")) {
    $envContent = @"
# Database credentials

# MySQL
MYSQL_ROOT_PASSWORD=root123
MYSQL_DATABASE=text2sql_db
MYSQL_USER=text2sql_user
MYSQL_PASSWORD=text2sql_pass

# MariaDB
MARIADB_ROOT_PASSWORD=root123
MARIADB_DATABASE=text2sql_db
MARIADB_USER=text2sql_user
MARIADB_PASSWORD=text2sql_pass

# Dataset paths
DATASETS_SOURCE_PATH=./datasets_source/data
PROCESSED_DATA_PATH=./data/processed
"@
    $envContent | Out-File -FilePath ".env" -Encoding UTF8
    Ok "Created .env file"
} else {
    Info ".env file exists"
}

# ----------------------------
# Step 4: Python venv + deps
# ----------------------------
Write-Host ""
Write-Host "Step 4: Setting up Python virtual environment..." -ForegroundColor Yellow

$venvPath = "venv"
$venvPython = Join-Path $venvPath "Scripts\python.exe"
$venvPip    = Join-Path $venvPath "Scripts\pip.exe"

if (!(Test-Path $venvPython)) {
    & python3 -m venv $venvPath
    Ok "Virtual environment created at .\$venvPath"
} else {
    Info "Virtual environment exists at .\$venvPath"
}

# Upgrade pip
Info "Upgrading pip..."
& $venvPython -m pip install --upgrade pip | Out-Null
Ok "pip upgraded"

# Install dependencies
Write-Host ""
Write-Host "Step 4b: Installing Python dependencies..." -ForegroundColor Yellow

$packages = @(
    "pymysql",
    "sqlalchemy",
    "pandas",
    "python-dotenv",
    "requests",
    "tqdm"
)

if (Test-Path "requirements.txt") {
    Info "Installing from requirements.txt (recommended for reproducibility)..."
    & $venvPython -m pip install -r requirements.txt
    Ok "Installed dependencies from requirements.txt"
} else {
    Warn "requirements.txt not found. Installing minimal deps..."
    & $venvPython -m pip install @packages
    Ok "Installed minimal dependencies"
}

# ----------------------------
# Step 5: Download datasets
# ----------------------------
Write-Host ""
Write-Host "Step 5: Downloading text2sql datasets..." -ForegroundColor Yellow

if (Test-Path "scripts\download_datasets.py") {
    Info "Running download_datasets.py..."
    & $venvPython scripts\download_datasets_copy.py
    Ok "Datasets download step complete"
} else {
    Warn "scripts\download_datasets.py not found, skipping..."
}


# ----------------------------
# Step 7.a: Start Docker containers
# ----------------------------
Write-Host ""
Write-Host "Step 7: Starting Docker containers..." -ForegroundColor Yellow

# Start Docker services
try {
    if ($ComposeCmd.Length -eq 2) {
        & $ComposeCmd[0] $ComposeCmd[1] up -d
    } else {
        & $ComposeCmd[0] up -d
    }
    Ok "Docker services started"
} catch {
    Fail "Failed to start Docker services."
    throw
}

# ----------------------------
# Step 7.b: Wait for healthchecks
# ----------------------------
Write-Host ""
Write-Host "Step 7: Waiting for databases to become healthy..." -ForegroundColor Yellow


function Get-ContainerHealth($name) {
    try {
        $status = docker inspect -f '{{.State.Health.Status}}' $name 2>$null
        return $status.Trim()
    } catch {
        return ""
    }
}

$mysqlName   = "text2sql-mysql"
$mariadbName = "text2sql-mariadb"

$timeoutSec = 180
$pollSec = 5
$elapsed = 0

while ($true) {
    $mysqlHealth = Get-ContainerHealth $mysqlName
    $mariaHealth = Get-ContainerHealth $mariadbName

    $mysqlShown = if ([string]::IsNullOrWhiteSpace($mysqlHealth)) { "<unknown>" } else { $mysqlHealth }
    $mariaShown = if ([string]::IsNullOrWhiteSpace($mariaHealth)) { "<unknown>" } else { $mariaHealth }

    Info ("MySQL health:   $mysqlShown")
    Info ("MariaDB health: $mariaShown")


    if ($mysqlHealth -eq "healthy" -and $mariaHealth -eq "healthy") {
        Ok "Both databases are healthy"
        break
    }

    if ($elapsed -ge $timeoutSec) {
        Fail "Timed out waiting for database healthchecks ($timeoutSec seconds)."
        Write-Host ""
        Write-Host "---- docker compose ps ----" -ForegroundColor Yellow
        if ($ComposeCmd.Length -eq 2) { & $ComposeCmd[0] $ComposeCmd[1] ps } else { & $ComposeCmd[0] ps }

        Write-Host ""
        Write-Host "---- mysql logs (tail) ----" -ForegroundColor Yellow
        docker logs --tail 120 $mysqlName

        Write-Host ""
        Write-Host "---- mariadb logs (tail) ----" -ForegroundColor Yellow
        docker logs --tail 120 $mariadbName

        exit 1
    }

    Start-Sleep -Seconds $pollSec
    $elapsed += $pollSec
}

# ----------------------------
# Step 6: Extract schemas
# ----------------------------
Write-Host ""
Write-Host "Step 6: Extracting database schemas..." -ForegroundColor Yellow

if (Test-Path "scripts\extract_schemas.py") {
    Info "Running extract_schemas.py..."
    & $venvPython scripts\extract_schemas_copy.py
    Ok "Database schema extraction complete"
} else {
    Warn "scripts\extract_schemas.py not found, skipping..."
}


# ----------------------------
# Step 8: Test connections
# ----------------------------
Write-Host ""
Write-Host "Step 8: Testing database connections..." -ForegroundColor Yellow

if (Test-Path "scripts\test_connections.py") {
    & $venvPython scripts\test_connections.py
} else {
    Warn "\scripts\test_connections.py not found, skipping..." -ForegroundColor Yellow
}

# ----------------------------
# Summary
# ----------------------------
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