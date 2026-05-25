# Run full Jepsen Docker suite (five fault scenarios). Requires Docker Desktop + WSL2.
$ErrorActionPreference = "Stop"
$Repo = "c:\Users\bhask\Documents\PROJECTS\trans-db"
$Docker = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"

if (-not (Test-Path $Docker)) {
    Write-Error "Docker CLI not found. Install Docker Desktop first."
}

& $Docker ps *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker engine not ready. Start Docker Desktop, wait until it shows Running, then re-run:"
    Write-Host "  powershell -File scripts\run_jepsen_docker.ps1"
    exit 1
}

# Build on WSL ext4 (faster than /mnt/c) with slim context (.dockerignore).
$cmd = @'
set -euo pipefail
SRC=/mnt/c/Users/bhask/Documents/PROJECTS/trans-db
DST=~/trans-db-docker
mkdir -p "$DST"
rsync -a --delete \
  --exclude build3 --exclude build_wsl --exclude '*/vcpkg_installed' \
  --exclude bench_data* --exclude .git \
  "$SRC/" "$DST/"
cd "$DST"
python3 tests/jepsen/history_checker.py --self-test
export DOCKER_BUILDKIT=1
docker compose -f deploy/compose/docker-compose.yml -f tests/jepsen/compose.jepsen.yml build
docker compose -f deploy/compose/docker-compose.yml -f tests/jepsen/compose.jepsen.yml up -d
sleep 12
python3 tests/jepsen/run_scenarios.py
rsync -a "$DST/tests/jepsen/results/" "$SRC/tests/jepsen/results/"
echo DONE
'@

wsl -e bash -lc $cmd
exit $LASTEXITCODE
