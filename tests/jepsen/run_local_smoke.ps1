# Local P1 smoke (Windows/Linux): checker self-test + one gRPC workload + history check.
$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $Root

python tests/jepsen/history_checker.py --self-test
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$worker = @(
  "build\Release\jepsen_worker.exe",
  "build\jepsen_worker.exe"
) | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $worker) {
  Write-Host "Building jepsen_worker..."
  cmake --build build --config Release --target jepsen_worker
  $worker = "build\Release\jepsen_worker.exe"
}

New-Item -ItemType Directory -Force -Path tests\jepsen\results | Out-Null
$hist = "tests\jepsen\results\local_smoke_history.jsonl"
& $worker --duration 6 --threads 2 --history $hist --data-dir tests\jepsen\results\local_data
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python tests/jepsen/history_checker.py $hist
exit $LASTEXITCODE
