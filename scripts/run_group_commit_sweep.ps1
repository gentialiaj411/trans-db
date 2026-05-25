# Group-commit parameter sweep (Task A) — isolated data dir per run.
$ErrorActionPreference = "Stop"
$RepoRoot = "c:\Users\bhask\Documents\PROJECTS\trans-db"
$Bench = Join-Path $RepoRoot "build3\Release\bench_tpcc.exe"

$Configs = @(
    @{ Id = "gc_off_clean"; GC = "0"; Coord = $null; Replica = $null; Batch = $null },
    @{ Id = "gc_w0_r0"; GC = "1"; Coord = "0"; Replica = "0"; Batch = "64" },
    @{ Id = "gc_w0_r50"; GC = "1"; Coord = "0"; Replica = "50"; Batch = "64" },
    @{ Id = "gc_w0_r100"; GC = "1"; Coord = "0"; Replica = "100"; Batch = "64" },
    @{ Id = "gc_w0_r250"; GC = "1"; Coord = "0"; Replica = "250"; Batch = "64" },
    @{ Id = "gc_w50_r100"; GC = "1"; Coord = "50"; Replica = "100"; Batch = "128" },
    @{ Id = "gc_w100_r250"; GC = "1"; Coord = "100"; Replica = "250"; Batch = "128" }
)

function Stop-StrayReplicas {
    Get-Process trans_db_replica,bench_tpcc -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3
}

function Parse-Metrics {
    param([string]$Stdout)
    $tps = $null
    $p99 = $null
    if ($Stdout -match "Throughput:\s+([\d.eE+-]+)\s+txn/sec") { $tps = [double]$Matches[1] }
    if ($Stdout -match "p99:\s+([\d.eE+-]+)\s+us") { $p99 = [double]$Matches[1] }
    return @{ Throughput = $tps; P99 = $p99 }
}

function Run-BenchOnce {
    param([hashtable]$Cfg, [int]$RepNum)
    Stop-StrayReplicas
    Set-Location $RepoRoot
    $dataDir = Join-Path $RepoRoot ("bench_data_sweep_{0}_r{1}" -f $Cfg.Id, $RepNum)
    if (Test-Path $dataDir) { Remove-Item -Recurse -Force $dataDir }

    $env:TRANS_DB_GROUP_COMMIT = $Cfg.GC
    if ($Cfg.Coord -ne $null) { $env:TRANS_DB_GROUP_COMMIT_WINDOW_US = $Cfg.Coord } else { Remove-Item Env:TRANS_DB_GROUP_COMMIT_WINDOW_US -ErrorAction SilentlyContinue }
    if ($Cfg.Replica -ne $null) { $env:TRANS_DB_REPLICA_GROUP_COMMIT_WINDOW_US = $Cfg.Replica } else { Remove-Item Env:TRANS_DB_REPLICA_GROUP_COMMIT_WINDOW_US -ErrorAction SilentlyContinue }
    if ($Cfg.Batch -ne $null) { $env:TRANS_DB_GROUP_COMMIT_BATCH = $Cfg.Batch } else { Remove-Item Env:TRANS_DB_GROUP_COMMIT_BATCH -ErrorAction SilentlyContinue }

    $args = @("--shards", "3", "--threads", "8", "--duration", "30", "--warmup", "5", "--transport", "grpc", "--data-dir", $dataDir)
    $out = & $Bench @args 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        Stop-StrayReplicas
        throw "bench_tpcc exit $($LASTEXITCODE) for $($Cfg.Id) rep $RepNum"
    }
    $m = Parse-Metrics -Stdout $out
    if ($null -eq $m.Throughput) {
        Stop-StrayReplicas
        throw "parse failed for $($Cfg.Id) rep $RepNum"
    }
    Stop-StrayReplicas
    return $m
}

$Results = @()
foreach ($cfg in $Configs) {
    Write-Host "=== $($cfg.Id) rep1 ===" -ForegroundColor Cyan
    $r1 = Run-BenchOnce -Cfg $cfg -RepNum 1
    Write-Host "  tps=$($r1.Throughput) p99=$($r1.P99)"
    Write-Host "=== $($cfg.Id) rep2 ===" -ForegroundColor Cyan
    $r2 = Run-BenchOnce -Cfg $cfg -RepNum 2
    Write-Host "  tps=$($r2.Throughput) p99=$($r2.P99)"
    $Results += [PSCustomObject]@{
        ConfigId = $cfg.Id
        GC = $cfg.GC
        Coord = $cfg.Coord
        Replica = $cfg.Replica
        Batch = $cfg.Batch
        Rep1Tps = $r1.Throughput
        Rep2Tps = $r2.Throughput
        MedianTps = ($r1.Throughput + $r2.Throughput) / 2.0
        MedianP99 = if ($null -ne $r1.P99 -and $null -ne $r2.P99) { ($r1.P99 + $r2.P99) / 2.0 } else { $null }
    }
}

$outJson = Join-Path $RepoRoot "bench\results\group_commit_sweep_raw.json"
$Results | ConvertTo-Json -Depth 4 | Set-Content $outJson -Encoding UTF8
$Results | Format-Table -AutoSize
Write-Host "Wrote $outJson"
