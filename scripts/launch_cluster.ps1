# Start 9 replica processes (3 shards x 3 replicas) for local gRPC Raft topology.
param(
  [string]$BuildDir = "build/Release",
  [string]$DataDir = "./cluster_data",
  [int]$ShardBasePort = 57000,
  [int]$RaftBasePort = 58000
)

$ReplicaBin = Join-Path $BuildDir "trans_db_replica.exe"
if (-not (Test-Path $ReplicaBin)) {
  $ReplicaBin = Join-Path $BuildDir "trans_db_replica"
}
if (-not (Test-Path $ReplicaBin)) {
  throw "Build trans_db_replica first: cmake --build build --config Release"
}

New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
$procs = @()

for ($s = 0; $s -lt 3; $s++) {
  for ($r = 0; $r -lt 3; $r++) {
    $peers = @()
    for ($p = 0; $p -lt 3; $p++) {
      if ($p -eq $r) { continue }
      $raftPort = $RaftBasePort + ($s * 10) + $p
      $peers += "$p@127.0.0.1:$raftPort"
    }
    $peerSpec = $peers -join ","
    $shardPort = $ShardBasePort + ($s * 10) + $r
    $raftPortLocal = $RaftBasePort + ($s * 10) + $r
    $repDir = Join-Path $DataDir "shard${s}_rep${r}"

    $args = @(
      "--shard", "$s",
      "--replica", "$r",
      "--data-dir", $repDir,
      "--shard-listen", "0.0.0.0:$shardPort",
      "--raft-listen", "0.0.0.0:$raftPortLocal",
      "--raft-peers", $peerSpec
    )
    $procs += Start-Process -FilePath $ReplicaBin -ArgumentList $args -PassThru
  }
}

Write-Host "Started $($procs.Count) replica processes."
Write-Host "Shard entrypoints: 127.0.0.1:$ShardBasePort, 127.0.0.1:$($ShardBasePort + 10), 127.0.0.1:$($ShardBasePort + 20)"
Write-Host "Stop with: Get-Process trans_db_replica | Stop-Process"
