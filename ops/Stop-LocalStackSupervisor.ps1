param(
  [switch]$StopServices
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$runtimeDir = Join-Path $repoRoot 'backend\runtime-data'
$statePath = Join-Path $runtimeDir 'local-supervisor-state.json'

if (Test-Path $statePath) {
  try {
    $state = Get-Content -Path $statePath -Raw | ConvertFrom-Json
    if ($state.supervisorPid) {
      Stop-Process -Id $state.supervisorPid -Force -ErrorAction SilentlyContinue
      Write-Output "Supervisor encerrado. PID=$($state.supervisorPid)"
    }
  } catch {
  }
}

if ($StopServices) {
  foreach ($port in 3015, 4180) {
    try {
      $owner = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop |
        Select-Object -First 1 -ExpandProperty OwningProcess
      if ($owner) {
        Stop-Process -Id $owner -Force -ErrorAction SilentlyContinue
        Write-Output "Processo da porta $port encerrado. PID=$owner"
      }
    } catch {
    }
  }

  Get-CimInstance Win32_Process |
    Where-Object { $_.Name -eq 'ngrok.exe' } |
    ForEach-Object {
      Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
      Write-Output "Ngrok encerrado. PID=$($_.ProcessId)"
    }
}
