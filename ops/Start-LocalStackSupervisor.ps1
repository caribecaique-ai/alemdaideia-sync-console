param(
  [int]$CheckIntervalSeconds = 10
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$runtimeDir = Join-Path $repoRoot 'backend\runtime-data'
$statePath = Join-Path $runtimeDir 'local-supervisor-state.json'
$runnerPath = Join-Path $scriptDir 'Run-LocalStackSupervisor.ps1'

New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

if (Test-Path $statePath) {
  try {
    $state = Get-Content -Path $statePath -Raw | ConvertFrom-Json
    if ($state.supervisorPid) {
      $process = Get-Process -Id $state.supervisorPid -ErrorAction SilentlyContinue
      if ($process) {
        Write-Output "Supervisor ja esta rodando. PID=$($process.Id)"
        exit 0
      }
    }
  } catch {
  }
}

$process = Start-Process -FilePath 'powershell.exe' `
  -ArgumentList @(
    '-ExecutionPolicy',
    'Bypass',
    '-File',
    $runnerPath,
    '-CheckIntervalSeconds',
    "$CheckIntervalSeconds"
  ) `
  -WorkingDirectory $repoRoot `
  -WindowStyle Hidden `
  -PassThru

Write-Output "Supervisor iniciado. PID=$($process.Id)"
