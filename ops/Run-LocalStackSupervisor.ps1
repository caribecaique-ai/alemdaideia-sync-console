param(
  [switch]$RunOnce,
  [int]$CheckIntervalSeconds = 10,
  [int]$BackendPort = 3015,
  [int]$FrontendPort = 4180,
  [string]$ExpectedPublicUrl = 'https://realizable-jacquelyne-pseudodramatic.ngrok-free.dev'
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$backendDir = Join-Path $repoRoot 'backend'
$frontendDir = Join-Path $repoRoot 'frontend'
$runtimeDir = Join-Path $backendDir 'runtime-data'

New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

$statePath = Join-Path $runtimeDir 'local-supervisor-state.json'
$supervisorLogPath = Join-Path $runtimeDir 'local-supervisor.log'
$backendLogPath = Join-Path $backendDir 'backend-supervised.log'
$backendErrLogPath = Join-Path $backendDir 'backend-supervised.err.log'
$frontendBuildLogPath = Join-Path $frontendDir 'frontend-build.log'
$frontendLogPath = Join-Path $frontendDir 'frontend-supervised.log'
$frontendErrLogPath = Join-Path $frontendDir 'frontend-supervised.err.log'
$ngrokLogPath = Join-Path $runtimeDir 'ngrok-supervised.log'
$ngrokErrLogPath = Join-Path $runtimeDir 'ngrok-supervised.err.log'

$nodeExe = (Get-Command node -ErrorAction Stop).Source
$npmCmd = (Get-Command npm.cmd -ErrorAction SilentlyContinue).Source
if (-not $npmCmd) {
  $npmCmd = (Get-Command npm -ErrorAction Stop).Source
}
$ngrokExe = (Get-Command ngrok -ErrorAction Stop).Source

function Write-Log {
  param(
    [string]$Message,
    [string]$Level = 'INFO'
  )

  $line = '{0} [{1}] {2}' -f (Get-Date).ToString('s'), $Level.ToUpperInvariant(), $Message
  Add-Content -Path $supervisorLogPath -Value $line
  Write-Output $line
}

function Save-State {
  param(
    [hashtable]$Extra = @{}
  )

  $payload = [ordered]@{
    updatedAt = (Get-Date).ToString('o')
    supervisorPid = $PID
    expectedPublicUrl = $ExpectedPublicUrl
    backendPort = $BackendPort
    frontendPort = $FrontendPort
  }

  foreach ($key in $Extra.Keys) {
    $payload[$key] = $Extra[$key]
  }

  $payload | ConvertTo-Json -Depth 8 | Set-Content -Path $statePath
}

function Get-ListeningProcessId {
  param([int]$Port)

  try {
    return Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop |
      Select-Object -First 1 -ExpandProperty OwningProcess
  } catch {
    return $null
  }
}

function Stop-ProcessIfExists {
  param([int]$ProcessId)

  if (-not $ProcessId) {
    return
  }

  try {
    Stop-Process -Id $ProcessId -Force -ErrorAction Stop
    Write-Log "Processo $ProcessId encerrado."
  } catch {
    Write-Log "Falha ao encerrar processo ${ProcessId}: $($_.Exception.Message)" 'WARN'
  }
}

function Invoke-Curl {
  param(
    [string]$Url,
    [int]$TimeoutSeconds = 10
  )

  $response = & curl.exe -sS --max-time $TimeoutSeconds $Url 2>$null
  if ($LASTEXITCODE -ne 0) {
    return $null
  }

  return $response
}

function Test-HttpStatus {
  param(
    [string]$Url,
    [int]$TimeoutSeconds = 10
  )

  $statusCode = & curl.exe -sS --max-time $TimeoutSeconds -o NUL -w '%{http_code}' $Url 2>$null
  if ($LASTEXITCODE -ne 0) {
    return $null
  }

  if ($statusCode -match '^\d+$') {
    return [int]$statusCode
  }

  return $null
}

function Wait-ForUrl {
  param(
    [string]$Url,
    [int]$ExpectedStatus = 200,
    [int]$Attempts = 30,
    [int]$DelayMs = 1000
  )

  for ($attempt = 1; $attempt -le $Attempts; $attempt += 1) {
    $status = Test-HttpStatus -Url $Url -TimeoutSeconds 10
    if ($status -eq $ExpectedStatus) {
      return $true
    }

    Start-Sleep -Milliseconds $DelayMs
  }

  return $false
}

function Build-Frontend {
  Write-Log 'Gerando build do frontend.'
  Push-Location $frontendDir
  try {
    & $npmCmd run build *>> $frontendBuildLogPath
    if ($LASTEXITCODE -ne 0) {
      throw "npm run build falhou com codigo $LASTEXITCODE."
    }
  } finally {
    Pop-Location
  }
}

function Ensure-Backend {
  $healthUrl = "http://localhost:$BackendPort/health"
  $status = Test-HttpStatus -Url $healthUrl -TimeoutSeconds 8
  if ($status -eq 200) {
    return @{
      ok = $true
      restarted = $false
      processId = (Get-ListeningProcessId -Port $BackendPort)
    }
  }

  $existingPid = Get-ListeningProcessId -Port $BackendPort
  if ($existingPid) {
    Write-Log "Backend na porta $BackendPort sem health. Reiniciando processo $existingPid." 'WARN'
    Stop-ProcessIfExists -ProcessId $existingPid
    Start-Sleep -Seconds 1
  }

  Write-Log 'Subindo backend supervisionado.'
  $process = Start-Process -FilePath $nodeExe `
    -ArgumentList 'src/server.js' `
    -WorkingDirectory $backendDir `
    -RedirectStandardOutput $backendLogPath `
    -RedirectStandardError $backendErrLogPath `
    -PassThru

  if (-not (Wait-ForUrl -Url $healthUrl -ExpectedStatus 200 -Attempts 240 -DelayMs 1000)) {
    throw 'Backend nao ficou saudavel a tempo.'
  }

  return @{
    ok = $true
    restarted = $true
    processId = $process.Id
  }
}

function Ensure-Frontend {
  $frontendUrl = "http://localhost:$FrontendPort"
  $status = Test-HttpStatus -Url $frontendUrl -TimeoutSeconds 8
  if ($status -eq 200) {
    return @{
      ok = $true
      restarted = $false
      processId = (Get-ListeningProcessId -Port $FrontendPort)
    }
  }

  $existingPid = Get-ListeningProcessId -Port $FrontendPort
  if ($existingPid) {
    Write-Log "Frontend na porta $FrontendPort sem resposta valida. Reiniciando processo $existingPid." 'WARN'
    Stop-ProcessIfExists -ProcessId $existingPid
    Start-Sleep -Seconds 1
  }

  Build-Frontend

  Write-Log 'Subindo frontend em modo preview supervisionado.'
  $process = Start-Process -FilePath $npmCmd `
    -ArgumentList @('run', 'preview', '--', '--host', '0.0.0.0', '--port', "$FrontendPort") `
    -WorkingDirectory $frontendDir `
    -RedirectStandardOutput $frontendLogPath `
    -RedirectStandardError $frontendErrLogPath `
    -PassThru

  if (-not (Wait-ForUrl -Url $frontendUrl -ExpectedStatus 200 -Attempts 60 -DelayMs 1000)) {
    throw 'Frontend nao ficou saudavel a tempo.'
  }

  return @{
    ok = $true
    restarted = $true
    processId = $process.Id
  }
}

function Get-NgrokTunnel {
  $raw = Invoke-Curl -Url 'http://127.0.0.1:4040/api/tunnels' -TimeoutSeconds 5
  if (-not $raw) {
    return $null
  }

  try {
    $payload = $raw | ConvertFrom-Json
  } catch {
    return $null
  }

  return @($payload.tunnels) | Where-Object { $_.proto -eq 'https' } | Select-Object -First 1
}

function Ensure-Ngrok {
  $tunnel = Get-NgrokTunnel
  $publicHealthOk = $false

  if ($tunnel -and $tunnel.public_url -eq $ExpectedPublicUrl) {
    $publicStatus = Test-HttpStatus -Url "$ExpectedPublicUrl/health" -TimeoutSeconds 10
    $publicHealthOk = $publicStatus -eq 200
  }

  if ($tunnel -and $tunnel.public_url -eq $ExpectedPublicUrl -and $publicHealthOk) {
    return @{
      ok = $true
      restarted = $false
      processId = (
        Get-CimInstance Win32_Process |
        Where-Object { $_.Name -eq 'ngrok.exe' -and $_.CommandLine -match 'http 3015' } |
        Select-Object -First 1 -ExpandProperty ProcessId
      )
      publicUrl = $tunnel.public_url
    }
  }

  $ngrokProcesses = Get-CimInstance Win32_Process |
    Where-Object { $_.Name -eq 'ngrok.exe' }

  foreach ($process in $ngrokProcesses) {
    Stop-ProcessIfExists -ProcessId $process.ProcessId
  }

  Write-Log 'Subindo tunnel supervisionado do ngrok.'
  $process = Start-Process -FilePath $ngrokExe `
    -ArgumentList @('http', "$BackendPort") `
    -WorkingDirectory $backendDir `
    -RedirectStandardOutput $ngrokLogPath `
    -RedirectStandardError $ngrokErrLogPath `
    -PassThru

  $matchedExpectedUrl = $false
  for ($attempt = 1; $attempt -le 45; $attempt += 1) {
    Start-Sleep -Milliseconds 750
    $currentTunnel = Get-NgrokTunnel
    if (-not $currentTunnel) {
      continue
    }

    if ($currentTunnel.public_url -eq $ExpectedPublicUrl) {
      $matchedExpectedUrl = $true
      $publicStatus = Test-HttpStatus -Url "$ExpectedPublicUrl/health" -TimeoutSeconds 10
      if ($publicStatus -eq 200) {
        return @{
          ok = $true
          restarted = $true
          processId = $process.Id
          publicUrl = $currentTunnel.public_url
        }
      }
    }
  }

  if (-not $matchedExpectedUrl) {
    throw "Ngrok nao voltou com a URL esperada: $ExpectedPublicUrl"
  }

  throw 'Ngrok nao deixou a URL publica saudavel a tempo.'
}

while ($true) {
  $cycle = @{
    backend = $null
    frontend = $null
    ngrok = $null
  }

  try {
    $cycle.backend = Ensure-Backend
    $cycle.frontend = Ensure-Frontend
    $cycle.ngrok = Ensure-Ngrok

    Save-State -Extra @{
      backendProcessId = $cycle.backend.processId
      frontendProcessId = $cycle.frontend.processId
      ngrokProcessId = $cycle.ngrok.processId
      publicUrl = $cycle.ngrok.publicUrl
      lastOkAt = (Get-Date).ToString('o')
      backendRestarted = $cycle.backend.restarted
      frontendRestarted = $cycle.frontend.restarted
      ngrokRestarted = $cycle.ngrok.restarted
    }

    if ($cycle.backend.restarted -or $cycle.frontend.restarted -or $cycle.ngrok.restarted) {
      Write-Log "Stack verificada. backend restart=$($cycle.backend.restarted) frontend restart=$($cycle.frontend.restarted) ngrok restart=$($cycle.ngrok.restarted)."
    }
  } catch {
    Save-State -Extra @{
      lastErrorAt = (Get-Date).ToString('o')
      lastError = $_.Exception.Message
    }
    Write-Log $_.Exception.Message 'ERROR'
  }

  if ($RunOnce) {
    break
  }

  Start-Sleep -Seconds $CheckIntervalSeconds
}
