$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
Set-Location -LiteralPath $projectRoot
node scripts/setup.mjs
if ($LASTEXITCODE -ne 0) { throw 'Local setup failed.' }
docker compose up -d --wait postgres
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL did not become healthy.' }
if (-not (Test-Path -LiteralPath (Join-Path $projectRoot 'dist/index.html'))) {
    npm run build
    if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
}
try {
    $existing = Invoke-RestMethod 'http://127.0.0.1:4280/api/session' -TimeoutSec 2
    if ($existing.environment -eq 'development' -and $existing.application -eq 'agentos') {
        Write-Output 'AgentOS is already running: http://127.0.0.1:4280'
        & (Join-Path $PSScriptRoot 'start-observer.ps1')
        & (Join-Path $PSScriptRoot 'start-continuation.ps1')
        exit 0
    }
} catch { }
$tsxPath = Join-Path $projectRoot 'node_modules/tsx/dist/cli.mjs'
$entryPath = Join-Path $projectRoot 'server/index.ts'
$process = Start-Process -FilePath (Get-Command node).Source -ArgumentList @(('"' + $tsxPath + '"'), ('"' + $entryPath + '"')) -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $projectRoot '.local/server.stdout.log') -RedirectStandardError (Join-Path $projectRoot '.local/server.stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath (Join-Path $projectRoot '.local/server.pid')
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    Start-Sleep -Milliseconds 500
    if ($process.HasExited) { throw 'AgentOS stopped. Inspect .local/server.stderr.log.' }
    try {
        $status = Invoke-RestMethod 'http://127.0.0.1:4280/api/session' -TimeoutSec 2
        if ($status.application -eq 'agentos') { Write-Output 'AgentOS: http://127.0.0.1:4280'; exit 0 }
    } catch { }
}
throw 'AgentOS did not respond. Inspect the local logs.'
