$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$pidPath = Join-Path $projectRoot '.local/continuation.pid'
if (Test-Path -LiteralPath $pidPath) {
    $previousPid = [int](Get-Content -LiteralPath $pidPath)
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $previousPid" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*server/continue.ts*') { Write-Output 'Continuation supervisor is running.'; exit 0 }
}
$process = Start-Process -FilePath (Get-Command node).Source -ArgumentList @('--import', 'tsx', 'server/continue.ts') -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $projectRoot '.local/continuation.stdout.log') -RedirectStandardError (Join-Path $projectRoot '.local/continuation.stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath $pidPath
Write-Output 'Automatic continuation started without restarting active workers.'
