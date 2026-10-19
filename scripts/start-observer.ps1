$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$observerPid = Join-Path $projectRoot '.local/observer.pid'
if (Test-Path -LiteralPath $observerPid) {
    $previousPid = [int](Get-Content -LiteralPath $observerPid)
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId = $previousPid" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -like '*server/observe.ts*') { Write-Output 'Execution observer is running.'; exit 0 }
}
$process = Start-Process -FilePath (Get-Command node).Source -ArgumentList @('--import', 'tsx', 'server/observe.ts') -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $projectRoot '.local/observer.stdout.log') -RedirectStandardError (Join-Path $projectRoot '.local/observer.stderr.log') -PassThru
$process.Id | Set-Content -LiteralPath $observerPid
Write-Output 'Execution observer started; it does not restart workers.'
