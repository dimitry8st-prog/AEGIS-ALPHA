param(
    [Parameter(Mandatory = $true)][string] $Repository,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')][string] $Revision
)

$ErrorActionPreference = 'Stop'
if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'This installer requires the current Windows user session.'
}

$repo = (Resolve-Path -LiteralPath $Repository).Path
$runtime = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'AEGIS/finam-daily'
$startup = [Environment]::GetFolderPath('Startup')
$credential = Join-Path $runtime 'finam-key.dpapi'
$log = Join-Path $runtime 'latest.log'
$runner = Join-Path $runtime ("background-" + $Revision.Substring(0, 12) + ".ps1")
$powerShell = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell/v1.0/powershell.exe'
if (-not (Test-Path -LiteralPath $credential -PathType Leaf)) {
    throw 'The existing encrypted Finam key was not found in the current Windows profile.'
}
if (-not $startup -or -not (Test-Path -LiteralPath $powerShell -PathType Leaf)) {
    throw 'Windows Startup or the system PowerShell could not be found.'
}
$git = (Get-Command git -CommandType Application | Select-Object -First 1).Source
$docker = (Get-Command docker -CommandType Application | Select-Object -First 1).Source
$source = & $git -C $repo show "${Revision}:scripts/run_finam_daily_background.ps1"
if ($LASTEXITCODE -ne 0) { throw 'Could not load the pinned background collector.' }
$runnerText = $source -join "`n"
$null = [scriptblock]::Create($runnerText)
[IO.File]::WriteAllText($runner, $runnerText)

$bootstrap = @'
try {
    & ([scriptblock]::Create([IO.File]::ReadAllText('__RUNNER__'))) -Repository '__REPO__' -Revision '__REV__' -Runtime '__RUNTIME__' -Git '__GIT__' -Docker '__DOCKER__'
} catch {
    [IO.File]::WriteAllText('__LOG__', "ERROR: " + $_.Exception.Message + [Environment]::NewLine + 'Collection state: FAILED')
    exit 1
}
'@
$values = @{
    '__RUNNER__' = $runner; '__REPO__' = $repo; '__REV__' = $Revision; '__RUNTIME__' = $runtime
    '__GIT__' = $git; '__DOCKER__' = $docker; '__LOG__' = $log
}
foreach ($placeholder in $values.Keys) {
    $bootstrap = $bootstrap.Replace($placeholder, $values[$placeholder].Replace("'", "''"))
}
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($bootstrap))
$arguments = "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded"
$null = New-Item -ItemType Directory -Path $startup -Force
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut((Join-Path $startup 'AEGIS Finam daily.lnk'))
$shortcut.TargetPath = $powerShell
$shortcut.Arguments = $arguments
$shortcut.WorkingDirectory = $runtime
$shortcut.Description = 'Collect Finam candles for AEGIS at 08:30 Moscow time.'
$shortcut.Save()

$started = [DateTime]::UtcNow
$process = Start-Process -FilePath $powerShell -ArgumentList $arguments -WorkingDirectory $runtime -WindowStyle Hidden -PassThru
$taskName = 'AEGIS - Finam daily collection'
$oldTask = Get-ScheduledTask -TaskName $taskName -TaskPath '\' -ErrorAction SilentlyContinue
if ($oldTask -and $oldTask.Description -eq 'AEGIS Alpha Finam daily collector (managed by install_finam_daily.ps1).') {
    try {
        Disable-ScheduledTask -InputObject $oldTask -ErrorAction Stop | Out-Null
    } catch {
        Write-Warning 'Windows did not allow disabling the previous AEGIS task.'
    }
}
Write-Host 'AEGIS background collection: user Startup, daily at 08:30 MSK.'
Write-Host "Log: $log"
for ($attempt = 0; $attempt -lt 45; $attempt++) {
    if ((Test-Path -LiteralPath $log -PathType Leaf) -and (Get-Item -LiteralPath $log).LastWriteTimeUtc -ge $started) {
        $result = Get-Content -LiteralPath $log -Tail 10
        if ($result -match '^Collection state: (READY|FAILED)$') {
            $result | Write-Host
            return
        }
    }
    $process.Refresh()
    if ($process.HasExited) {
        if ($process.ExitCode -eq 0) { Write-Host 'The background collector is already active.' }
        else { throw "The PowerShell background process exited with code $($process.ExitCode)." }
        return
    }
    Start-Sleep -Seconds 1
}
Write-Host 'The background process is running. The log will show the collection result.'
