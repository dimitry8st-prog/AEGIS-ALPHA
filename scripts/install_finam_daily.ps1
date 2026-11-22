param(
    [Parameter(Mandatory = $true)]
    [string] $Repository,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string] $Revision,

    [switch] $ReplaceKey
)

$ErrorActionPreference = 'Stop'

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'This installer requires Windows Task Scheduler and Windows DPAPI.'
}

$repo = (Resolve-Path -LiteralPath $Repository).Path
$null = & git -C $repo cat-file -e "${Revision}:scripts/start_finam_dashboard.ps1"
if ($LASTEXITCODE -ne 0) { throw 'The pinned dashboard collector was not found in the local repository.' }
if (-not (Test-Path -LiteralPath (Join-Path $repo '.venv/Scripts/python.exe') -PathType Leaf)) {
    throw 'The Windows Python executable .venv/Scripts/python.exe was not found.'
}

$taskName = 'AEGIS - Finam daily collection'
$taskDescription = 'AEGIS Alpha Finam daily collector (managed by install_finam_daily.ps1).'
$oldTask = Get-ScheduledTask -TaskName $taskName -TaskPath '\' -ErrorAction SilentlyContinue
if ($oldTask -and $oldTask.Description -ne $taskDescription) {
    throw "A different Windows task already uses the name $taskName."
}

$runtime = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'AEGIS/finam-daily'
$null = New-Item -ItemType Directory -Path $runtime -Force
$credential = Join-Path $runtime 'finam-key.dpapi'
$log = Join-Path $runtime 'latest.log'
$runner = Join-Path $runtime ("run-" + $Revision.Substring(0, 12) + ".ps1")
$powerShell = Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell/v1.0/powershell.exe'
if (-not (Test-Path -LiteralPath $powerShell -PathType Leaf)) {
    throw 'The Windows PowerShell system executable was not found.'
}
if ($ReplaceKey -or -not (Test-Path -LiteralPath $credential -PathType Leaf)) {
    $key = Read-Host 'Finam API key for daily collection (hidden input)' -AsSecureString
    try {
        if ($key.Length -eq 0) { throw 'No Finam API key was entered.' }
        $encrypted = ConvertFrom-SecureString -SecureString $key
        $temporary = Join-Path $runtime ([IO.Path]::GetRandomFileName())
        try {
            [IO.File]::WriteAllText($temporary, $encrypted)
            if (Test-Path -LiteralPath $credential -PathType Leaf) {
                [IO.File]::Replace($temporary, $credential, [NullString]::Value)
            } else {
                [IO.File]::Move($temporary, $credential)
            }
        } finally {
            Remove-Item -LiteralPath $temporary -ErrorAction SilentlyContinue
        }
    } finally {
        $key.Dispose()
    }
}

$payload = @'
$repo = '__REPO__'
$revision = '__REVISION__'
$credential = '__CREDENTIAL__'
$log = '__LOG__'
$exitCode = 0
. {
    try {
        Write-Output ("Started: " + [DateTimeOffset]::Now.ToString('o'))
        $source = & git -C $repo show "${revision}:scripts/start_finam_dashboard.ps1"
        if ($LASTEXITCODE -ne 0) { throw 'Could not load the pinned Finam collector.' }
        & ([scriptblock]::Create(($source -join "`n"))) -Repository $repo -Revision $revision -CollectOnly -CredentialFile $credential
        Write-Output 'Collection state: READY'
    } catch {
        Write-Output ("ERROR: " + $_.Exception.Message)
        Write-Output 'Collection state: FAILED'
        $exitCode = 1
    }
} *> $log
exit $exitCode
'@
$payload = $payload.Replace('__REPO__', $repo.Replace("'", "''")).Replace('__REVISION__', $Revision)
$payload = $payload.Replace('__CREDENTIAL__', $credential.Replace("'", "''")).Replace('__LOG__', $log.Replace("'", "''"))
[IO.File]::WriteAllText($runner, $payload)
$bootstrap = @'
try {
    & ([scriptblock]::Create([IO.File]::ReadAllText('__RUNNER__')))
} catch {
    [IO.File]::WriteAllText('__LOG__', "ERROR: " + $_.Exception.Message + [Environment]::NewLine + 'Collection state: FAILED')
    exit 1
}
'@
$bootstrap = $bootstrap.Replace('__RUNNER__', $runner.Replace("'", "''")).Replace('__LOG__', $log.Replace("'", "''"))
$encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($bootstrap))
$action = New-ScheduledTaskAction -Execute $powerShell -Argument "-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -EncodedCommand $encoded" -WorkingDirectory $runtime

$now = [DateTimeOffset]::UtcNow.ToOffset([TimeSpan]::FromHours(3))
$first = [DateTimeOffset]::new($now.Year, $now.Month, $now.Day, 8, 30, 0, $now.Offset)
if ($first -le $now) { $first = $first.AddDays(1) }
$trigger = New-ScheduledTaskTrigger -Daily -At $first.LocalDateTime
$trigger.StartBoundary = $first.ToString("yyyy-MM-dd'T'HH:mm:sszzz", [Globalization.CultureInfo]::InvariantCulture)
$userId = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 5)
$null = Register-ScheduledTask -TaskName $taskName -TaskPath '\' -Description $taskDescription `
    -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force

Write-Host "Scheduled task: $taskName (daily at 08:30 MSK)"
Write-Host "Log: $log"
$started = [DateTime]::UtcNow
Start-ScheduledTask -TaskName $taskName -TaskPath '\'
for ($attempt = 0; $attempt -lt 45; $attempt++) {
    if ((Test-Path -LiteralPath $log -PathType Leaf) -and (Get-Item -LiteralPath $log).LastWriteTimeUtc -ge $started) {
        $result = Get-Content -LiteralPath $log -Tail 8
        if ($result -match '^Collection state: (READY|FAILED)$') {
            $result | Write-Host
            return
        }
    }
    $task = Get-ScheduledTask -TaskName $taskName -TaskPath '\'
    $info = Get-ScheduledTaskInfo -InputObject $task
    if ($task.State -eq 'Ready' -and $info.LastRunTime.ToUniversalTime() -ge $started.AddSeconds(-1) -and $info.LastTaskResult -ne 0) {
        Write-Host ('Task Scheduler result: 0x{0:X8}' -f [uint32] $info.LastTaskResult)
        try {
            Get-WinEvent -FilterHashtable @{
                LogName = 'Microsoft-Windows-TaskScheduler/Operational'
                StartTime = $started.ToLocalTime().AddSeconds(-1)
                Level = 1, 2, 3
            } -ErrorAction Stop |
                Where-Object { $_.Message -like "*$taskName*" } |
                Select-Object -First 3 -ExpandProperty Message |
                Write-Host
        } catch {
            Write-Host 'The Task Scheduler Operational log had no accessible error events.'
        }
        return
    }
    Start-Sleep -Seconds 1
}
Write-Host 'Test collection is still queued or running. Check the log for its final status.'
