param(
    [Parameter(Mandatory = $true)][string] $Repository,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')][string] $Revision,
    [Parameter(Mandatory = $true)][string] $Runtime,
    [Parameter(Mandatory = $true)][string] $Git,
    [Parameter(Mandatory = $true)][string] $Docker
)

$ErrorActionPreference = 'Stop'
$log = Join-Path $Runtime 'latest.log'
$successPath = Join-Path $Runtime 'last-success.day'
$credential = Join-Path $Runtime 'finam-key.dpapi'
try {
    $lease = [IO.File]::Open(
        (Join-Path $Runtime 'background.lock'), [IO.FileMode]::OpenOrCreate,
        [IO.FileAccess]::ReadWrite, [IO.FileShare]::None
    )
} catch [IO.IOException] {
    if (($_.Exception.HResult -band 0xffff) -in 32, 33) { return }
    throw
}

try {
    $env:PATH = @(
        (Split-Path -Parent $Git)
        (Split-Path -Parent $Docker)
        $env:PATH
    ) -join [IO.Path]::PathSeparator
    $source = & $Git -C $Repository show "${Revision}:scripts/start_finam_dashboard.ps1"
    if ($LASTEXITCODE -ne 0) { throw 'Could not load the pinned Finam collector.' }
    $collector = [scriptblock]::Create(($source -join "`n"))
    $lastSuccess = ''
    if (Test-Path -LiteralPath $successPath -PathType Leaf) {
        $lastSuccess = ([IO.File]::ReadAllText($successPath)).Trim()
    }
    $attemptDay = ''
    $attempts = 0
    $retryAfter = [DateTimeOffset]::MinValue
    $moscowOffset = [TimeSpan]::FromHours(3)
    $runTime = [TimeSpan]::FromHours(8.5)

    while ($true) {
        $now = [DateTimeOffset]::UtcNow
        $moscow = $now.ToOffset($moscowOffset)
        $due = $moscow.Date
        if ($moscow.TimeOfDay -lt $runTime) { $due = $due.AddDays(-1) }
        $dueDay = $due.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
        if ($attemptDay -ne $dueDay) {
            $attemptDay = $dueDay
            $attempts = 0
            $retryAfter = [DateTimeOffset]::MinValue
        }
        if ([string]::CompareOrdinal($dueDay, $lastSuccess) -gt 0 -and $attempts -lt 4 -and $now -ge $retryAfter) {
            $attempts++
            . {
                try {
                    Write-Output ("Started: " + $moscow.ToString('o'))
                    & $collector -Repository $Repository -Revision $Revision -CollectOnly -CredentialFile $credential
                    [IO.File]::WriteAllText($successPath, $dueDay)
                    $lastSuccess = $dueDay
                    Write-Output 'Collection state: READY'
                } catch {
                    Write-Output ("ERROR: " + $_.Exception.Message)
                    Write-Output 'Collection state: FAILED'
                }
            } *> $log
            $retryAfter = [DateTimeOffset]::UtcNow.AddMinutes(15)
        }
        Start-Sleep -Seconds 30
    }
} finally {
    $lease.Dispose()
}
