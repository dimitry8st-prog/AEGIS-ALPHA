param(
    [Parameter(Mandatory = $true)]
    [string] $Repository,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string] $Revision
)

$ErrorActionPreference = 'Stop'

function Invoke-Checked([string] $Program, [string[]] $Arguments) {
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Program exited with code $LASTEXITCODE."
    }
}

$repo = (Resolve-Path -LiteralPath $Repository).Path
$composeFile = Join-Path $repo 'docker-compose.yml'
if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw 'docker-compose.yml was not found in the local AEGIS repository.'
}

$shortRevision = $Revision.Substring(0, 12)
$checkout = Join-Path (Split-Path $repo -Parent) "AEGIS-dashboard-$shortRevision"
$container = "aegis-dashboard-$shortRevision"

Invoke-Checked docker @('info', '--format', '{{.ServerVersion}}')
if (-not (Test-Path -LiteralPath $checkout)) {
    Invoke-Checked git @('-C', $repo, 'worktree', 'add', '--detach', $checkout, $Revision)
}
$checkoutRevision = Invoke-Checked git @('-C', $checkout, 'rev-parse', 'HEAD')
$checkoutChanges = Invoke-Checked git @('-C', $checkout, 'status', '--porcelain')
if ($checkoutRevision -ne $Revision -or $checkoutChanges) {
    throw 'The dashboard worktree already exists with a different revision or local changes.'
}

$compose = @('compose', '--project-directory', $repo, '-f', $composeFile)
$envFile = Join-Path $repo '.env'
if (Test-Path -LiteralPath $envFile -PathType Leaf) {
    $compose += @('--env-file', $envFile)
}
Invoke-Checked docker ($compose + @('up', '-d', '--no-recreate', '--wait', '--wait-timeout', '90', 'postgres', 'redis'))

$existing = Invoke-Checked docker @(
    'ps', '-a', '--filter', "name=^/$container$", '--filter', "label=aegis.dashboard.revision=$Revision",
    '--format', '{{.Names}}'
)
if ($existing -eq $container) {
    Invoke-Checked docker @('start', $container)
} else {
    $overridePath = [IO.Path]::GetTempFileName()
    $override = @{
        services = @{
            app = @{
                image = "aegis-dashboard-local:$shortRevision"
                build = @{ context = $checkout }
                environment = @{ FINAM_API_TOKEN = ''; PYTHONDONTWRITEBYTECODE = '1' }
                volumes = @(@{ type = 'bind'; source = $checkout; target = '/app'; read_only = $true })
                command = @('sh', '-c', 'alembic upgrade head && exec uvicorn app.main:app --host 0.0.0.0 --port 8000')
            }
        }
    }
    try {
        [IO.File]::WriteAllText($overridePath, ($override | ConvertTo-Json -Depth 10))
        Invoke-Checked docker ($compose + @(
            '-f', $overridePath, 'run', '-d', '--no-deps', '--build',
            '--name', $container, '--label', "aegis.dashboard.revision=$Revision",
            '--publish', '127.0.0.1::8000', 'app'
        ))
    } finally {
        Remove-Item -LiteralPath $overridePath -ErrorAction SilentlyContinue
    }
}

$binding = Invoke-Checked docker @('port', $container, '8000/tcp')
if ($binding -notmatch '^127[.]0[.]0[.]1:([0-9]+)$') {
    throw 'Could not determine the dashboard loopback port.'
}
$baseUrl = "http://127.0.0.1:$($Matches[1])"
$ready = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    try {
        $response = Invoke-WebRequest -Uri "$baseUrl/ready" -UseBasicParsing -TimeoutSec 2
        $ready = $response.StatusCode -eq 200
        if ($ready) { break }
    } catch {
        $running = Invoke-Checked docker @('inspect', '--format', '{{.State.Running}}', $container)
        if ($running -ne 'true') { break }
    }
    Start-Sleep -Seconds 1
}
if (-not $ready) {
    Invoke-Checked docker @('logs', '--tail', '35', $container)
    throw 'The dashboard API did not become ready.'
}
$page = Invoke-WebRequest -Uri "$baseUrl/dashboard" -UseBasicParsing -TimeoutSec 5
if ($page.Content -notmatch 'AEGIS Alpha') {
    throw 'The dashboard route returned an unexpected page.'
}

Write-Host "Dashboard: $baseUrl/dashboard"
$endDay = [DateTime]::UtcNow.AddHours(3).Date
$start = $endDay.AddDays(-30).ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
$end = $endDay.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
$secure = $null
try {
    $secure = Read-Host 'Finam API key (hidden input)' -AsSecureString
    if ($secure.Length -eq 0) { throw 'No Finam API key was entered.' }
    $secret = ([pscredential]::new('finam', $secure)).GetNetworkCredential().Password
    $reader = "import os,sys; os.environ['FINAM_API_TOKEN'] = sys.stdin.readline().strip(); from worker.__main__ import main; raise SystemExit(main())"
    $reportJson = $secret | & docker exec -i $container python -c $reader collect --provider finam --symbols 'GAZP,GMKN' --adjustment raw --start $start --end $end
    $collectExit = $LASTEXITCODE
    $report = ($reportJson -join "`n") | ConvertFrom-Json
    if ($collectExit -ne 0 -or $report.status -ne 'READY') {
        Write-Host ($reportJson -join "`n")
        throw "Finam collection returned $($report.status). The dashboard API is still running."
    }
    Write-Host "Finam: READY. Inserted $($report.counters.inserted), duplicates $($report.counters.duplicates)."
} finally {
    Remove-Variable secret -ErrorAction SilentlyContinue
    if ($null -ne $secure) { $secure.Dispose() }
    Start-Process "$baseUrl/dashboard"
}
