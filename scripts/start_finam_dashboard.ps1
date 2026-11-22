param(
    [Parameter(Mandatory = $true)]
    [string] $Repository,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string] $Revision
)

$ErrorActionPreference = 'Stop'

function Invoke-Checked([string] $Program, [string[]] $Arguments) {
    $ErrorActionPreference = 'Continue'
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Program exited with code $LASTEXITCODE."
    }
}

function Get-ComposePort([string] $Service, [string] $InternalPort) {
    $binding = Invoke-Checked docker ($compose + @('port', $Service, $InternalPort))
    if (($binding | Select-Object -First 1) -notmatch ':(\d+)$') {
        throw "Could not determine the host port for $Service."
    }
    return [int] $Matches[1]
}

function Test-Dashboard([string] $BaseUrl) {
    try {
        $health = Invoke-RestMethod -Uri "$BaseUrl/health" -TimeoutSec 2
        return ($health.instance_id -eq $instance)
    } catch {
        return $false
    }
}

$repo = (Resolve-Path -LiteralPath $Repository).Path
$composeFile = Join-Path $repo 'docker-compose.yml'
$python = Join-Path $repo '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) {
    throw 'docker-compose.yml was not found in the local AEGIS repository.'
}
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'The Windows Python executable .venv/Scripts/python.exe was not found.'
}

$shortRevision = $Revision.Substring(0, 12)
$checkout = Join-Path (Split-Path $repo -Parent) "AEGIS-dashboard-$shortRevision"
$instance = "aegis-dashboard-$shortRevision"

Get-Command -Name git, docker -ErrorAction Stop | Out-Null
Invoke-Checked docker @('info', '--format', '{{.ServerVersion}}')
if (-not (Test-Path -LiteralPath $checkout)) {
    Invoke-Checked git @('-C', $repo, 'worktree', 'add', '--detach', $checkout, $Revision)
}
$checkoutRevision = Invoke-Checked git @('-C', $checkout, 'rev-parse', 'HEAD')
$checkoutChanges = Invoke-Checked git @('-C', $checkout, 'status', '--porcelain')
if ($checkoutRevision -ne $Revision -or $checkoutChanges) {
    throw 'The dashboard worktree has a different revision or local changes.'
}

$compose = @('compose', '--project-directory', $repo, '-f', $composeFile)
$envFile = Join-Path $repo '.env'
if (Test-Path -LiteralPath $envFile -PathType Leaf) {
    $compose += @('--env-file', $envFile)
}
Invoke-Checked docker ($compose + @('up', '-d', '--no-recreate', '--wait', '--wait-timeout', '90', 'postgres', 'redis'))
$composeConfig = ((Invoke-Checked docker ($compose + @('config', '--format', 'json'))) -join "`n") | ConvertFrom-Json
$postgres = $composeConfig.services.postgres.environment
$app = $composeConfig.services.app.environment
$dbPort = Get-ComposePort postgres 5432
$redisPort = Get-ComposePort redis 6379
$dbUser = [uri]::EscapeDataString($postgres.POSTGRES_USER)
$dbPassword = [uri]::EscapeDataString($postgres.POSTGRES_PASSWORD)
$dbName = [uri]::EscapeDataString($postgres.POSTGRES_DB)

$runtime = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "AEGIS/dashboard/$shortRevision"
$null = New-Item -ItemType Directory -Path $runtime -Force
$compat = Join-Path $runtime 'bcrypt'
$state = Join-Path $runtime 'port'
$stdoutLog = Join-Path $runtime 'api.out.log'
$stderrLog = Join-Path $runtime 'api.err.log'

$environment = @{
    DATABASE_URL = "postgresql://${dbUser}:${dbPassword}@127.0.0.1:$dbPort/$dbName"
    REDIS_URL = "redis://127.0.0.1:$redisPort/0"
    JWT_SECRET_KEY = $app.JWT_SECRET_KEY
    JWT_ALGORITHM = $app.JWT_ALGORITHM
    JWT_EXPIRATION_HOURS = $app.JWT_EXPIRATION_HOURS
    INSTANCE_ID = $instance
    FINAM_API_TOKEN = $null
    PYTHONDONTWRITEBYTECODE = '1'
    PYTHONIOENCODING = 'utf-8'
    PYTHONPATH = $compat
}
$previous = @{}
$secure = $null
$dashboardUrl = $null
Push-Location -LiteralPath $checkout
try {
    foreach ($name in $environment.Keys) {
        $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
        [Environment]::SetEnvironmentVariable($name, $environment[$name], 'Process')
    }
    $bcryptVersion = Invoke-Checked $python @(
        '-c', "from importlib.metadata import version, packages_distributions; print(version('bcrypt') if 'bcrypt' in packages_distributions() else 'missing')"
    )
    if ($bcryptVersion -notmatch '^4[.]') {
        Invoke-Checked $python @(
            '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '--only-binary=:all:',
            '--no-deps', '--upgrade', '--target', $compat, 'bcrypt==4.3.0'
        )
    }
    Invoke-Checked $python @('-m', 'alembic', 'upgrade', 'head')

    $baseUrl = $null
    if (Test-Path -LiteralPath $state -PathType Leaf) {
        $storedPort = (Get-Content -LiteralPath $state -Raw).Trim()
        if ($storedPort -match '^\d{1,5}$' -and [int] $storedPort -le 65535) {
            $candidate = "http://127.0.0.1:$storedPort"
            if (Test-Dashboard $candidate) { $baseUrl = $candidate }
        }
    }
    if (-not $baseUrl) {
        $server = Start-Process -FilePath $python -ArgumentList @(
            '-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1', '--port', '0'
        ) -WorkingDirectory $checkout -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -WindowStyle Hidden -PassThru
        for ($attempt = 0; $attempt -lt 30; $attempt++) {
            $server.Refresh()
            if ($server.HasExited) { break }
            if ((Get-Content -LiteralPath $stderrLog -Raw) -match 'Uvicorn running on http://127[.]0[.]0[.]1:(\d+)') {
                $port = $Matches[1]
                $candidate = "http://127.0.0.1:$port"
                if (Test-Dashboard $candidate) {
                    $baseUrl = $candidate
                    [IO.File]::WriteAllText($state, $port)
                    break
                }
            }
            Start-Sleep -Seconds 1
        }
    }
    if (-not $baseUrl) { throw "The dashboard API did not start. See: $stderrLog" }

    $null = Invoke-WebRequest -Uri "$baseUrl/ready" -UseBasicParsing -TimeoutSec 5
    $page = Invoke-WebRequest -Uri "$baseUrl/dashboard" -UseBasicParsing -TimeoutSec 5
    if ($page.Content -notmatch 'AEGIS Alpha') {
        throw 'The dashboard route returned an unexpected page.'
    }
    $dashboardUrl = "$baseUrl/dashboard"
    Write-Host "Dashboard: $dashboardUrl"
    $endDay = [DateTime]::UtcNow.AddHours(3).Date
    $start = $endDay.AddDays(-30).ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    $end = $endDay.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    $secure = Read-Host 'Finam API key (hidden input)' -AsSecureString
    if ($secure.Length -eq 0) { throw 'No Finam API key was entered.' }
    [Environment]::SetEnvironmentVariable(
        'FINAM_API_TOKEN', ([pscredential]::new('finam', $secure)).GetNetworkCredential().Password, 'Process'
    )
    $reportJson = & $python -m worker collect --provider finam --symbols 'GAZP,GMKN' --adjustment raw --start $start --end $end
    $collectExit = $LASTEXITCODE
    [Environment]::SetEnvironmentVariable('FINAM_API_TOKEN', $null, 'Process')
    $report = ($reportJson -join "`n") | ConvertFrom-Json
    if ($collectExit -ne 0 -or $report.status -ne 'READY') {
        Write-Host ($reportJson -join "`n")
        throw "Finam collection returned $($report.status). The dashboard API is still running."
    }
    Write-Host "Finam: READY. Inserted $($report.counters.inserted), duplicates $($report.counters.duplicates)."
} finally {
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
    if ($null -ne $secure) { $secure.Dispose() }
    Pop-Location
    if ($dashboardUrl) { Start-Process $dashboardUrl }
}
