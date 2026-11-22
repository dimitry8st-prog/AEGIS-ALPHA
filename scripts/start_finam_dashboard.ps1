param(
    [Parameter(Mandatory = $true)]
    [string] $Repository,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string] $Revision,

    [switch] $CollectOnly,

    [string] $CredentialFile
)

$ErrorActionPreference = 'Stop'

function Invoke-Checked([string] $Program, [string[]] $Arguments) {
    $ErrorActionPreference = 'Continue'
    & $Program @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Program exited with code $LASTEXITCODE."
    }
}

function Get-PublishedPort([string[]] $Arguments) {
    $binding = Invoke-Checked docker $Arguments
    if (($binding | Select-Object -First 1) -notmatch ':(\d+)$') {
        throw 'Could not determine the published Docker port.'
    }
    return [int] $Matches[1]
}

function Start-DashboardRelay {
    $network = $composeConfig.networks.'aegis-network'.name
    if (-not $network) { throw 'The AEGIS Docker network was not found.' }
    $relay = "aegis-dashboard-relay-$shortRevision"
    $label = "aegis.dashboard.relay=$Revision"
    $running = Invoke-Checked docker @('ps', '--filter', "network=$network", '--filter', 'label=aegis.dashboard.relay', '--format', '{{.Names}}')
    $existing = Invoke-Checked docker @('ps', '-a', '--filter', "name=^/$relay$", '--filter', "label=$label", '--format', '{{.Names}}')
    if ($running) {
        $relay = $running | Select-Object -First 1
    } elseif ($existing -eq $relay) {
        Invoke-Checked docker @('start', $relay) | Out-Host
    } else {
        $config = Join-Path $checkout 'docker/dashboard/haproxy.cfg'
        Invoke-Checked docker @(
            'run', '-d', '--name', $relay, '--label', $label, '--network', $network,
            '--publish', '127.0.0.1::15432', '--publish', '127.0.0.1::16379',
            '--mount', "type=bind,source=$config,target=/usr/local/etc/haproxy/haproxy.cfg,readonly",
            'haproxy:3.2-alpine'
        ) | Out-Host
    }
    return @{
        Postgres = Get-PublishedPort @('port', $relay, '15432/tcp')
        Redis = Get-PublishedPort @('port', $relay, '16379/tcp')
    }
}

function Test-Dashboard([string] $BaseUrl) {
    try {
        $health = Invoke-RestMethod -Uri "$BaseUrl/health" -TimeoutSec 2
        if ($health.instance_id -ne $instance) { return $false }
        $ready = Invoke-RestMethod -Uri "$BaseUrl/ready" -TimeoutSec 3
        return ($ready.status -eq 'ready')
    } catch {
        return $false
    }
}

function Get-DatabaseUrl([string] $Password) {
    $encoded = [uri]::EscapeDataString($Password)
    return "postgresql+asyncpg://${dbUser}:${encoded}@127.0.0.1:$dbPort/$dbName"
}

function Test-Database([string] $Password) {
    [Environment]::SetEnvironmentVariable('DATABASE_URL', (Get-DatabaseUrl $Password), 'Process')
    return Invoke-Checked $python @('scripts/check_dashboard_dependencies.py')
}

function Invoke-FinamCollection([securestring] $Key, [int] $LookbackDays) {
    $ErrorActionPreference = 'Continue'
    $endDay = [DateTime]::UtcNow.AddHours(3).Date
    $start = $endDay.AddDays(-$LookbackDays).ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    $end = $endDay.ToString('yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    if ($Key.Length -eq 0) { throw 'No Finam API key was supplied.' }
    [Environment]::SetEnvironmentVariable(
        'FINAM_API_TOKEN', ([pscredential]::new('finam', $Key)).GetNetworkCredential().Password, 'Process'
    )
    try {
        $reportJson = & $python -m worker collect --provider finam --symbols 'GAZP,GMKN' --adjustment raw --start $start --end $end
        $collectExit = $LASTEXITCODE
    } finally {
        [Environment]::SetEnvironmentVariable('FINAM_API_TOKEN', $null, 'Process')
    }
    $report = ($reportJson -join "`n") | ConvertFrom-Json -ErrorAction Stop
    if ($collectExit -ne 0 -or $report.status -ne 'READY') {
        Write-Host ($reportJson -join "`n")
        throw "Finam collection returned $($report.status)."
    }
    Write-Host "Finam: READY. Window: [$start, $end). Inserted $($report.counters.inserted), duplicates $($report.counters.duplicates)."
}

if ($CollectOnly -and (-not $CredentialFile -or -not (Test-Path -LiteralPath $CredentialFile -PathType Leaf))) {
    throw 'The scheduled Finam credential file is missing.'
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
$services = @('postgres')
if (-not $CollectOnly) { $services += 'redis' }
Invoke-Checked docker ($compose + @('up', '-d', '--no-recreate', '--wait', '--wait-timeout', '90') + $services)
$composeConfig = ((Invoke-Checked docker ($compose + @('config', '--format', 'json'))) -join "`n") | ConvertFrom-Json
$configuredPostgres = $composeConfig.services.postgres.environment
$app = $composeConfig.services.app.environment
$postgresId = Invoke-Checked docker ($compose + @('ps', '-q', 'postgres'))
if ($postgresId -notmatch '^[0-9a-f]{12,64}$') {
    throw 'A single running PostgreSQL container was not found in the AEGIS Compose project.'
}
$runtimeEnv = ((Invoke-Checked docker @('inspect', '--format', '{{json .Config.Env}}', $postgresId)) -join "`n") | ConvertFrom-Json
$postgres = @{}
foreach ($setting in $runtimeEnv) {
    if ($setting -match '^(POSTGRES_USER|POSTGRES_PASSWORD|POSTGRES_DB)=(.*)$') {
        $postgres[$Matches[1]] = $Matches[2]
    }
}
if (-not $postgres.POSTGRES_USER -or -not $postgres.POSTGRES_DB) {
    throw 'The running PostgreSQL container has no configured user or database.'
}
$dbPort = Get-PublishedPort ($compose + @('port', 'postgres', '5432'))
$redisUrl = $null
if (-not $CollectOnly) {
    $redisPort = Get-PublishedPort ($compose + @('port', 'redis', '6379'))
    $redisUrl = "redis://127.0.0.1:$redisPort/0"
}
$dbUser = [uri]::EscapeDataString($postgres.POSTGRES_USER)
$dbName = [uri]::EscapeDataString($postgres.POSTGRES_DB)

$runtime = Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) "AEGIS/dashboard/$shortRevision"
$null = New-Item -ItemType Directory -Path $runtime -Force
$compat = Join-Path $runtime 'bcrypt'
$state = Join-Path $runtime 'port'
$stdoutLog = Join-Path $runtime 'api.out.log'
$stderrLog = Join-Path $runtime 'api.err.log'

$environment = @{
    DATABASE_URL = $null
    REDIS_URL = $redisUrl
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
    $dbStatus = Test-Database $postgres.POSTGRES_PASSWORD
    $redisStatus = if ($CollectOnly) { 'SKIPPED' } else { Invoke-Checked $python @('scripts/check_dashboard_dependencies.py', 'redis') }
    $connectionFailures = @('08000', '08001', '08003', '08006', '08P01', 'TIMEOUT', 'UNREACHABLE')
    if ($dbStatus -in $connectionFailures -or $redisStatus -eq 'UNREACHABLE') {
        Write-Host "Direct ports: PostgreSQL $dbStatus, Redis $redisStatus. Opening new loopback ports."
        $ports = Start-DashboardRelay
        $dbPort = $ports.Postgres
        if (-not $CollectOnly) {
            $redisPort = $ports.Redis
            [Environment]::SetEnvironmentVariable('REDIS_URL', "redis://127.0.0.1:$redisPort/0", 'Process')
        }
        for ($attempt = 0; $attempt -lt 5; $attempt++) {
            $dbStatus = Test-Database $postgres.POSTGRES_PASSWORD
            if (-not $CollectOnly) {
                $redisStatus = Invoke-Checked $python @('scripts/check_dashboard_dependencies.py', 'redis')
            }
            if ($dbStatus -notin $connectionFailures -and $redisStatus -in @('READY', 'SKIPPED')) { break }
            Start-Sleep -Seconds 1
        }
    }
    if ($dbStatus -eq '28P01' -and $configuredPostgres.POSTGRES_USER -ceq $postgres.POSTGRES_USER -and
        $configuredPostgres.POSTGRES_DB -ceq $postgres.POSTGRES_DB -and
        $configuredPostgres.POSTGRES_PASSWORD -cne $postgres.POSTGRES_PASSWORD) {
        $dbStatus = Test-Database $configuredPostgres.POSTGRES_PASSWORD
    }
    if ($dbStatus -eq '28P01' -and -not $CollectOnly) {
        Write-Host 'PostgreSQL rejected the configured login (28P01).'
        $dbSecure = Read-Host 'Existing AEGIS PostgreSQL password (hidden input)' -AsSecureString
        try {
            if ($dbSecure.Length -eq 0) { throw 'The existing PostgreSQL password is required.' }
            $dbStatus = Test-Database (([pscredential]::new('postgres', $dbSecure)).GetNetworkCredential().Password)
        } finally {
            $dbSecure.Dispose()
        }
    }
    if ($dbStatus -ne 'READY') {
        $detail = switch ($dbStatus) {
            '28P01' { 'PostgreSQL rejected the existing login.' }
            '28000' { 'PostgreSQL does not allow this user or connection.' }
            '3D000' { 'The configured database does not exist in PostgreSQL.' }
            '42501' { 'The configured user does not have access to the database.' }
            '08003' { 'PostgreSQL closed the connection on the local port.' }
            'TIMEOUT' { 'PostgreSQL did not answer on the published Docker port.' }
            'UNREACHABLE' { 'The published Docker port could not be reached.' }
            default { 'PostgreSQL connection check failed.' }
        }
        throw "$detail Status: $dbStatus. Endpoint: 127.0.0.1:$dbPort."
    }
    Write-Host "PostgreSQL: READY on 127.0.0.1:$dbPort"
    if ($CollectOnly) {
        $secure = (Get-Content -LiteralPath $CredentialFile -Raw).Trim() | ConvertTo-SecureString
        Invoke-FinamCollection -Key $secure -LookbackDays 7
        return
    }
    if ($redisStatus -ne 'READY') {
        throw "Redis did not answer on 127.0.0.1:$redisPort."
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
    $secure = Read-Host 'Finam API key (hidden input)' -AsSecureString
    Invoke-FinamCollection -Key $secure -LookbackDays 30
} finally {
    foreach ($name in $previous.Keys) {
        [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process')
    }
    if ($null -ne $secure) { $secure.Dispose() }
    Pop-Location
    if ($dashboardUrl) { Start-Process $dashboardUrl }
}
