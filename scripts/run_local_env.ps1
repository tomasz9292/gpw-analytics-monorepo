$ErrorActionPreference = "Stop"

function Ensure-EnvFile {
    param (
        [string]$EnvFile,
        [string]$ExampleFile
    )

    if (-not (Test-Path $EnvFile) -and (Test-Path $ExampleFile)) {
        Write-Host "[setup] Creating .env.local from example..."
        Copy-Item $ExampleFile $EnvFile
    }
}

function Resolve-ComposeCommand {
    if (Get-Command "docker" -ErrorAction SilentlyContinue) {
        try {
            docker compose version | Out-Null
            return @("docker", "compose")
        } catch {
            if (Get-Command "docker-compose" -ErrorAction SilentlyContinue) {
                return @("docker-compose")
            }
        }
    }
    throw "Docker (with compose plugin) or docker-compose is required."
}

$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$EnvFile = Join-Path $RepoRoot ".env.local"
$EnvExample = Join-Path $RepoRoot ".env.local.example"
$ComposeFile = Join-Path $RepoRoot "docker-compose.local-dev.yml"

Ensure-EnvFile -EnvFile $EnvFile -ExampleFile $EnvExample
$ComposeCommand = Resolve-ComposeCommand

Write-Host "[info] Starting local stack using $ComposeFile"
& $ComposeCommand -f $ComposeFile up --build

Write-Host ""
Write-Host "Services exposed locally:"
Write-Host "  • Frontend:   http://localhost:$($env:FRONTEND_PORT ?? 3000)"
Write-Host "  • Backend:    http://localhost:$($env:BACKEND_PORT ?? 8000)/api/admin/ping"
Write-Host "  • ClickHouse: http://localhost:$($env:CLICKHOUSE_HTTP_PORT ?? 8123)"
