param(
    [string]$OutputDir
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendRoot = Split-Path -Parent $scriptDir
$distDir = if ($OutputDir) { $OutputDir } else { Join-Path $scriptDir "dist" }
$buildVenv = Join-Path $scriptDir ".venv-build"
$python = "python"
$iconBase64Path = Join-Path $scriptDir "resources\gpw_agent_icon.b64"
$iconPath = Join-Path $scriptDir "resources\gpw-agent.ico"

if (Test-Path $iconBase64Path) {
    $base64Raw = Get-Content -Path $iconBase64Path -Raw
    if (-not [string]::IsNullOrWhiteSpace($base64Raw)) {
        $normalized = ($base64Raw -replace "\s", "")
        [IO.File]::WriteAllBytes($iconPath, [Convert]::FromBase64String($normalized))
    }
}

if (-not (Test-Path $buildVenv)) {
    Write-Host "Tworzenie wirtualnego środowiska..."
    & $python -m venv $buildVenv
}

$venvPython = Join-Path $buildVenv "Scripts\python.exe"
$venvPip = Join-Path $buildVenv "Scripts\pip.exe"
$pyinstallerExe = Join-Path $buildVenv "Scripts\pyinstaller.exe"

Write-Host "Aktualizacja pip..."
& $venvPython -m pip install --upgrade pip > $null

Write-Host "Instalacja zależności..."
& $venvPip install -r (Join-Path $backendRoot "requirements.txt") pyinstaller > $null

Write-Host "Budowanie aplikacji (PyInstaller)..."
& $pyinstallerExe --noconfirm --clean (Join-Path $scriptDir "gpw_agent.spec")

if ($OutputDir) {
    Write-Host "Przenoszenie wyników do $distDir"
    if (-not (Test-Path $distDir)) {
        New-Item -ItemType Directory -Path $distDir | Out-Null
    }
    Copy-Item -Path (Join-Path $scriptDir "dist\GPWAnalyticsAgent") -Destination $distDir -Recurse -Force
}

$exePath = Join-Path $scriptDir "dist\GPWAnalyticsAgent\GPWAnalyticsAgent.exe"
if (-not (Test-Path $exePath)) {
    throw "Plik wykonywalny nie został wygenerowany."
}

$shortcutDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs"
if (-not (Test-Path $shortcutDir)) {
    New-Item -ItemType Directory -Path $shortcutDir | Out-Null
}

$shortcutPath = Join-Path $shortcutDir "GPW Analytics Agent.lnk"
$wsh = New-Object -ComObject WScript.Shell
$shortcut = $wsh.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $exePath
$shortcut.WorkingDirectory = Split-Path $exePath
$shortcut.IconLocation = $iconPath
$shortcut.Save()

Write-Host "Skrót utworzony: $shortcutPath"
Write-Host "Budowanie zakończone. Uruchom skrót, aby wystartować aplikację z ikoną w menu Start."
