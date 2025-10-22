param(
    [string]$OutputDir
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendRoot = Split-Path -Parent $scriptDir
$distDir = if ($OutputDir) { $OutputDir } else { Join-Path $scriptDir "dist" }
$buildVenv = Join-Path $scriptDir ".venv-build"
$pythonCandidates = @("py", "python")
$pythonCommand = $null
$pythonArgs = @()

foreach ($candidate in $pythonCandidates) {
    $commandInfo = Get-Command $candidate -ErrorAction SilentlyContinue
    if (-not $commandInfo) {
        continue
    }

    $commandPath = $commandInfo.Source
    if ($commandPath -like "*Microsoft\\WindowsApps*") {
        Write-Host "Ignoring Windows Store Python alias at $commandPath"
        continue
    }

    $pythonCommand = $commandPath
    if ($candidate -eq "py") {
        $pythonArgs = @("-3")
    } else {
        $pythonArgs = @()
    }
    break
}

if (-not $pythonCommand) {
    $fallbackSearchRoots = @()
    if ($env:LOCALAPPDATA) {
        $fallbackSearchRoots += Join-Path $env:LOCALAPPDATA "Programs\Python"
    }
    if ($env:ProgramFiles) {
        $fallbackSearchRoots += Join-Path $env:ProgramFiles "Python"
    }
    if (${env:ProgramFiles(x86)}) {
        $fallbackSearchRoots += Join-Path ${env:ProgramFiles(x86)} "Python"
    }

    $foundInterpreters = @()
    foreach ($root in $fallbackSearchRoots) {
        if (-not (Test-Path $root)) {
            continue
        }

        $foundInterpreters += Get-ChildItem -Path $root -Filter "python.exe" -Recurse -ErrorAction SilentlyContinue
    }

    $foundInterpreters = $foundInterpreters | Where-Object { $_.FullName -notlike "*Microsoft\\WindowsApps*" }

    if ($foundInterpreters.Count -gt 0) {
        $pythonExecutable = $foundInterpreters | Sort-Object -Property LastWriteTime -Descending | Select-Object -First 1
        $pythonCommand = $pythonExecutable.FullName
        $pythonArgs = @()
    }

    if (-not $pythonCommand) {
        throw "Python 3 is required but was not found on PATH. Please install it and try again."
    }
}

$pythonDisplay = $pythonCommand
if ($pythonArgs.Count -gt 0) {
    $pythonDisplay = "$pythonDisplay $($pythonArgs -join ' ')"
}
Write-Host "Using Python interpreter: $pythonDisplay"
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
    Write-Host "Creating virtual environment..."
    & $pythonCommand @pythonArgs -m venv $buildVenv
}

$venvPython = Join-Path $buildVenv "Scripts\python.exe"
$venvPip = Join-Path $buildVenv "Scripts\pip.exe"
$pyinstallerExe = Join-Path $buildVenv "Scripts\pyinstaller.exe"

Write-Host "Updating pip..."
& $venvPython -m pip install --upgrade pip > $null

Write-Host "Installing dependencies..."
& $venvPip install -r (Join-Path $backendRoot "requirements.txt") pyinstaller > $null

Write-Host "Building application (PyInstaller)..."
& $pyinstallerExe --noconfirm --clean (Join-Path $scriptDir "gpw_agent.spec")

if ($OutputDir) {
    Write-Host "Moving build outputs to $distDir"
    if (-not (Test-Path $distDir)) {
        New-Item -ItemType Directory -Path $distDir | Out-Null
    }
    Copy-Item -Path (Join-Path $scriptDir "dist\GPWAnalyticsAgent") -Destination $distDir -Recurse -Force
}

$exePath = Join-Path $scriptDir "dist\GPWAnalyticsAgent\GPWAnalyticsAgent.exe"
if (-not (Test-Path $exePath)) {
    throw "Executable file was not generated."
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

Write-Host "Shortcut created: $shortcutPath"
Write-Host "Build complete. Launch the shortcut to start the application with the Start Menu icon."
