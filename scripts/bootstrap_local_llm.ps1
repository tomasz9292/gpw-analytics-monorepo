#!/usr/bin/env pwsh
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Log {
    param(
        [Parameter(Mandatory=$true)][string]$Message
    )
    Write-Host "`n>>> $Message"
}

$HomeDir = if ($env:USERPROFILE) { $env:USERPROFILE } elseif ($env:HOME) { $env:HOME } else { [Environment]::GetFolderPath('UserProfile') }
$TargetDir = Join-Path $HomeDir "gpw-llm"
$VenvDir = Join-Path $TargetDir "venv"
$ModelDir = Join-Path $TargetDir "models"
$ModelName = "zephyr-7b-beta.Q4_K_M.gguf"
$ModelUrl = "https://huggingface.co/TheBloke/zephyr-7B-beta-GGUF/resolve/main/$ModelName?download=1"
$PythonCommand = $null

Write-Log "Tworzenie katalogow w $TargetDir"
New-Item -ItemType Directory -Path $ModelDir -Force | Out-Null

$pythonCandidates = @("python", "python3")
foreach ($candidate in $pythonCandidates) {
    if (Get-Command $candidate -ErrorAction SilentlyContinue) {
        $PythonCommand = $candidate
        break
    }
}

if (-not $PythonCommand) {
    Write-Error "Blad: wymagany jest python lub python3"
}

if (-not (Test-Path $VenvDir)) {
    Write-Log "Tworzenie wirtualnego srodowiska"
    & $PythonCommand -m venv $VenvDir
}

$VenvPython = Join-Path $VenvDir "Scripts/python.exe"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = Join-Path $VenvDir "bin/python"
}

if (-not (Test-Path $VenvPython)) {
    Write-Error "Nie mozna odnalezc interpretera wirtualnego srodowiska w $VenvDir"
}

Write-Log "Instalacja zaleznosci w srodowisku"
& $VenvPython -m pip install --upgrade pip
& $VenvPython -m pip install "llama-cpp-python==0.2.78"

$ModelPath = Join-Path $ModelDir $ModelName
if (-not (Test-Path $ModelPath)) {
    Write-Log "Pobieranie przykladowego modelu GGUF"
    if (Get-Command "curl" -ErrorAction SilentlyContinue) {
        & curl -L $ModelUrl -o $ModelPath
    }
    elseif (Get-Command "wget" -ErrorAction SilentlyContinue) {
        & wget $ModelUrl -O $ModelPath
    }
    else {
        Write-Log "Brak curl oraz wget - uzywam Invoke-WebRequest"
        Invoke-WebRequest -Uri $ModelUrl -OutFile $ModelPath
    }
}
else {
    Write-Log "Plik modelu juz istnieje - pomijam pobieranie"
}

$GpuLayers = 0
if (Get-Command "nvidia-smi" -ErrorAction SilentlyContinue -CommandType Application) {
    $GpuLayers = 20
} elseif (Get-Command "rocm-smi" -ErrorAction SilentlyContinue -CommandType Application) {
    $GpuLayers = 16
}

$ConfigFile = Join-Path $TargetDir "config.json"
$Config = @{ model_path = $ModelPath; gpu_layers = $GpuLayers }
$Config | ConvertTo-Json -Depth 3 | Set-Content -Path $ConfigFile -Encoding UTF8

Write-Log "Srodowisko LLM przygotowane"
Write-Host "MODEL_PATH=$ModelPath"
Write-Host "GPU_LAYERS=$GpuLayers"
Write-Host ("Konfiguracja zapisana w {0}" -f $ConfigFile)

