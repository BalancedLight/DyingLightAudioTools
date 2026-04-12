Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($null -ne $pythonCmd) {
    & $pythonCmd.Source -m PyInstaller --noconfirm .\DyingAudio.spec
    exit $LASTEXITCODE
}

$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if ($null -ne $pyLauncher) {
    & $pyLauncher.Source -3 -m PyInstaller --noconfirm .\DyingAudio.spec
    exit $LASTEXITCODE
}

throw "Could not find Python. Install Python 3.11+ or make sure the Python launcher is available, then rerun the EXE build."
