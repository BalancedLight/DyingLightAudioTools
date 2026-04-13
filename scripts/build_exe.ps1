Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Test-TkBuildReady {
    param(
        [Parameter(Mandatory = $true)]
        [string] $PythonExe
    )

    & $PythonExe -c "import tkinter; from PyInstaller.utils.hooks.tcl_tk import tcltk_info; raise SystemExit(0 if tcltk_info.available else 1)" 2>$null
    return $LASTEXITCODE -eq 0
}

function Get-TkEnvironment {
    $roots = @()

    $localPythonRoot = Join-Path $env:LOCALAPPDATA "Programs\Python\Python314"
    if (Test-Path $localPythonRoot) {
        $roots += $localPythonRoot
    }

    $pythonExeRoot = Split-Path -Parent (Get-Command python -ErrorAction SilentlyContinue | ForEach-Object { $_.Source } | Select-Object -First 1)
    if ($pythonExeRoot) {
        $roots += $pythonExeRoot
    }

    $roots += "C:\Python314"

    foreach ($root in $roots | Select-Object -Unique) {
        if (-not $root) {
            continue
        }
        $tclRoot = Join-Path $root "tcl"
        if (-not (Test-Path $tclRoot)) {
            continue
        }

        $tclDir = Get-ChildItem -Path $tclRoot -Directory -Filter "tcl8*" -ErrorAction SilentlyContinue |
            Sort-Object Name -Descending |
            Select-Object -First 1
        $tkDir = Get-ChildItem -Path $tclRoot -Directory -Filter "tk8*" -ErrorAction SilentlyContinue |
            Sort-Object Name -Descending |
            Select-Object -First 1
        if ($null -ne $tclDir -and $null -ne $tkDir) {
            return @{
                TCL_LIBRARY = $tclDir.FullName
                TK_LIBRARY  = $tkDir.FullName
            }
        }
    }

    return $null
}

function Get-PreferredPythonExe {
    $localPython = Join-Path $env:LOCALAPPDATA "Programs\Python\Python314\python.exe"
    if ((Test-Path $localPython) -and (Test-TkBuildReady -PythonExe $localPython)) {
        return $localPython
    }

    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $pythonCmd -and (Test-TkBuildReady -PythonExe $pythonCmd.Source)) {
        return $pythonCmd.Source
    }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $pyLauncher) {
        & $pyLauncher.Source -3 -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0) {
            $resolvedExe = (& $pyLauncher.Source -3 -c "import sys; print(sys.executable)" 2>$null).Trim()
            if ($resolvedExe -and (Test-Path $resolvedExe) -and (Test-TkBuildReady -PythonExe $resolvedExe)) {
                return $resolvedExe
            }
        }
    }

    return $null
}

$pythonExe = Get-PreferredPythonExe
if ([string]::IsNullOrWhiteSpace($pythonExe)) {
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($null -eq $pythonCmd) {
        throw "Could not find Python. Install Python 3.11+ with tkinter/Tcl/Tk support."
    }
    $pythonExe = $pythonCmd.Source
}

$tkEnv = Get-TkEnvironment
if ($null -ne $tkEnv) {
    $env:TCL_LIBRARY = $tkEnv.TCL_LIBRARY
    $env:TK_LIBRARY = $tkEnv.TK_LIBRARY
    Write-Host "Using Tcl/Tk runtime:"
    Write-Host "  TCL_LIBRARY=$($env:TCL_LIBRARY)"
    Write-Host "  TK_LIBRARY=$($env:TK_LIBRARY)"
}

& $pythonExe -c "import tkinter; from PyInstaller.utils.hooks.tcl_tk import tcltk_info; raise SystemExit(0 if tcltk_info.available else 1)"
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller still cannot locate Tcl/Tk data for this build. Install a standard Windows Python with Tcl/Tk support, or ensure TCL_LIBRARY and TK_LIBRARY point to valid tcl8.6/tk8.6 folders."
}

Write-Host "Building with Python: $pythonExe"
& $pythonExe -m PyInstaller --noconfirm .\DyingAudio.spec
$pyInstallerExit = $LASTEXITCODE
if ($pyInstallerExit -ne 0) {
    exit $pyInstallerExit
}

$builtExe = Join-Path $projectRoot "dist\DyingAudio.exe"
if (-not (Test-Path $builtExe)) {
    throw "PyInstaller completed without error, but dist\\DyingAudio.exe was not created."
}

Write-Host "Build complete: $builtExe"
exit 0
