Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

function Test-IsWindowsAppsAlias {
    param(
        [Parameter(Mandatory = $true)]
        [string] $CommandPath
    )

    if ([string]::IsNullOrWhiteSpace($CommandPath)) {
        return $false
    }

    $windowsAppsRoot = Join-Path $env:LOCALAPPDATA "Microsoft\WindowsApps"
    $fullPath = [System.IO.Path]::GetFullPath($CommandPath)
    return $fullPath.StartsWith($windowsAppsRoot, [System.StringComparison]::OrdinalIgnoreCase)
}

function Get-CommandPath {
    param(
        [Parameter(Mandatory = $true)]
        $CommandInfo
    )

    if ($null -ne $CommandInfo.Source -and -not [string]::IsNullOrWhiteSpace($CommandInfo.Source)) {
        return $CommandInfo.Source
    }

    return $CommandInfo.Path
}

function Test-TkBuildReady {
    param(
        [Parameter(Mandatory = $true)]
        [string] $PythonExe,

        [int] $TimeoutSeconds = 15
    )

    if ([string]::IsNullOrWhiteSpace($PythonExe) -or -not (Test-Path $PythonExe) -or (Test-IsWindowsAppsAlias -CommandPath $PythonExe)) {
        return $false
    }

    $probeScript = 'import tkinter; from PyInstaller.utils.hooks.tcl_tk import tcltk_info; raise SystemExit(0 if tcltk_info.available else 1)'
    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $PythonExe
    $startInfo.Arguments = '-c "' + $probeScript + '"'
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo

    try {
        [void] $process.Start()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            try {
                $process.Kill($true)
            }
            catch {
            }
            Write-Warning "Timed out while probing Python interpreter: $PythonExe"
            return $false
        }

        [void] $process.StandardOutput.ReadToEnd()
        [void] $process.StandardError.ReadToEnd()
        return $process.ExitCode -eq 0
    }
    catch {
        return $false
    }
    finally {
        $process.Dispose()
    }
}

function Get-TkEnvironment {
    $roots = @()

    $localPythonRoot = Join-Path $env:LOCALAPPDATA "Programs\Python\Python314"
    if (Test-Path $localPythonRoot) {
        $roots += $localPythonRoot
    }

    $pythonCmd = Get-Command python -All -ErrorAction SilentlyContinue |
        Where-Object {
            $commandPath = Get-CommandPath -CommandInfo $_
            -not (Test-IsWindowsAppsAlias -CommandPath $commandPath)
        } |
        Select-Object -First 1
    $pythonExePath = if ($null -ne $pythonCmd) { Get-CommandPath -CommandInfo $pythonCmd } else { $null }
    $pythonExeRoot = Split-Path -Parent $pythonExePath
    if ($pythonExeRoot) {
        $roots += $pythonExeRoot
        if ((Split-Path -Leaf $pythonExeRoot) -ieq "bin") {
            $roots += Split-Path -Parent $pythonExeRoot
        }
    }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $pyLauncher) {
        $resolvedExe = (& (Get-CommandPath -CommandInfo $pyLauncher) -3 -c "import sys; print(sys.executable)" 2>$null).Trim()
        if ($resolvedExe -and (Test-Path $resolvedExe)) {
            $roots += Split-Path -Parent $resolvedExe
        }
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

    $venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
    if ((Test-Path $venvPython) -and (Test-TkBuildReady -PythonExe $venvPython)) {
        return $venvPython
    }

    $pythonCmd = Get-Command python -All -ErrorAction SilentlyContinue |
        Where-Object {
            $commandPath = Get-CommandPath -CommandInfo $_
            -not (Test-IsWindowsAppsAlias -CommandPath $commandPath)
        } |
        Select-Object -First 1
    if ($null -ne $pythonCmd) {
        $pythonCmdPath = Get-CommandPath -CommandInfo $pythonCmd
        if (Test-TkBuildReady -PythonExe $pythonCmdPath) {
            return $pythonCmdPath
        }
    }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $pyLauncher) {
        $pyLauncherPath = Get-CommandPath -CommandInfo $pyLauncher
        & $pyLauncherPath -3 -c "import sys; print(sys.executable)" 2>$null
        if ($LASTEXITCODE -eq 0) {
            $resolvedExe = (& $pyLauncherPath -3 -c "import sys; print(sys.executable)" 2>$null).Trim()
            if ($resolvedExe -and (Test-Path $resolvedExe) -and (Test-TkBuildReady -PythonExe $resolvedExe)) {
                return $resolvedExe
            }
        }
    }

    return $null
}

$pythonExe = Get-PreferredPythonExe
if ([string]::IsNullOrWhiteSpace($pythonExe)) {
    $pythonCmd = Get-Command python -All -ErrorAction SilentlyContinue |
        Where-Object {
            $commandPath = Get-CommandPath -CommandInfo $_
            -not (Test-IsWindowsAppsAlias -CommandPath $commandPath)
        } |
        Select-Object -First 1
    if ($null -eq $pythonCmd) {
        throw "Could not find a real Python interpreter. Install Python 3.11+ with tkinter/Tcl/Tk support and disable the Windows Apps python alias if it is shadowing your install."
    }
    $pythonExe = Get-CommandPath -CommandInfo $pythonCmd
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
