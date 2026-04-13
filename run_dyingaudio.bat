@echo off
setlocal
set "PROJECT_ROOT=%~dp0"
set "PYTHONPATH=%PROJECT_ROOT%src;%PYTHONPATH%"

set "PYTHON_EXE="
for %%P in ("%LocalAppData%\Programs\Python\Python314\pythonw.exe" "%LocalAppData%\Programs\Python\Python313\pythonw.exe" "%LocalAppData%\Programs\Python\Python312\pythonw.exe") do (
    if exist "%%~fP" (
        set "PYTHON_EXE=%%~fP"
        goto :python_found
    )
)
for %%P in ("%LocalAppData%\Programs\Python\Python314\python.exe" "%LocalAppData%\Programs\Python\Python313\python.exe" "%LocalAppData%\Programs\Python\Python312\python.exe") do (
    if exist "%%~fP" (
        set "PYTHON_EXE=%%~fP"
        goto :python_found
    )
)
where py >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_EXE=py -3"
    goto :python_found
)
where python >nul 2>nul
if not errorlevel 1 (
    set "PYTHON_EXE=python"
    goto :python_found
)

echo Could not find a usable Python launcher.
echo Install Python 3 or update run_dyingaudio.bat with the correct path.
pause
exit /b 1

:python_found
if exist "%LocalAppData%\Programs\Python\Python314\tcl\tcl8.6\init.tcl" (
    set "TCL_LIBRARY=%LocalAppData%\Programs\Python\Python314\tcl\tcl8.6"
)
if exist "%LocalAppData%\Programs\Python\Python314\tcl\tk8.6\tk.tcl" (
    set "TK_LIBRARY=%LocalAppData%\Programs\Python\Python314\tcl\tk8.6"
)

%PYTHON_EXE% -m dyingaudio
if errorlevel 1 (
    echo.
    echo DyingAudio failed to start.
    pause
    exit /b %errorlevel%
)
