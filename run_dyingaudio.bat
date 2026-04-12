@echo off
setlocal
set "PROJECT_ROOT=%~dp0"
set "PYTHONPATH=%PROJECT_ROOT%src;%PYTHONPATH%"
pythonw -m dyingaudio
