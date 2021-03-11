@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set ES_MAIN_CLASS=NodeToolCli
call "%~dp0elasticsearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
