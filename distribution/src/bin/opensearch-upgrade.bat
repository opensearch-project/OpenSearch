@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set OPENSEARCH_MAIN_CLASS=org.opensearch.upgrade.UpgradeCli
set OPENSEARCH_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/upgrade-cli
call "%~dp0opensearch-cli.bat" ^
  %%* ^
  || goto exit


endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
