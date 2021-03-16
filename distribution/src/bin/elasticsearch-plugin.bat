@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set OPENSEARCH_MAIN_CLASS=org.opensearch.plugins.PluginCli
set OPENSEARCH_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/plugin-cli
call "%~dp0opensearch-cli.bat" ^
  %%* ^
  || goto exit


endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
