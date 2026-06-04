@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set OPENSEARCH_MAIN_CLASS=org.opensearch.tools.cli.keystore.KeyStoreCli
set OPENSEARCH_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/keystore-cli
call "%~dp0opensearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
