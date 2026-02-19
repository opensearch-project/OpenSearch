@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set OPENSEARCH_MAIN_CLASS=org.opensearch.tools.cli.fips.truststore.FipsTrustStoreCommand
set OPENSEARCH_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/fips-demo-installer-cli

call "%~dp0opensearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
