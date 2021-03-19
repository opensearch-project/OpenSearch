@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set OPENSEARCH_MAIN_CLASS=org.opensearch.index.shard.ShardToolCli
call "%~dp0opensearch-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
