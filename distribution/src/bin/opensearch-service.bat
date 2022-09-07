@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set NOJAVA=nojava
if /i "%1" == "install" set NOJAVA=

call "%~dp0opensearch-env.bat" %NOJAVA% || exit /b 1

rem opensearch-service-x64.exe is based off of the Apache Commons Daemon procrun service application.
rem Run "opensearch-service-x64.exe version" for version information.
rem Run "opensearch-service-x64.exe help" for command options.
rem See https://commons.apache.org/proper/commons-daemon/procrun.html for more information.
set EXECUTABLE=%OPENSEARCH_HOME%\bin\opensearch-service-x64.exe
if "%SERVICE_ID%" == "" set SERVICE_ID=opensearch-service-x64
set ARCH=64-bit

if EXIST "%EXECUTABLE%" goto okExe
echo opensearch-service-x64.exe was not found...
exit /B 1

:okExe
set OPENSEARCH_VERSION=${project.version}

if "%SERVICE_LOG_DIR%" == "" set SERVICE_LOG_DIR=%OPENSEARCH_HOME%\logs
rem The logs directory must exist for the service to start.
if not exist "%SERVICE_LOG_DIR%" (
	mkdir "%SERVICE_LOG_DIR%"
)

if "x%1x" == "xx" goto displayUsage
set SERVICE_CMD=%1
shift
if "x%1x" == "xx" goto checkServiceCmd
set SERVICE_ID=%1

:checkServiceCmd

if "%LOG_OPTS%" == "" set LOG_OPTS=--LogPath "%SERVICE_LOG_DIR%" --LogPrefix "%SERVICE_ID%" --StdError auto --StdOutput auto

if /i %SERVICE_CMD% == install goto doInstall
if /i %SERVICE_CMD% == remove goto doRemove
if /i %SERVICE_CMD% == start goto doStart
if /i %SERVICE_CMD% == stop goto doStop
if /i %SERVICE_CMD% == manager goto doManagment
echo Unknown option "%SERVICE_CMD%"
exit /B 1

:displayUsage
echo.
echo Usage: opensearch-service.bat install^|remove^|start^|stop^|manager [SERVICE_ID]
goto:eof

:doStart
rem //ES == Execute Service
"%EXECUTABLE%" //ES//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto started
echo Failed starting '%SERVICE_ID%' service
exit /B 1
goto:eof
:started
echo The service '%SERVICE_ID%' has been started
goto:eof

:doStop
rem //SS == Stop Service
"%EXECUTABLE%" //SS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto stopped
echo Failed stopping '%SERVICE_ID%' service
exit /B 1
goto:eof
:stopped
echo The service '%SERVICE_ID%' has been stopped
goto:eof

:doManagment
rem opensearch-service-mgr.exe is based off of the Apache Commons Daemon procrun monitor application.
rem See https://commons.apache.org/proper/commons-daemon/procrun.html for more information.
set EXECUTABLE_MGR=%OPENSEARCH_HOME%\bin\opensearch-service-mgr
rem //ES == Edit Service
"%EXECUTABLE_MGR%" //ES//%SERVICE_ID%
if not errorlevel 1 goto managed
echo Failed starting service manager for '%SERVICE_ID%'
exit /B 1
goto:eof
:managed
echo Successfully started service manager for '%SERVICE_ID%'.
goto:eof

:doRemove
rem Remove the service
rem //DS == Delete Service
"%EXECUTABLE%" //DS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto removed
echo Failed removing '%SERVICE_ID%' service
exit /B 1
goto:eof
:removed
echo The service '%SERVICE_ID%' has been removed
goto:eof

:doInstall
echo Installing service      :  "%SERVICE_ID%"
echo Using JAVA_HOME (%ARCH%):  "%JAVA_HOME%"

rem Check JVM server dll first
if exist "%JAVA_HOME%\jre\bin\server\jvm.dll" (
	set JVM_DLL=\jre\bin\server\jvm.dll
	goto foundJVM
)

rem Check 'server' JRE (JRE installed on Windows Server)
if exist "%JAVA_HOME%\bin\server\jvm.dll" (
	set JVM_DLL=\bin\server\jvm.dll
	goto foundJVM
) else (
  	echo JAVA_HOME ("%JAVA_HOME%"^) points to an invalid Java installation (no jvm.dll found in "%JAVA_HOME%\jre\bin\server" or "%JAVA_HOME%\bin\server"^). Exiting...
  	goto:eof
)

:foundJVM
if not defined OPENSEARCH_TMPDIR (
  for /f "tokens=* usebackq" %%a in (`CALL %JAVA% -cp "!OPENSEARCH_CLASSPATH!" "org.opensearch.tools.launchers.TempDirectory"`) do set OPENSEARCH_TMPDIR=%%a
)

rem The JVM options parser produces the final JVM options to start
rem OpenSearch. It does this by incorporating JVM options in the following
rem way:
rem   - first, system JVM options are applied (these are hardcoded options in
rem     the parser)
rem   - second, JVM options are read from jvm.options and
rem     jvm.options.d/*.options
rem   - third, JVM options from OPENSEARCH_JAVA_OPTS are applied
rem   - fourth, ergonomic JVM options are applied

@setlocal
for /F "usebackq delims=" %%a in (`CALL %JAVA% -cp "!OPENSEARCH_CLASSPATH!" "org.opensearch.tools.launchers.JvmOptionsParser" "!OPENSEARCH_PATH_CONF!" ^|^| echo jvm_options_parser_failed`) do set OPENSEARCH_JAVA_OPTS=%%a
@endlocal & set "MAYBE_JVM_OPTIONS_PARSER_FAILED=%OPENSEARCH_JAVA_OPTS%" & set OPENSEARCH_JAVA_OPTS=%OPENSEARCH_JAVA_OPTS%

if "%MAYBE_JVM_OPTIONS_PARSER_FAILED%" == "jvm_options_parser_failed" (
  exit /b 1
)

rem The output of the JVM options parses is space-delimited. We need to
rem convert to semicolon-delimited and avoid doubled semicolons.
@setlocal
if not "%OPENSEARCH_JAVA_OPTS%" == "" (
  set OPENSEARCH_JAVA_OPTS=!OPENSEARCH_JAVA_OPTS: =;!
  set OPENSEARCH_JAVA_OPTS=!OPENSEARCH_JAVA_OPTS:;;=;!
)
@endlocal & set OPENSEARCH_JAVA_OPTS=%OPENSEARCH_JAVA_OPTS%

if "%OPENSEARCH_JAVA_OPTS:~-1%"==";" set OPENSEARCH_JAVA_OPTS=%OPENSEARCH_JAVA_OPTS:~0,-1%

echo %OPENSEARCH_JAVA_OPTS%

@setlocal EnableDelayedExpansion
for %%a in ("%OPENSEARCH_JAVA_OPTS:;=","%") do (
  set var=%%a
  set other_opt=true
  if "!var:~1,4!" == "-Xms" (
    set XMS=!var:~5,-1!
    set other_opt=false
    call:convertxm !XMS! JVM_MS
  )
  if "!var:~1,16!" == "-XX:MinHeapSize=" (
    set XMS=!var:~17,-1!
    set other_opt=false
    call:convertxm !XMS! JVM_MS
  )
  if "!var:~1,4!" == "-Xmx" (
    set XMX=!var:~5,-1!
    set other_opt=false
    call:convertxm !XMX! JVM_MX
  )
  if "!var:~1,16!" == "-XX:MaxHeapSize=" (
    set XMX=!var:~17,-1!
    set other_opt=false
    call:convertxm !XMX! JVM_MX
  )
  if "!var:~1,4!" == "-Xss" (
    set XSS=!var:~5,-1!
    set other_opt=false
    call:convertxk !XSS! JVM_SS
  )
  if "!var:~1,20!" == "-XX:ThreadStackSize=" (
    set XSS=!var:~21,-1!
    set other_opt=false
    call:convertxk !XSS! JVM_SS
  )
  if "!other_opt!" == "true" set OTHER_JAVA_OPTS=!OTHER_JAVA_OPTS!;!var!
)
@endlocal & set JVM_MS=%JVM_MS% & set JVM_MX=%JVM_MX% & set JVM_SS=%JVM_SS% & set OTHER_JAVA_OPTS=%OTHER_JAVA_OPTS%

if "%JVM_MS%" == "" (
  echo minimum heap size not set; configure using -Xms via "%OPENSEARCH_PATH_CONF%/jvm.options.d", or OPENSEARCH_JAVA_OPTS
  goto:eof
)
if "%JVM_MX%" == "" (
  echo maximum heap size not set; configure using -Xmx via "%OPENSEARCH_PATH_CONF%/jvm.options.d", or OPENSEARCH_JAVA_OPTS
  goto:eof
)
if "%JVM_SS%" == "" (
  echo thread stack size not set; configure using -Xss via "%OPENSEARCH_PATH_CONF%/jvm.options.d", or OPENSEARCH_JAVA_OPTS
  goto:eof
)
set OTHER_JAVA_OPTS=%OTHER_JAVA_OPTS:"=%
set OTHER_JAVA_OPTS=%OTHER_JAVA_OPTS:~1%

set OPENSEARCH_PARAMS=-Dopensearch;-Dopensearch.path.home="%OPENSEARCH_HOME%";-Dopensearch.path.conf="%OPENSEARCH_PATH_CONF%";-Dopensearch.distribution.type="%OPENSEARCH_DISTRIBUTION_TYPE%";-Dopensearch.bundled_jdk="%OPENSEARCH_BUNDLED_JDK%"

if "%OPENSEARCH_START_TYPE%" == "" set OPENSEARCH_START_TYPE=manual
if "%OPENSEARCH_STOP_TIMEOUT%" == "" set OPENSEARCH_STOP_TIMEOUT=0

if "%SERVICE_DISPLAY_NAME%" == "" set SERVICE_DISPLAY_NAME=OpenSearch %OPENSEARCH_VERSION% (%SERVICE_ID%)
if "%SERVICE_DESCRIPTION%" == "" set SERVICE_DESCRIPTION=OpenSearch %OPENSEARCH_VERSION% Windows Service - https://opensearch.org

if not "%SERVICE_USERNAME%" == "" (
	if not "%SERVICE_PASSWORD%" == "" (
		set SERVICE_PARAMS=%SERVICE_PARAMS% --ServiceUser "%SERVICE_USERNAME%" --ServicePassword "%SERVICE_PASSWORD%"
	)
)
rem //IS == Install Service
"%EXECUTABLE%" //IS//%SERVICE_ID% --Startup %OPENSEARCH_START_TYPE% --StopTimeout %OPENSEARCH_STOP_TIMEOUT% --StartClass org.opensearch.bootstrap.OpenSearch --StartMethod main ++StartParams --quiet --StopClass org.opensearch.bootstrap.OpenSearch --StopMethod close --Classpath "%OPENSEARCH_CLASSPATH%" --JvmMs %JVM_MS% --JvmMx %JVM_MX% --JvmSs %JVM_SS% --JvmOptions %OTHER_JAVA_OPTS% ++JvmOptions %OPENSEARCH_PARAMS% %LOG_OPTS% --PidFile "%SERVICE_ID%.pid" --DisplayName "%SERVICE_DISPLAY_NAME%" --Description "%SERVICE_DESCRIPTION%" --Jvm "%JAVA_HOME%%JVM_DLL%" --StartMode jvm --StopMode jvm --StartPath "%OPENSEARCH_HOME%" %SERVICE_PARAMS% ++Environment HOSTNAME="%%COMPUTERNAME%%"

if not errorlevel 1 goto installed
echo Failed installing '%SERVICE_ID%' service
exit /B 1
goto:eof

:installed
echo The service '%SERVICE_ID%' has been installed.
goto:eof

:err
echo JAVA_HOME environment variable must be set!
pause
goto:eof

rem ---
rem Function for converting Xm[s|x] values into MB which Commons Daemon accepts
rem ---
:convertxm
set value=%~1
rem extract last char (unit)
set unit=%value:~-1%
rem assume the unit is specified
set conv=%value:~0,-1%

if "%unit%" == "k" goto kilo
if "%unit%" == "K" goto kilo
if "%unit%" == "m" goto mega
if "%unit%" == "M" goto mega
if "%unit%" == "g" goto giga
if "%unit%" == "G" goto giga

rem no unit found, must be bytes; consider the whole value
set conv=%value%
rem convert to KB
set /a conv=%conv% / 1024
:kilo
rem convert to MB
set /a conv=%conv% / 1024
goto mega
:giga
rem convert to MB
set /a conv=%conv% * 1024
:mega
set "%~2=%conv%"
goto:eof

:convertxk
set value=%~1
rem extract last char (unit)
set unit=%value:~-1%
rem assume the unit is specified
set conv=%value:~0,-1%

if "%unit%" == "k" goto kilo
if "%unit%" == "K" goto kilo
if "%unit%" == "m" goto mega
if "%unit%" == "M" goto mega
if "%unit%" == "g" goto giga
if "%unit%" == "G" goto giga

rem no unit found, must be bytes; consider the whole value
set conv=%value%
rem convert to KB
set /a conv=%conv% / 1024
goto kilo
:mega
rem convert to KB
set /a conv=%conv% * 1024
goto kilo
:giga
rem convert to KB
set /a conv=%conv% * 1024 * 1024
:kilo
set "%~2=%conv%"
goto:eof

endlocal
endlocal

exit /b %ERRORLEVEL%
