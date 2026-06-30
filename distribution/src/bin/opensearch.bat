@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

SET params='%*'
SET checkpassword=Y

:loop
FOR /F "usebackq tokens=1* delims= " %%A IN (!params!) DO (
    SET current=%%A
    SET params='%%B'
	SET silent=N

	IF "!current!" == "-s" (
		SET silent=Y
	)
	IF "!current!" == "--silent" (
		SET silent=Y
	)

	IF "!current!" == "-h" (
		SET checkpassword=N
	)
	IF "!current!" == "--help" (
		SET checkpassword=N
	)

	IF "!current!" == "-V" (
		SET checkpassword=N
	)
	IF "!current!" == "--version" (
		SET checkpassword=N
	)

	IF "!silent!" == "Y" (
		SET nopauseonerror=Y
	) ELSE (
	    IF "x!newparams!" NEQ "x" (
	        SET newparams=!newparams! !current!
        ) ELSE (
            SET newparams=!current!
        )
	)

    IF "x!params!" NEQ "x" (
		GOTO loop
	)
)

CALL "%~dp0opensearch-env.bat" || exit /b 1
IF ERRORLEVEL 1 (
	IF NOT DEFINED nopauseonerror (
		PAUSE
	)
	EXIT /B %ERRORLEVEL%
)

if "%SERVICE_LOG_DIR%" == "" set SERVICE_LOG_DIR=%OPENSEARCH_HOME%\logs
rem The logs directory must exist for the service to start.
if not exist "%SERVICE_LOG_DIR%" (
	mkdir "%SERVICE_LOG_DIR%"
)

IF "%checkpassword%"=="Y" (
  CALL "%~dp0opensearch-keystore.bat" has-passwd --silent
  IF !ERRORLEVEL! EQU 0 (
    if defined KEYSTORE_PASSWORD (
      ECHO Using value of KEYSTORE_PASSWORD from the environment
    ) else (
      SET /P KEYSTORE_PASSWORD=OpenSearch keystore password:
      IF !ERRORLEVEL! NEQ 0 (
        ECHO Failed to read keystore password on standard input
        EXIT /B !ERRORLEVEL!
      )
    )
  )
)

if not defined OPENSEARCH_TMPDIR (
  for /f "tokens=* usebackq" %%a in (`CALL "%JAVA%" -cp "!OPENSEARCH_CLASSPATH!" "org.opensearch.tools.launchers.TempDirectory"`) do set  OPENSEARCH_TMPDIR=%%a
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
for /F "usebackq delims=" %%a in (`CALL "%JAVA%" -cp "!OPENSEARCH_CLASSPATH!" "org.opensearch.tools.launchers.JvmOptionsParser" "!OPENSEARCH_PATH_CONF!" ^|^| echo jvm_options_parser_failed`) do set OPENSEARCH_JAVA_OPTS=%%a
@endlocal & set "MAYBE_JVM_OPTIONS_PARSER_FAILED=%OPENSEARCH_JAVA_OPTS%" & set OPENSEARCH_JAVA_OPTS=%OPENSEARCH_JAVA_OPTS%

if "%MAYBE_JVM_OPTIONS_PARSER_FAILED%" == "jvm_options_parser_failed" (
  exit /b 1
)

rem windows batch pipe will choke on special characters in strings
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^^=^^^^!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^&=^^^&!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^|=^^^|!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^<=^^^<!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^>=^^^>!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^\=^^^\!

ECHO.!KEYSTORE_PASSWORD!| "%JAVA%" %OPENSEARCH_JAVA_OPTS% -Dopensearch ^
  -Dopensearch.path.home="%OPENSEARCH_HOME%" -Dopensearch.path.conf="%OPENSEARCH_PATH_CONF%" ^
  -Dopensearch.distribution.type="%OPENSEARCH_DISTRIBUTION_TYPE%" ^
  -Dopensearch.bundled_jdk="%OPENSEARCH_BUNDLED_JDK%" ^
  -cp "%OPENSEARCH_CLASSPATH%" "org.opensearch.bootstrap.OpenSearch" !newparams!

endlocal
endlocal
exit /b %ERRORLEVEL%
