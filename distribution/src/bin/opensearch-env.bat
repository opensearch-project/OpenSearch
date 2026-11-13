set SCRIPT=%0

rem determine OpenSearch home; to do this, we strip from the path until we
rem find bin, and then strip bin (there is an assumption here that there is no
rem nested directory under bin also named bin)
if not defined OPENSEARCH_HOME goto opensearch_home_start_setup
goto opensearch_home_done_setup

:opensearch_home_start_setup
for %%I in (%SCRIPT%) do set OPENSEARCH_HOME=%%~dpI

:opensearch_home_loop
for %%I in ("%OPENSEARCH_HOME:~1,-1%") do set DIRNAME=%%~nxI
if not "%DIRNAME%" == "bin" (
  for %%I in ("%OPENSEARCH_HOME%..") do set OPENSEARCH_HOME=%%~dpfI
  goto opensearch_home_loop
)
for %%I in ("%OPENSEARCH_HOME%..") do set OPENSEARCH_HOME=%%~dpfI

:opensearch_home_done_setup
rem now set the classpath — include lib\* (core jars)
set OPENSEARCH_CLASSPATH=!OPENSEARCH_HOME!\lib\*

set HOSTNAME=%COMPUTERNAME%

if not defined OPENSEARCH_PATH_CONF (
  set OPENSEARCH_PATH_CONF=!OPENSEARCH_HOME!\config
)

rem now make OPENSEARCH_PATH_CONF absolute
for %%I in ("%OPENSEARCH_PATH_CONF%..") do set OPENSEARCH_PATH_CONF=%%~dpfI

REM FIPS mode is runtime-configured via env var (default: false)
if "%OPENSEARCH_FIPS_MODE%"=="" set "OPENSEARCH_FIPS_MODE=false"

rem Check if any bc-fips jar exists on classpath
set "FOUND_BC_FIPS="
if exist "%OPENSEARCH_HOME%\lib\bc-fips*.jar" (
    set "FOUND_BC_FIPS=true"
)

REM Enable if value equals "true" (case-insensitive); FIPS mode without the BC-FIPS JAR is an error
set "JAVA_SECURITY_MODE=default"
if /I "%OPENSEARCH_FIPS_MODE%"=="true" (
    if not "%FOUND_BC_FIPS%"=="true" (
        echo OPENSEARCH_FIPS_MODE=true requires the BouncyCastle FIPS jars in %OPENSEARCH_HOME%\lib, but this distribution does not contain them 1>&2
        exit /b 1
    )
    echo FIPS mode enabled, setting JVM options.
    set "JAVA_SECURITY_MODE=fips"
    set "OPENSEARCH_JAVA_OPTS=-Dorg.bouncycastle.fips.approved_only=true %OPENSEARCH_JAVA_OPTS%"
)

set OPENSEARCH_DISTRIBUTION_TYPE=${opensearch.distribution.type}
set OPENSEARCH_BUNDLED_JDK=${opensearch.bundled_jdk}

if "%OPENSEARCH_BUNDLED_JDK%" == "false" (
  echo "warning: no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release" >&2
)

cd /d "%OPENSEARCH_HOME%"

rem now set the path to java, pass "nojava" arg to skip setting JAVA_HOME and JAVA
if "%1" == "nojava" (
   exit /b
)

rem comparing to empty string makes this equivalent to bash -v check on env var
rem and allows to effectively force use of the bundled jdk when launching OpenSearch
rem by setting OPENSEARCH_JAVA_HOME= and JAVA_HOME=
if not "%OPENSEARCH_JAVA_HOME%" == "" (
  set "JAVA=%OPENSEARCH_JAVA_HOME%\bin\java.exe"
  set JAVA_TYPE=OPENSEARCH_JAVA_HOME
) else if not "%JAVA_HOME%" == "" (
  set "JAVA=%JAVA_HOME%\bin\java.exe"
  set JAVA_TYPE=JAVA_HOME
) else (
  set "JAVA=%OPENSEARCH_HOME%\jdk\bin\java.exe"
  set "JAVA_HOME=%OPENSEARCH_HOME%\jdk"
  set JAVA_TYPE=bundled jdk
)

if not exist !JAVA! (
  echo "could not find java in !JAVA_TYPE! at !JAVA!" >&2
  exit /b 1
)

rem do not let JAVA_TOOL_OPTIONS slip in (as the JVM does by default)
if defined JAVA_TOOL_OPTIONS (
  echo warning: ignoring JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%
  set JAVA_TOOL_OPTIONS=
)

rem JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
rem warn them that we are not observing the value of %JAVA_OPTS%
if defined JAVA_OPTS (
  (echo|set /p=warning: ignoring JAVA_OPTS=%JAVA_OPTS%; )
  echo pass JVM parameters via OPENSEARCH_JAVA_OPTS
)

rem check the Java version
"%JAVA%" -cp "%OPENSEARCH_CLASSPATH%" "org.opensearch.tools.java_version_checker.JavaVersionChecker" || exit /b 1

rem Generate the java.security file for the JDK selected above. It replaces the JDK's own file (note the "=="), so the
rem provider list is exactly the one declared: fips_java.security in FIPS mode, otherwise the JDK's providers followed by
rem BouncyCastle FIPS. Prints nothing when OPENSEARCH_JAVA_OPTS already sets java.security.properties.
set "JAVA_SECURITY_FILE="
for /F "usebackq delims=" %%a in (`CALL "%JAVA%" -cp "!OPENSEARCH_CLASSPATH!" "org.opensearch.tools.launchers.JavaSecurityFile" "!OPENSEARCH_PATH_CONF!" "!JAVA_SECURITY_MODE!" ^|^| echo java_security_file_failed`) do set "JAVA_SECURITY_FILE=%%a"
if "!JAVA_SECURITY_FILE!" == "java_security_file_failed" (
  exit /b 1
)
if defined JAVA_SECURITY_FILE (
  set "OPENSEARCH_JAVA_OPTS=-Djava.security.properties==""!JAVA_SECURITY_FILE!"" %OPENSEARCH_JAVA_OPTS%"
)
