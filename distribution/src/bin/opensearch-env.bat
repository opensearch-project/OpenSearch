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
rem now set the classpath
set OPENSEARCH_CLASSPATH=!OPENSEARCH_HOME!\lib\*

set HOSTNAME=%COMPUTERNAME%

if not defined OPENSEARCH_PATH_CONF (
  set OPENSEARCH_PATH_CONF=!OPENSEARCH_HOME!\config
)

rem now make OPENSEARCH_PATH_CONF absolute
for %%I in ("%OPENSEARCH_PATH_CONF%..") do set OPENSEARCH_PATH_CONF=%%~dpfI

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
