call "%~dp0opensearch-env.bat" || exit /b 1

if defined OPENSEARCH_ADDITIONAL_SOURCES (
  for %%a in ("%OPENSEARCH_ADDITIONAL_SOURCES:;=","%") do (
    call "%~dp0%%a"
  )
)

if defined OPENSEARCH_ADDITIONAL_CLASSPATH_DIRECTORIES (
  for %%a in ("%OPENSEARCH_ADDITIONAL_CLASSPATH_DIRECTORIES:;=","%") do (
    set OPENSEARCH_CLASSPATH=!OPENSEARCH_CLASSPATH!;!OPENSEARCH_HOME!/%%a/*
  )
)

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting OPENSEARCH_JAVA_OPTS
set OPENSEARCH_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %OPENSEARCH_JAVA_OPTS%

"%JAVA%" ^
  %OPENSEARCH_JAVA_OPTS% ^
  -Dopensearch.path.home="%OPENSEARCH_HOME%" ^
  -Dopensearch.path.conf="%OPENSEARCH_PATH_CONF%" ^
  -Dopensearch.distribution.type="%OPENSEARCH_DISTRIBUTION_TYPE%" ^
  -cp "%OPENSEARCH_CLASSPATH%" ^
  "%OPENSEARCH_MAIN_CLASS%" ^
  %*

exit /b %ERRORLEVEL%
