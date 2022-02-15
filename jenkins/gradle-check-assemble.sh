#!/bin/bash
IFS=`echo -ne "\n\b"`

docker ps
docker stop `docker ps -qa` > /dev/null 2>&1

cd search

echo "./gradlew check --no-daemon --no-scan"
./gradlew check --no-daemon --no-scan
GRADLE_CHECK_STATUS=$?


# Archive reports
cd ../
REPORTS_DIR=`find ./search -name reports`
zip -9 -q -r gradle_check_${BUILD_NUMBER}_reports.zip ${REPORTS_DIR}
ls | grep "gradle_check_${BUILD_NUMBER}"

if [ "$GRADLE_CHECK_STATUS" != 0 ]
then
  echo Gradle Check Failed!
  exit 1
fi
