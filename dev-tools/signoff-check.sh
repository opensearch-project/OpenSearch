#!/bin/sh

### Script to check for signoff presents on commits

# Validate input parameters
if [ -z $1 ] || [ -z $2 ]
then
   echo usage: ./signoff-check.sh commit1 commit2
   echo
   echo Checks all of the commits between commit1 \(exclusive\) and commit2 \(inclusive\)
   echo were made with the --signoff flag enabled
   exit 1
fi

# Get the list of commit ids to check from git
commits=$(git rev-list $1..$2)

# Scan each commit for the sign off message
missingSignoff=0
for commitId in $commits; do
    commitMessage=$(git rev-list --format=%B --max-count=1 $commitId)
    signoffStringCount=$(echo $commitMessage | grep -c Signed-off-by)
    if [ $signoffStringCount -eq 0 ]; then
      echo !!! Commit "$commitId" is missing signoff, amend this commit with the --signoff flag
      missingSignoff=1
    fi
done

# Return non-zero error code if any commits were missing signoff
exit $missingSignoff

