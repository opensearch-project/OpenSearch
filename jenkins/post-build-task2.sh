#!/bin/bash
set -e

OLDIFS=$IFS
ISSUE_TITLE="Jenkins Randomized Gradle Check Failed on ${sha1}"

###############################################

function create_issue() {

echo create new issue

curl -X "POST" "https://api.github.com/repos/opensearch-project/OpenSearch/issues?state=all" \
     -H "Cookie: logged_in=no" \
     -H "Authorization: token $GITHUB_PASSWORD_CREDENTIAL" \
     -H "Content-Type: text/x-markdown; charset=utf-8" \
     -d $'{
  "title": "'$ISSUE_TITLE'",
  "body": "@opensearch-project/opensearch-core team, please take a look at Build '${BUILD_NUMBER}' in gradle_check log for more information.",
  "labels": [
    "cicd",
    "bug"
  ]
}' > create_issue_output.json

} 

###############################################

echo $sha1

if echo $sha1 | grep pr
then
      echo "No need to send as it is PR checks"
else
    echo -e "Jenkins Randomized Gradle Check Build ${BUILD_NUMBER} Failed on ${sha1} `date`: \n${BUILD_URL} \n\nGitHub Issue: https://github.com/opensearch-project/OpenSearch/issues?q=is%3Aissue+is%3Aopen+Jenkins+Randomized+Gradle+Check" > SLACK_MSG.md
    SLACK_MSG=`cat SLACK_MSG.md`
    SLACK_DATA='{"Content":"'$SLACK_MSG'"}'

    curl -X POST -H 'Content-type: application/json' --data "$SLACK_DATA" ${SLACK_OPENSEARCH_CORE}

    
    MATCH_COUNTER=0
    echo ISSUE_TITLE: $ISSUE_TITLE
    IFS=`echo -en "\n\b"` && for i in `curl -s https://api.github.com/repos/opensearch-project/OpenSearch/issues -H "Authorization: token $GITHUB_PASSWORD_CREDENTIAL" | jq -r .[].title`;
    do 
        echo $i
        if echo $i | grep -qw "$ISSUE_TITLE";
        then
            echo match
            MATCH_COUNTER=$((MATCH_COUNTER+1))
        else 
            echo not match
        fi
    done

    if [ "$MATCH_COUNTER" -eq 0 ]; then create_issue; fi


fi
