#!/bin/bash

# Copyright OpenSearch Contributors

# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# This script is used in OpenSearch Core repo github actions
# To trigger Jenkins Gradle Check from a PR


JENKINS_URL="https://build.ci.opensearch.org"
TRIGGER_TOKEN=""
GITHUB_USER=""
GITHUB_TOKEN=""

GRADLE_CHECK_COMMAND=""

while getopts "u:t:p:c:" opt; do
  case $opt in
    t)
      TRIGGER_TOKEN="$OPTARG"
      ;;
    u)
      GITHUB_USER="$OPTARG"
      ;;
    p)
      GITHUB_TOKEN="$OPTARG"
      ;;
    c)
      GRADLE_CHECK_COMMAND="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

if [ -z "$TRIGGER_TOKEN" ]; then
  echo "Error: TRIGGER_TOKEN is required. Use -t option to provide it."
  exit 1
fi

if [ -z "$GITHUB_USER" ]; then
  echo "Error: GITHUB_USER is required. Use -u option to provide it."
  exit 1
fi

if [ -z "$GITHUB_TOKEN" ]; then
  echo "Error: GITHUB_TOKEN is required. Use -p option to provide it."
  exit 1
fi


TIMEPASS=0
TIMEOUT=7200
RESULT="null"
PR_TITLE_NEW=`echo $pr_title | tr -dc '[:alnum:] ' | tr '[:upper:]' '[:lower:]'`
PAYLOAD_JSON="{\"pr_from_sha\": \"$pr_from_sha\", \"pr_from_clone_url\": \"$pr_from_clone_url\", \"pr_to_clone_url\": \"$pr_to_clone_url\", \"pr_title\": \"$PR_TITLE_NEW\", \"pr_number\": \"$pr_number\", \"post_merge_action\": \"$post_merge_action\", \"pr_owner\": \"$pr_owner\", \"gradle_check_command\": \"$GRADLE_CHECK_COMMAND\"}"

perform_curl_and_process_with_jq() {
    local url=$1
    local jq_filter=$2
    local max_retries=$3
    local count=0
    local success=false

    while [ "$count" -lt "$max_retries" ]; do
        response=$(curl -s -XGET "${url}api/json" --user ${GITHUB_USER}:${GITHUB_TOKEN})
        processed_response=$(echo "$response" | jq --raw-output "$jq_filter")
        jq_exit_code=$?

        if [ "$jq_exit_code" -eq "0" ]; then
            success=true
            echo "$processed_response"
            break
        else
            echo "Attempt $((count+1))/$max_retries failed. The jq processing failed with exit code: $jq_exit_code. Retrying..."
        fi

        count=$((count+1))
        sleep 10
    done

    if [ "$success" != "true" ]; then
        echo "Failed to retrieve and process data after $max_retries attempts."
        exit 1
    fi
}

echo "Trigger Jenkins workflows"
JENKINS_REQ=`curl -s -XPOST \
     -H "Authorization: Bearer $TRIGGER_TOKEN" \
     -H "Content-Type: application/json" \
     "$JENKINS_URL/generic-webhook-trigger/invoke" \
     --data "$(echo $PAYLOAD_JSON)"`

echo $PAYLOAD_JSON
echo $JENKINS_REQ

QUEUE_URL=$(echo $JENKINS_REQ | jq --raw-output '.jobs."gradle-check".url')
echo QUEUE_URL $QUEUE_URL
echo "wait for jenkins to start workflow" && sleep 15

echo "Check if queue exist in Jenkins after triggering"
if [ -z "$QUEUE_URL" ] || [ "$QUEUE_URL" != "null" ]; then
    while [ "$RESULT" = "null" ] && [ "$TIMEPASS" -le "$TIMEOUT" ]; do
        echo "Use queue information to find build number in Jenkins if available"
        WORKFLOW_URL=$(curl -s -XGET ${JENKINS_URL}/${QUEUE_URL}api/json --user ${GITHUB_USER}:${GITHUB_TOKEN} | jq --raw-output .executable.url)
        echo WORKFLOW_URL $WORKFLOW_URL
    
        if [ -n "$WORKFLOW_URL" ] && [ "$WORKFLOW_URL" != "null" ]; then
    
            RUNNING="true"
    
            echo "Waiting for Jenkins to complete the run"
            while [ "$RUNNING" = "true" ] && [ "$TIMEPASS" -le "$TIMEOUT" ]; do
                echo "Still running, wait for another 30 seconds before checking again, max timeout $TIMEOUT"
                echo "Jenkins Workflow Url: $WORKFLOW_URL"
                TIMEPASS=$(( TIMEPASS + 30 )) && echo time passed: $TIMEPASS
                sleep 30
                RUNNING=$(perform_curl_and_process_with_jq "$WORKFLOW_URL" ".building" 10)
                echo "Workflow running status :$RUNNING"
            done
    
            if [ "$RUNNING" = "true" ]; then
                echo "Timed out"
                RESULT="TIMEOUT"
            else
                echo "Complete the run, checking results now......"
                RESULT=$(curl -s -XGET ${WORKFLOW_URL}api/json --user ${GITHUB_USER}:${GITHUB_TOKEN} | jq --raw-output .result)
            fi
    
        else
            echo "Job not started yet. Waiting for 60 seconds before next attempt."
            TIMEPASS=$(( TIMEPASS + 60 )) && echo time passed: $TIMEPASS
            sleep 60
        fi
    done
fi

echo "Please check jenkins url for logs: $WORKFLOW_URL"
echo "Result: $RESULT"
if [ "$RESULT" == "SUCCESS" ] || [ "$RESULT" == "UNSTABLE" ]; then
    echo "Get codeCoverage.xml" && curl -SLO ${WORKFLOW_URL}artifact/codeCoverage.xml --user ${GITHUB_USER}:${GITHUB_TOKEN}
else
    exit 1
fi
