#!/bin/bash

# Start container with required tools to build packages
# Requires Docker
# Script usage: bash ./builder.sh

set -e

# ====
# Checks that the script is run from the intended location
# ====
function check_project_root_folder() {
    current=$(basename "$(pwd)")

    if [[ "$1" != "./builder.sh" && "$1" != "builder.sh" ]]; then
        echo "Run the script from its location"
        usage
        exit 1
    fi
    # Change working directory to the root of the repository
    cd ../..
}

# ====
# Parse arguments
# ====
function parse_args() {

    while getopts ":p:r:R:s:d:a:Dh" arg; do
        case $arg in
        h)
            usage
            exit 1
            ;;
        p)
            INDEXER_PLUGINS_BRANCH=$OPTARG
            ;;
        r)
            INDEXER_REPORTING_BRANCH=$OPTARG
            ;;
        R)
            REVISION=$OPTARG
            ;;
        s)
            IS_STAGE=$OPTARG
            ;;
        d)
            DISTRIBUTION=$OPTARG
            ;;
        a)
            ARCHITECTURE=$OPTARG
            ;;
        D)
            DESTROY=true
            ;;
        :)
            echo "Error: -${OPTARG} requires an argument"
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${arg}"
            exit 1
            ;;
        esac
    done

    ## Set defaults:
    [ -z "$INDEXER_PLUGINS_BRANCH" ] && INDEXER_PLUGINS_BRANCH="main"
    [ -z "$INDEXER_REPORTING_BRANCH" ] && INDEXER_REPORTING_BRANCH="main"
    [ -z "$REVISION" ] && REVISION="0"
    [ -z "$IS_STAGE" ] && IS_STAGE="false"
    [ -z "$DISTRIBUTION" ] && DISTRIBUTION="rpm"
    [ -z "$ARCHITECTURE" ] && ARCHITECTURE="x64"
    [ -z "$DESTROY" ] && DESTROY=false
}

# ====
# Displays usage
# ====
function usage() {
    echo "Usage: $0 [args]"
    echo ""
    echo "Arguments:"
    echo -e "-p INDEXER_PLUGINS_BRANCH\t[Optional] wazuh-indexer-plugins repo branch, default is 'main'."
    echo -e "-r INDEXER_REPORTING_BRANCH\t[Optional] wazuh-indexer-reporting repo branch, default is 'main'."
    echo -e "-R REVISION\t[Optional] Package revision, default is '0'."
    echo -e "-s STAGE\t[Optional] Staging build, default is 'false'."
    echo -e "-d DISTRIBUTION\t[Optional] Distribution, default is 'rpm'."
    echo -e "-a ARCHITECTURE\t[Optional] Architecture, default is 'x64'."
    echo -e "-D\tDestroy the docker environment"
    echo -e "-h\tPrint help"
}

# ====
# Main function
# ====
function main() {
    check_project_root_folder $0
    compose_file="build-scripts/${current}/compose.yml"
    compose_cmd="docker compose -f $compose_file"
    REPO_PATH=$(pwd)
    VERSION="$(bash ${REPO_PATH}/build-scripts/product_version.sh)"
    export REPO_PATH
    export VERSION
    export INDEXER_PLUGINS_BRANCH
    export INDEXER_REPORTING_BRANCH
    export REVISION
    export IS_STAGE
    export DISTRIBUTION
    export ARCHITECTURE

    parse_args "${@}"

    if [[ "$DESTROY" == true || "$DESTROY" == "1" ]]; then
        $compose_cmd down -v
        exit 0
    fi

    $compose_cmd up
    #$compose_cmd down -v
}

main "$@"
