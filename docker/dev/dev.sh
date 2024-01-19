#!/bin/bash

# Attaches the project as a volume to a JDK 17 container
# Requires Docker
# Script usage: bash ./dev.sh

set -e

# ====
# Checks that the script is run from the intended location
# ====
function check_project_root_folder() {
    current=$(basename "$(pwd)")

    if [[ "$0" != "./dev.sh" && "$0" != "dev.sh" ]]; then
        echo "Run the script from its location"
        usage
        exit 1
    fi
    # Change working directory to the root of the repository
    cd ../..
}

# ====
# Displays usage
# ====
function usage() {
    echo "Usage: ./dev.sh {up|down|stop}"
}

# ====
# Main function
# ====
function main() {
    check_project_root_folder "$@"
    compose_file="docker/${current}/dev.yml"
    compose_cmd="docker compose -f $compose_file"
    REPO_PATH=$(pwd)
    VERSION=$(cat VERSION)
    export REPO_PATH
    export VERSION

    case $1 in
    up)
        $compose_cmd up -d
        ;;
    down)
        $compose_cmd down
        ;;
    stop)
        $compose_cmd stop
        ;;
    *)
        usage
        exit 1
        ;;
    esac
}

main "$@"
