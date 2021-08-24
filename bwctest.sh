#!/bin/bash

set -e

function usage() {
  echo ""
    echo "This script is used to run Backwards Compatibility tests"
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo "None"
    echo ""
    echo -e "-h\tPrint this message."
    echo "--------------------------------------------------------------------------"
}

while getopts ":h" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        ?)
            echo "Invalid option: -${OPTARG}"
            exit 1
            ;;
    esac
done 

./gradlew bwctest
