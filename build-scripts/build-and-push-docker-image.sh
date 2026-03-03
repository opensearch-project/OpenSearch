#!/bin/bash

# This script builds and pushes wazuh-indexer docker images to the quay.io registry.
# The Docker image is built from a wazuh-indexer tarball (tar.gz), which must be
# present in the same folder as the Dockerfile in wazuh-indexer/build-scripts/docker.
# For addtional information, read this document:
#   - wazuh-indexer/build-scripts/README.md
#
# To push images, credentials must be set at environment level:
#  - QUAY_USERNAME
#  - QUAY_TOKEN
#
# Usage: build-scripts/build-and-push-docker-image.sh [args]

# Arguments:
# -n NAME         [required] Tarball name.
# -r REVISION     [Optional] Revision qualifier, default is 0.
# -h help

set -e

DOCKER_REGISTRY="quay.io"
DOCKER_REPOSITORY="$DOCKER_REGISTRY/wazuh/wazuh-indexer"

# ====
# Usage.
# ====
function usage() {
    echo "Usage: $0 [args]"
    echo ""
    echo "Arguments:"
    echo -e "-n NAME    \t[required] Tarball name."
    echo -e "-r REVISION\t[Optional] Revision qualifier, default is 0."
    echo -e "-h help"
}

# ====
# Exit with failure function.
# ====
function fail() {
    echo "Required environment variable is not set: $1"
    exit 1
}

# ====
# Parse arguments
# ====
function parse_args() {

    while getopts ":n:r:h" arg; do
        case $arg in
        h)
            usage
            exit 1
            ;;
        n)
            TARBALL=$OPTARG
            ;;
        r)
            REVISION=$OPTARG
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

    if [ -z "$TARBALL" ]; then
        echo "Missing required argument 'TARBALL'"
        echo ""
        usage
        exit 1
    fi

    REVISION="${REVISION:-0}"
    VERSION=$(bash build-scripts/product_version.sh)
}

# ====
# Main function
# ====
function main() {
    # Check required environment variables are set. Exit otherwise.
    [[ -z "${QUAY_USERNAME}" ]] && fail "QUAY_USERNAME"
    [[ -z "${QUAY_TOKEN}" ]] && fail "QUAY_TOKEN"

    # Parse args
    parse_args "${@}"

    # Login to the registry.
    docker login -u="${QUAY_USERNAME}" -p="${QUAY_TOKEN}" "${DOCKER_REGISTRY}"

    # Build the Docker image.
    local dockerfile_path="build-scripts/docker"
    cd ${dockerfile_path}
    docker build \
        --build-arg="VERSION=${VERSION}" \
        --build-arg="INDEXER_TAR_NAME=${TARBALL}" \
        --tag="${DOCKER_REPOSITORY}:${VERSION}-${REVISION}" \
        --progress=plain --no-cache .

    # Push the Docker image.
    docker push "${DOCKER_REPOSITORY}:${VERSION}-${REVISION}"
}

main "${@}"
