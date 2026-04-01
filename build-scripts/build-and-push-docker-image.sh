#!/bin/bash

# This script builds and pushes wazuh-indexer docker images to the quay.io registry.
# The Docker image is built from a wazuh-indexer tarball (tar.gz), which must be
# named wazuh-indexer-<arch>.tar.gz (e.g. wazuh-indexer-amd64.tar.gz) and placed
# in the wazuh-indexer/build-scripts/docker directory.
# For additional information, read this document:
#   - wazuh-indexer/build-scripts/README.md
#
# To push images, credentials must be set at environment level:
#  - QUAY_USERNAME
#  - QUAY_TOKEN
#
# Usage: build-scripts/build-and-push-docker-image.sh [args]

# Arguments:
# -a ARCHITECTURE [Optional] Target architecture (amd64 or arm64), default is host arch.
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
    echo -e "-a ARCHITECTURE\t[Optional] Target architecture (amd64 or arm64), default is host arch."
    echo -e "-r REVISION    \t[Optional] Revision qualifier, default is 0."
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

    while getopts ":a:r:h" arg; do
        case $arg in
        h)
            usage
            exit 1
            ;;
        a)
            ARCHITECTURE=$OPTARG
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

    # Default to host architecture mapped to Docker naming
    if [ -z "$ARCHITECTURE" ]; then
        case "$(uname -m)" in
            x86_64)  ARCHITECTURE="amd64" ;;
            aarch64) ARCHITECTURE="arm64" ;;
            arm64)   ARCHITECTURE="arm64" ;;
            *)
                echo "Unsupported host architecture: $(uname -m)"
                exit 1
                ;;
        esac
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
    local tarball="${dockerfile_path}/wazuh-indexer-${ARCHITECTURE}.tar.gz"
    if [ ! -f "$tarball" ]; then
        echo "Error: tarball not found at ${tarball}"
        echo "The tarball must be named wazuh-indexer-${ARCHITECTURE}.tar.gz"
        exit 1
    fi

    cd ${dockerfile_path}
    docker build \
        --build-arg="VERSION=${VERSION}" \
        --tag="${DOCKER_REPOSITORY}:${VERSION}-${REVISION}" \
        --progress=plain --no-cache .

    # Push the Docker image.
    docker push "${DOCKER_REPOSITORY}:${VERSION}-${REVISION}"
}

main "${@}"
