#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Set default values for environment variables
INDEXER_PLUGINS_BRANCH=${INDEXER_PLUGINS_BRANCH:-main}
INDEXER_REPORTING_BRANCH=${INDEXER_REPORTING_BRANCH:-main}
REVISION=${REVISION:-0}
IS_STAGE=${IS_STAGE:-false}
DISTRIBUTION=${DISTRIBUTION:-rpm}
ARCHITECTURE=${ARCHITECTURE:-x64}

PLUGINS_REPO_DIR="/repositories/wazuh-indexer-plugins"
REPORTING_REPO_DIR="/repositories/wazuh-indexer-reporting"

# Function to clone repositories
clone_repositories() {
    echo "----------------------------------------"
    echo "Cloning Repositories"
    echo "----------------------------------------"

    if [ -d "$PLUGINS_REPO_DIR/.git" ]; then
        git -C "$PLUGINS_REPO_DIR" checkout "$INDEXER_PLUGINS_BRANCH"
    else
        git clone --branch "$INDEXER_PLUGINS_BRANCH" https://github.com/wazuh/wazuh-indexer-plugins --depth 1 "$PLUGINS_REPO_DIR"
    fi

    if [ -d "$REPORTING_REPO_DIR/.git" ]; then
        git -C "$REPORTING_REPO_DIR" checkout "$INDEXER_REPORTING_BRANCH"
    else
        git clone --branch "$INDEXER_REPORTING_BRANCH" https://github.com/wazuh/wazuh-indexer-reporting --depth 1 "$REPORTING_REPO_DIR"
    fi
}

# Function to build wazuh-indexer-plugins
build_plugins() {
    echo "----------------------------------------"
    echo "Building Plugins"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${PLUGINS_REPO_DIR}/plugins/setup
    echo "Building setup plugin..."
    ./gradlew build -Dversion="$version" -Drevision="$revision" --no-daemon
}

# Function to build wazuh-indexer-reporting
build_reporting() {
    echo "----------------------------------------"
    echo "Building Reporting"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${REPORTING_REPO_DIR}
    echo "Building reporting..."
    ./gradlew build -Dversion="$version" -Drevision="$revision" --no-daemon
}

# Function to copy builds
copy_builds() {
    echo "----------------------------------------"
    echo "Copying Builds"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    mkdir -p ~/artifacts/plugins
    echo "Copying setup plugin..."
    cp ${PLUGINS_REPO_DIR}/plugins/setup/build/distributions/wazuh-indexer-setup-"$version"."$revision".zip ~/artifacts/plugins
    echo "Copying reporting..."
    cp ${REPORTING_REPO_DIR}/build/distributions/wazuh-indexer-reports-scheduler-"$version"."$revision".zip ~/artifacts/plugins
}

# Function for packaging process
package_artifacts() {
    echo "----------------------------------------"
    echo "Packaging Artifacts"
    echo "----------------------------------------"
    local architecture="$1"
    local distribution="$2"
    local revision="$3"
    local is_stage="$4"

    local plugins_hash
    local reporting_hash
    local package_min_name
    local package_name

    plugins_hash=$(cd ${PLUGINS_REPO_DIR} && git rev-parse --short HEAD)

    reporting_hash=$(cd ${REPORTING_REPO_DIR} && git rev-parse --short HEAD)

    cd ~

    echo "Creating package minimum name..."
    package_min_name=$(bash build-scripts/baptizer.sh -m \
        -a "$architecture" \
        -d "$distribution" \
        -r "$revision" \
        -l "$plugins_hash" \
        -e "$reporting_hash" \
        "$(if [ "$is_stage" = "true" ]; then echo "-x"; fi)")

    echo "Creating package name..."
    package_name=$(bash build-scripts/baptizer.sh \
        -a "$architecture" \
        -d "$distribution" \
        -r "$revision" \
        -l "$plugins_hash" \
        -e "$reporting_hash" \
        "$(if [ "$is_stage" = "true" ]; then echo "-x"; fi)")

    echo "Building package..."
    bash build-scripts/build.sh -a "$architecture" -d "$distribution" -n "$package_min_name"
    echo "Assembling package..."
    bash build-scripts/assemble.sh \
        -a "$architecture" \
        -d "$distribution" \
        -r "$revision" \
        -l "$plugins_hash" \
        -e "$reporting_hash" \

}

# Main script execution
main() {
    echo "---------Starting Build Process---------"
    clone_repositories
    # Set version env var
    VERSION=$(bash ~/build-scripts/product_version.sh)
    # Build and assemble the package
    build_plugins "$VERSION" "$REVISION"
    build_reporting "$VERSION" "$REVISION"
    copy_builds "$VERSION" "$REVISION"
    package_artifacts "$ARCHITECTURE" "$DISTRIBUTION" "$REVISION" "$IS_STAGE"

    # Clean the environment
    echo "----------------------------------------"
    echo "Build and Packaging Process Completed Successfully!"
    echo "----------------------------------------"
}

# Execute the main function
main
