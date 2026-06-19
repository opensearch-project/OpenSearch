#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -ex

# Set default values for environment variables
INDEXER_PLUGINS_BRANCH=${INDEXER_PLUGINS_BRANCH:-main}
INDEXER_REPORTING_BRANCH=${INDEXER_REPORTING_BRANCH:-main}
SECURITY_ANALYTICS_BRANCH=${SECURITY_ANALYTICS_BRANCH:-main}
NOTIFICATIONS_BRANCH=${NOTIFICATIONS_BRANCH:-main}
ALERTING_BRANCH=${ALERTING_BRANCH:-main}
COMMON_UTILS_BRANCH=${COMMON_UTILS_BRANCH:-main}
ENGINE_TARBALL=${ENGINE_TARBALL:-}
REVISION=${REVISION:-0}
IS_STAGE=${IS_STAGE:-false}
DISTRIBUTION=${DISTRIBUTION:-rpm}
ARCHITECTURE=${ARCHITECTURE:-x64}

# Define repositories as an associative array
declare -A REPOS=(
    [PLUGINS]="wazuh-indexer-plugins|$INDEXER_PLUGINS_BRANCH"
    [REPORTING]="wazuh-indexer-reporting|$INDEXER_REPORTING_BRANCH"
    [SECURITY_ANALYTICS]="wazuh-indexer-security-analytics|$SECURITY_ANALYTICS_BRANCH"
    [COMMON_UTILS]="wazuh-indexer-common-utils|$COMMON_UTILS_BRANCH"
    [NOTIFICATIONS]="wazuh-indexer-notifications|$NOTIFICATIONS_BRANCH"
    [ALERTING]="wazuh-indexer-alerting|$ALERTING_BRANCH"
)

# Set repo directory variables
PLUGINS_REPO_DIR="/repositories/wazuh-indexer-plugins"
REPORTING_REPO_DIR="/repositories/wazuh-indexer-reporting"
SECURITY_ANALYTICS_REPO_DIR="/repositories/wazuh-indexer-security-analytics"
NOTIFICATIONS_REPO_DIR="/repositories/wazuh-indexer-notifications"
ALERTING_REPO_DIR="/repositories/wazuh-indexer-alerting"
COMMON_UTILS_REPO_DIR="/repositories/wazuh-indexer-common-utils"

# Function to clone repositories
clone_repositories() {
    echo "----------------------------------------"
    echo "Cloning Repositories"
    echo "----------------------------------------"

    local repo_name 
    local repo_url 
    local branch 
    local repo_dir_var 
    local repo_dir

    for key in "${!REPOS[@]}"; do
        IFS='|' read -r repo_name branch <<< "${REPOS[$key]}"
        repo_dir_var="${key}_REPO_DIR"
        repo_dir="${!repo_dir_var}"
        repo_url="https://github.com/wazuh/$repo_name"

        if [ ! -d "$repo_dir/.git" ]; then
            git clone --depth 1 "$repo_url" "$repo_dir"
        fi
        # Resolve a branch, tag, or commit SHA (volumes persist across runs).
        git -C "$repo_dir" fetch --depth 1 origin "$branch"
        git -C "$repo_dir" checkout --detach FETCH_HEAD
    done
}

# Invoke the script to download the snapshots
download_snapshots() {
    echo "----------------------------------------"
    echo "Downloading Snapshots"
    echo "----------------------------------------"

    bash ~/build-scripts/download_snapshots.sh \
        --env "${CTI_API_URL:-https://api.pre.cloud.wazuh.com/api/v1}" \
        --output-dir ~/artifacts/snapshots
}

# Function to build wazuh-indexer-plugins
build_plugins() {
    echo "----------------------------------------"
    echo "Building Plugins"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${PLUGINS_REPO_DIR}
    echo "Building setup and content-manager plugins..."
    ./gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check
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
    ./gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check
}

# Function to build wazuh-indexer-security-analytics
build_security_analytics() {
    echo "----------------------------------------"
    echo "Building Security Analytics"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${SECURITY_ANALYTICS_REPO_DIR}

    echo "Building security analytics commons..."
    ./gradlew wazuh-indexer-security-analytics-commons:publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check

    echo "Building security analytics..."
    ./gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check
}

# Function to build wazuh-indexer-common-utils
build_common_utils() {
    echo "----------------------------------------"
    echo "Building Common Utils"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${COMMON_UTILS_REPO_DIR}
    echo "Building common-utils..."
    ./gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check
}

# Function to build wazuh-indexer-notifications
build_notifications() {
    echo "----------------------------------------"
    echo "Building Notifications"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${NOTIFICATIONS_REPO_DIR}/notifications
    echo "Building notifications..."
    ./gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check
}

# Function to build wazuh-indexer-alerting
build_alerting() {
    echo "----------------------------------------"
    echo "Building Alerting"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${ALERTING_REPO_DIR}
    echo "Building alerting..."
    ./gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon -x check
}

# Function to copy builds
copy_builds() {
    echo "----------------------------------------"
    echo "Copying Builds"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    local v="${version}.${revision}"
    local m2="/var/m2/com/wazuh"
    mkdir -p ~/artifacts/plugins

    # All plugins are published to Maven local — iterate and copy
    local maven_plugins=(
        "wazuh-indexer-setup"
        "wazuh-indexer-content-manager"
        "wazuh-indexer-reports-scheduler"
        "wazuh-indexer-security-analytics"
        "wazuh-indexer-notifications-core"
        "wazuh-indexer-notifications"
        "wazuh-indexer-alerting"
    )
    for artifact in "${maven_plugins[@]}"; do
        echo "Copying ${artifact}..."
        cp "${m2}/${artifact}/${v}/${artifact}-${v}.zip" ~/artifacts/plugins
    done
}

# Function to set up the Wazuh Engine tarball
setup_engine_tarball() {
    if [ -z "$ENGINE_TARBALL" ] || [ ! -f /tmp/engine-tarball.tar.gz ]; then
        echo "Error: wazuh-engine tarball is required but was not provided or is not accessible at /tmp/engine-tarball.tar.gz."
        exit 1
    fi
    echo "----------------------------------------"
    echo "Setting up Wazuh Engine tarball"
    echo "----------------------------------------"
    local tarball_name dest
    tarball_name=$(basename "$ENGINE_TARBALL")
    dest=~/artifacts/engine/"$tarball_name"
    mkdir -p ~/artifacts/engine
    # Skip the copy if the source and destination are the same file.
    if [ /tmp/engine-tarball.tar.gz -ef "$dest" ]; then
        echo "Engine tarball already in place at artifacts/engine/$tarball_name"
    else
        cp /tmp/engine-tarball.tar.gz "$dest"
        echo "Engine tarball copied to artifacts/engine/$tarball_name"
    fi
}

# Function for packaging process
package_artifacts() {
    echo "----------------------------------------"
    echo "Packaging Artifacts"
    echo "----------------------------------------"
    # Drop packages from previous runs (the bind-mounted artifacts/ persists
    # locally); assemble.sh matches wazuh-indexer-min* by glob and breaks on a
    # stale match.
    rm -rf ~/artifacts/dist ~/artifacts/tmp

    local architecture="$1"
    local distribution="$2"
    local revision="$3"
    local is_stage="$4"

    local plugins_hash
    local reporting_hash
    local security_analytics_hash
    local notifications_hash
    local alerting_hash
    local package_min_name

    plugins_hash=$(cd ${PLUGINS_REPO_DIR} && git rev-parse --short HEAD)
    reporting_hash=$(cd ${REPORTING_REPO_DIR} && git rev-parse --short HEAD)
    security_analytics_hash=$(cd ${SECURITY_ANALYTICS_REPO_DIR} && git rev-parse --short HEAD)
    notifications_hash=$(cd ${NOTIFICATIONS_REPO_DIR} && git rev-parse --short HEAD)
    alerting_hash=$(cd ${ALERTING_REPO_DIR} && git rev-parse --short HEAD)

    cd ~

    echo "Creating package minimum name..."
    package_min_name=$(bash build-scripts/baptizer.sh -m \
        -a "$architecture" \
        -d "$distribution" \
        -r "$revision" \
        -l "$plugins_hash" \
        -e "$reporting_hash" \
        -s "$security_analytics_hash" \
        -n "$notifications_hash" \
        -t "$alerting_hash" \
        "$(if [ "$is_stage" = "true" ]; then echo "-x"; fi)")

    echo "Building package..."
    bash build-scripts/build.sh -a "$architecture" -d "$distribution" -n "$package_min_name"
    echo "Assembling package..."
    bash build-scripts/assemble.sh \
        -a "$architecture" \
        -d "$distribution" \
        -r "$revision" \
        -o "$(pwd)/artifacts" \
        -l "$plugins_hash" \
        -e "$reporting_hash" \
        -s "$security_analytics_hash" \
        -n "$notifications_hash" \
        -t "$alerting_hash"

}

# Main script execution
main() {
    echo "---------Starting Build Process---------"
    clone_repositories
    # Set version env var
    VERSION=$(bash ~/build-scripts/product_version.sh)
    # Build and assemble the package
    download_snapshots
    # Order matters
    build_common_utils "$VERSION" "$REVISION"
    build_alerting "$VERSION" "$REVISION"
    build_security_analytics "$VERSION" "$REVISION"
    build_notifications "$VERSION" "$REVISION"
    build_reporting "$VERSION" "$REVISION"
    build_plugins "$VERSION" "$REVISION"
    
    copy_builds "$VERSION" "$REVISION"
    setup_engine_tarball
    package_artifacts "$ARCHITECTURE" "$DISTRIBUTION" "$REVISION" "$IS_STAGE"

    # Clean the environment
    echo "----------------------------------------"
    echo "Build and Packaging Process Completed Successfully!"
    echo "----------------------------------------"
}

# Execute the main function
main
