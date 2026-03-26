#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -ex

# Set default values for environment variables
INDEXER_PLUGINS_BRANCH=${INDEXER_PLUGINS_BRANCH:-main}
INDEXER_REPORTING_BRANCH=${INDEXER_REPORTING_BRANCH:-main}
SECURITY_ANALYTICS_BRANCH=${SECURITY_ANALYTICS_BRANCH:-main}
NOTIFICATIONS_BRANCH=${NOTIFICATIONS_BRANCH:-main}
COMMON_UTILS_BRANCH=${COMMON_UTILS_BRANCH:-main}
ENGINE_TARBALL=${ENGINE_TARBALL:-}
REVISION=${REVISION:-0}
IS_STAGE=${IS_STAGE:-false}
DISTRIBUTION=${DISTRIBUTION:-rpm}
ARCHITECTURE=${ARCHITECTURE:-x64}

PLUGINS_REPO_DIR="/repositories/wazuh-indexer-plugins"
REPORTING_REPO_DIR="/repositories/wazuh-indexer-reporting"
SECURITY_ANALYTICS_REPO_DIR="/repositories/wazuh-indexer-security-analytics"
NOTIFICATIONS_REPO_DIR="/repositories/wazuh-indexer-notifications"
COMMON_UTILS_REPO_DIR="/repositories/wazuh-indexer-common-utils"

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

    if [ -d "$SECURITY_ANALYTICS_REPO_DIR/.git" ]; then
        git -C "$SECURITY_ANALYTICS_REPO_DIR" checkout "$SECURITY_ANALYTICS_BRANCH"
    else
        git clone --branch "$SECURITY_ANALYTICS_BRANCH" https://github.com/wazuh/wazuh-indexer-security-analytics --depth 1 "$SECURITY_ANALYTICS_REPO_DIR"
    fi

    if [ -d "$COMMON_UTILS_REPO_DIR/.git" ]; then
        git -C "$COMMON_UTILS_REPO_DIR" checkout "$COMMON_UTILS_BRANCH"
    else
        git clone --branch "$COMMON_UTILS_BRANCH" https://github.com/wazuh/wazuh-indexer-common-utils --depth 1 "$COMMON_UTILS_REPO_DIR"
    fi

    if [ -d "$NOTIFICATIONS_REPO_DIR/.git" ]; then
        git -C "$NOTIFICATIONS_REPO_DIR" checkout "$NOTIFICATIONS_BRANCH"
    else
        git clone --branch "$NOTIFICATIONS_BRANCH" https://github.com/wazuh/wazuh-indexer-notifications --depth 1 "$NOTIFICATIONS_REPO_DIR"
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
    cd ${PLUGINS_REPO_DIR}/plugins/content-manager
    echo "Building content-manager plugin..."
    ./gradlew build -Dversion="$version" -Drevision="$revision" --no-daemon -x check
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

# Function to build wazuh-indexer-security-analytics
build_security_analytics() {
    echo "----------------------------------------"
    echo "Building Security Analytics"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${SECURITY_ANALYTICS_REPO_DIR}

    echo "Building security analytics commons..."
    ./gradlew wazuh-indexer-security-analytics-commons:build -Dversion="$version" -Drevision="$revision" --no-daemon -x check
    ./gradlew wazuh-indexer-security-analytics-commons:publishToMavenLocal -x check

    echo "Building security analytics..."
    ./gradlew build -Dversion="$version" -Drevision="$revision" --no-daemon -x check
    ./gradlew publishToMavenLocal -x check
}

# Invoke the script to download the snapshots
download_snapshots() {
    echo "----------------------------------------"
    echo "Downloading Snapshots"
    echo "----------------------------------------"

    bash ~/build-scripts/download_snapshots.sh \
        --env "https://cti.pre.cloud.wazuh.com/api/v1/" \
        --output-dir ~/artifacts/snapshots
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
    ./gradlew build -Dversion="$version" -Drevision="$revision" --no-daemon
    ./gradlew publishToMavenLocal -x check --no-daemon
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
    ./gradlew build -Dversion="$version" -Dbuild.revision="$revision" --no-daemon -x check
}

# Function to publish SA commons to local Maven (required by security-analytics)
publish_sa_commons() {
    echo "----------------------------------------"
    echo "Publishing Security Analytics Commons to Local Maven"
    echo "----------------------------------------"
    local version="$1"
    local revision="$2"
    cd ${SECURITY_ANALYTICS_REPO_DIR}/commons
    echo "Publishing security-analytics-commons..."
    ../gradlew publishToMavenLocal -Dversion="$version" -Drevision="$revision" --no-daemon
}

# Function to publish security-analytics to local Maven (required by content-manager)
publish_security_analytics() {
    echo "----------------------------------------"
    echo "Publishing Security Analytics to Local Maven"
    echo "----------------------------------------"
    cd ${SECURITY_ANALYTICS_REPO_DIR}
    echo "Publishing security-analytics..."
    ./gradlew publishToMavenLocal -x check --no-daemon
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
    echo "Copying content-manager plugin..."
    cp ${PLUGINS_REPO_DIR}/plugins/content-manager/build/distributions/wazuh-indexer-content-manager-"$version"."$revision".zip ~/artifacts/plugins
    echo "Copying reporting..."
    cp ${REPORTING_REPO_DIR}/build/distributions/wazuh-indexer-reports-scheduler-"$version"."$revision".zip ~/artifacts/plugins
    echo "Copying security analytics..."
    cp ${SECURITY_ANALYTICS_REPO_DIR}/build/distributions/wazuh-indexer-security-analytics-"$version"."$revision".zip ~/artifacts/plugins
    echo "Copying notifications..."
    cp ${NOTIFICATIONS_REPO_DIR}/notifications/notifications/build/distributions/wazuh-indexer-notifications-"$version"."$revision".zip ~/artifacts/plugins
    echo "Copying notifications-core..."
    cp ${NOTIFICATIONS_REPO_DIR}/notifications/core/build/distributions/wazuh-indexer-notifications-core-"$version"."$revision".zip ~/artifacts/plugins
    echo "Copying common-utils..."
    # common-utils is a library (JAR via shadowJar), not an OpenSearch plugin (ZIP).
    # Copy whatever distributable artifacts exist (zip or jar).
    find ${COMMON_UTILS_REPO_DIR}/build -type f \( -name "*.zip" -o -name "*.jar" \) \
        \( -path "*/distributions/*" -o -path "*/libs/*" \) \
        ! -path "*/agent/*" \
        -exec cp -v {} ~/artifacts/plugins/ \; || true
}

# Function to set up the Wazuh Engine tarball
setup_engine_tarball() {
    if [ -n "$ENGINE_TARBALL" ] && [ -f /tmp/engine-tarball.tar.gz ]; then
        echo "----------------------------------------"
        echo "Setting up Wazuh Engine tarball"
        echo "----------------------------------------"
        local tarball_name
        tarball_name=$(basename "$ENGINE_TARBALL")
        mkdir -p ~/artifacts/engine
        cp /tmp/engine-tarball.tar.gz ~/artifacts/engine/"$tarball_name"
        echo "Engine tarball copied to artifacts/engine/$tarball_name"
    else
        echo "WARNING: No engine tarball provided. Packaging may fail."
    fi
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
    local security_analytics_hash
    local package_min_name
    # local package_name

    plugins_hash=$(cd ${PLUGINS_REPO_DIR} && git rev-parse --short HEAD)
    reporting_hash=$(cd ${REPORTING_REPO_DIR} && git rev-parse --short HEAD)
    security_analytics_hash=$(cd ${SECURITY_ANALYTICS_REPO_DIR} && git rev-parse --short HEAD)
    notifications_hash=$(cd ${NOTIFICATIONS_REPO_DIR} && git rev-parse --short HEAD)

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
        "$(if [ "$is_stage" = "true" ]; then echo "-x"; fi)")

    # echo "Creating package name..."
    # package_name=$(bash build-scripts/baptizer.sh \
    #     -a "$architecture" \
    #     -d "$distribution" \
    #     -r "$revision" \
    #     -l "$plugins_hash" \
    #     -e "$reporting_hash" \
    #     -s "$security_analytics_hash" \
    #     -n "$notifications_hash" \
    #     "$(if [ "$is_stage" = "true" ]; then echo "-x"; fi)")

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
        -n "$notifications_hash"

}

# Main script execution
main() {
    echo "---------Starting Build Process---------"
    clone_repositories
    # Set version env var
    VERSION=$(bash ~/build-scripts/product_version.sh)
    # Build and assemble the package
    download_snapshots
    build_common_utils "$VERSION" "$REVISION"
    publish_sa_commons "$VERSION" "$REVISION"
    build_security_analytics "$VERSION" "$REVISION"
    publish_security_analytics
    build_notifications "$VERSION" "$REVISION"
    build_plugins "$VERSION" "$REVISION"
    build_reporting "$VERSION" "$REVISION"
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
