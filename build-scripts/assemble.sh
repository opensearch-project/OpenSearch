#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

set -ex

### The $TEST variable determines whether we include a minimalistic
### or the full set of OpenSearch plugins

TEST=${TEST:-false}

if ($TEST); then
    plugins=(
        "opensearch-security"
    )
    wazuh_plugins=()
else
    plugins=(
        "alerting" # "opensearch-alerting"
        "opensearch-job-scheduler"
        "opensearch-anomaly-detection" # Requires "opensearch-job-scheduler"
        "asynchronous-search"          # "opensearch-asynchronous-search"
        "opensearch-cross-cluster-replication"
        "geospatial" # "opensearch-geospatial"
        "opensearch-index-management"
        "opensearch-knn"
        "opensearch-ml-plugin" # "opensearch-ml"
        "neural-search"        # "opensearch-neural-search"
        "opensearch-notifications-core"
        "notifications" # "opensearch-notifications". Requires "opensearch-notifications-core"
        "opensearch-observability"
        "opensearch-security"
        "opensearch-sql-plugin" # "opensearch-sql"
    )
    wazuh_plugins=(
        "wazuh-indexer-setup"
        "wazuh-indexer-content-manager"
        "wazuh-indexer-reports-scheduler"
        "wazuh-indexer-security-analytics" # Flagged as Trojan by some antivirus software
    )
fi

# ====
# Usage
# ====
function usage() {
    echo "Usage: $0 [args]"
    echo ""
    echo "Arguments:"
    echo -e "-p PLATFORM\t[Optional] Platform, default is 'uname -s'."
    echo -e "-a ARCHITECTURE\t[Optional] Build architecture, default is 'uname -m'."
    echo -e "-d DISTRIBUTION\t[Optional] Distribution, default is 'tar'."
    echo -e "-r REVISION\t[Optional] Package revision, default is '0'."
    echo -e "-l PLUGINS_HASH\t[Optional] wazuh-indexer-plugins commit hash, default is '0'."
    echo -e "-e REPORTING_HASH\t[Optional] wazuh-indexer-reporting commit hash, default is '0'."
    echo -e "-s SECURITY_HASH\t[Optional] wazuh-indexer-security-analytics commit hash, default is '0'."
    echo -e "-o OUTPUT\t[Optional] Output path, default is 'artifacts'."
    echo -e "-h help"
}

# ====
# Parse arguments
# ====
function parse_args() {

    while getopts ":ho:p:a:d:r:l:e:s:" arg; do
        case $arg in
        h)
            usage
            exit 1
            ;;
        o)
            OUTPUT=$OPTARG
            ;;
        p)
            PLATFORM=$OPTARG
            ;;
        a)
            ARCHITECTURE=$OPTARG
            ;;
        d)
            DISTRIBUTION=$OPTARG
            ;;
        r)
            REVISION=$OPTARG
            ;;
        l)
            PLUGINS_HASH=$OPTARG
            ;;
        e)
            REPORTING_HASH=$OPTARG
            ;;
        s)
            SECURITY_HASH=$OPTARG
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

    [ -z "$OUTPUT" ] && OUTPUT=artifacts

    # Assemble distribution artifact
    # see https://github.com/opensearch-project/OpenSearch/blob/main/settings.gradle#L34 for other distribution targets

    [ -z "$PLATFORM" ] && PLATFORM=$(uname -s | awk '{print tolower($0)}')
    [ -z "$ARCHITECTURE" ] && ARCHITECTURE=$(uname -m)
    [ -z "$DISTRIBUTION" ] && DISTRIBUTION="tar"
    [ -z "$REVISION" ] && REVISION="0"
    [ -z "$PLUGINS_HASH" ] && PLUGINS_HASH="0"
    [ -z "$REPORTING_HASH" ] && REPORTING_HASH="0"
    [ -z "$SECURITY_HASH" ] && SECURITY_HASH="0"

    case $PLATFORM-$DISTRIBUTION-$ARCHITECTURE in
    linux-tar-x64 | darwin-tar-x64)
        PACKAGE="tar"
        EXT="tar.gz"
        TARGET="$PLATFORM-$PACKAGE"
        SUFFIX="$PLATFORM-x64"
        ;;
    linux-tar-arm64 | darwin-tar-arm64)
        PACKAGE="tar"
        EXT="tar.gz"
        TARGET="$PLATFORM-arm64-$PACKAGE"
        SUFFIX="$PLATFORM-arm64"
        ;;
    linux-deb-x64)
        PACKAGE="deb"
        EXT="deb"
        TARGET="deb"
        SUFFIX="amd64"
        ;;
    linux-deb-arm64)
        PACKAGE="deb"
        EXT="deb"
        TARGET="arm64-deb"
        SUFFIX="arm64"
        ;;
    linux-rpm-x64)
        PACKAGE="rpm"
        EXT="rpm"
        TARGET="rpm"
        SUFFIX="x86_64"
        ;;
    linux-rpm-arm64)
        PACKAGE="rpm"
        EXT="rpm"
        TARGET="arm64-rpm"
        SUFFIX="aarch64"
        ;;
    *)
        echo "Unsupported platform-distribution-architecture combination: $PLATFORM-$DISTRIBUTION-$ARCHITECTURE"
        exit 1
        ;;
    esac
}

# ====
# Set up configuration files
# ====
function add_configuration_files() {
    # Add our settings to the configuration files
    cat "$PATH_CONF/security/roles.wazuh.yml" >>"$PATH_CONF/opensearch-security/roles.yml"
    cat "$PATH_CONF/security/roles_mapping.wazuh.yml" >>"$PATH_CONF/opensearch-security/roles_mapping.yml"
    cat "$PATH_CONF/security/internal_users.wazuh.yml" >>"$PATH_CONF/opensearch-security/internal_users.yml"
    # Disable multi-tenancy
    sed -i 's/#kibana:/kibana:/' "$PATH_CONF/opensearch-security/config.yml"
    sed -i 's/#multitenancy_enabled: true/  multitenancy_enabled: false/' "$PATH_CONF/opensearch-security/config.yml"

    cp "$PATH_CONF/opensearch.prod.yml" "$PATH_CONF/opensearch.yml"

    rm -r "$PATH_CONF/security"
    rm "$PATH_CONF/opensearch.prod.yml"

    # Remove symbolic links and bat files
    find . -type l -exec rm -rf {} \;
    find . -name "*.bat" -exec rm -rf {} \;
}

# ====
# Remove unneeded files
# ====
function remove_unneeded_files() {
    rm "$PATH_PLUGINS/opensearch-security/tools/install_demo_configuration.sh"
}

# ====
# Add additional tools into packages
# ====
function add_wazuh_tools() {
    local version=${1%%.[[:digit:]]}

    local download_url
    download_url="https://packages-dev.wazuh.com/${version}"

    curl -sL "${download_url}/config.yml" -o "$PATH_PLUGINS"/opensearch-security/tools/config.yml
    curl -sL "${download_url}/wazuh-passwords-tool.sh" -o "$PATH_PLUGINS"/opensearch-security/tools/wazuh-passwords-tool.sh
    curl -sL "${download_url}/wazuh-certs-tool.sh" -o "$PATH_PLUGINS"/opensearch-security/tools/wazuh-certs-tool.sh
}

# ====
# Add demo certificates installer
# ====
function add_demo_certs_installer() {
    cp install-demo-certificates.sh "$PATH_PLUGINS"/opensearch-security/tools/
}

# ====
# Install plugins
# ====
function install_plugins() {
    echo "Installing OpenSearch plugins"
    local maven_repo_local="$HOME/.m2"
    for plugin in "${plugins[@]}"; do
        local plugin_from_maven="org.opensearch.plugin:${plugin}:${UPSTREAM_VERSION}.0"
        mvn -Dmaven.repo.local="${maven_repo_local}" org.apache.maven.plugins:maven-dependency-plugin:2.1:get -DrepoUrl=https://repo1.maven.org/maven2 -Dartifact="${plugin_from_maven}:zip"
        OPENSEARCH_PATH_CONF=$PATH_CONF "${PATH_BIN}/opensearch-plugin" install --batch --verbose "file:${maven_repo_local}/org/opensearch/plugin/${plugin}/${UPSTREAM_VERSION}.0/${plugin}-${UPSTREAM_VERSION}.0.zip"
    done

    echo "Installing Wazuh plugins"
    local indexer_plugin_version="${1}.${REVISION}"
    for plugin_name in "${wazuh_plugins[@]}"; do
        # Check if the plugin is in the local maven repository. This is usually
        # case for local executions.
        local plugin_path="${maven_repo_local}/repository/org/wazuh/${plugin_name}-plugin/${indexer_plugin_version}/${plugin_name}-${indexer_plugin_version}.zip"

        # Otherwise, search for the plugins in the output folder.
        if [ -z "${plugin_from_maven_local}" ]; then
            echo "Plugin ${plugin_name} not found in local maven repository. Searching on ./${OUTPUT}/plugins"
            # Working directory at this point is: wazuh-indexer/artifacts/tmp/{rpm|deb|tar}
            plugin_path="$(pwd)/../../plugins/${plugin_name}-${indexer_plugin_version}.zip"
        fi

        OPENSEARCH_PATH_CONF=$PATH_CONF "${PATH_BIN}/opensearch-plugin" install --batch --verbose "file:${plugin_path}"
    done
}

# ====
# Clean
# ====
function clean() {
    echo "Cleaning temporary ${TMP_DIR} folder"
    rm -r "${OUTPUT}/tmp"
    echo "After execution, shell path is $(pwd)"
}

# ====
# Add commit to VERSION.json
# ====
function generate_installer_version_file() {
    local dir
    dir="${1}"
    jq \
      --arg commit "${INDEXER_HASH}-${PLUGINS_HASH}-${REPORTING_HASH}-${SECURITY_HASH}" \
      '. + {"commit": $commit}' \
      "${REPO_PATH}"/VERSION.json > "${dir}"/VERSION.json
}

# ====
# Tar assemble
# ====
function assemble_tar() {
    cd "${TMP_DIR}"

    # Extract
    echo "Extract ${ARTIFACT_BUILD_NAME} archive"
    tar -zvxf "${ARTIFACT_BUILD_NAME}"
    local decompressed_tar_dir
    decompressed_tar_dir=$(ls -d wazuh-indexer-*/)

    generate_installer_version_file "${decompressed_tar_dir}"

    PATH_CONF="${decompressed_tar_dir}/config"
    PATH_BIN="${decompressed_tar_dir}/bin"
    PATH_PLUGINS="${decompressed_tar_dir}/plugins"

    # Install plugins
    install_plugins "${PRODUCT_VERSION}"
    add_demo_certs_installer
    # Swap configuration files
    add_configuration_files
    remove_unneeded_files
    add_wazuh_tools "${PRODUCT_VERSION}"

    # Pack
    archive_name="wazuh-indexer-${PRODUCT_VERSION}"
    tar -cvf "${archive_name}-${SUFFIX}.${EXT}" "${archive_name}"
    cd ../../..
    cp "${TMP_DIR}/${archive_name}-${SUFFIX}.${EXT}" "${OUTPUT}/dist/$ARTIFACT_PACKAGE_NAME"

    clean
}

# ====
# RPM assemble
# ====
function assemble_rpm() {
    # Copy spec
    cp "distribution/packages/src/rpm/wazuh-indexer.rpm.spec" "${TMP_DIR}"

    cd "${TMP_DIR}"
    local src_path="./usr/share/wazuh-indexer"
    PATH_CONF="./etc/wazuh-indexer"
    PATH_BIN="${src_path}/bin"
    PATH_PLUGINS="${src_path}/plugins"

    # Extract min-package. Creates usr/, etc/ and var/ in the current directory
    echo "Extract ${ARTIFACT_BUILD_NAME} archive"
    rpm2cpio "${ARTIFACT_BUILD_NAME}" | cpio -imdv

    generate_installer_version_file "${src_path}"

    # Install plugins
    install_plugins "${PRODUCT_VERSION}"
    add_demo_certs_installer
    # Swap configuration files
    add_configuration_files
    remove_unneeded_files
    add_wazuh_tools "${PRODUCT_VERSION}"

    # Generate final package
    local topdir
    local spec_file="wazuh-indexer.rpm.spec"
    topdir=$(pwd)
    rpmbuild --bb \
        --define "_topdir ${topdir}" \
        --define "_version ${PRODUCT_VERSION}" \
        --define "_architecture ${SUFFIX}" \
        --define "_release ${REVISION}" \
        ${spec_file}

    # Move to the root folder, copy the package and clean.
    cd ../../..
    package_name="wazuh-indexer-${PRODUCT_VERSION}-${REVISION}.${SUFFIX}.${EXT}"
    cp "${TMP_DIR}/RPMS/${SUFFIX}/${package_name}" "${OUTPUT}/dist/$ARTIFACT_PACKAGE_NAME"

    clean
}

# ====
# DEB assemble
# ====
function assemble_deb() {
    # Copy spec
    cp "distribution/packages/src/deb/Makefile" "${TMP_DIR}"
    cp "distribution/packages/src/deb/debmake_install.sh" "${TMP_DIR}"
    cp -r "distribution/packages/src/deb/debian" "${TMP_DIR}"
    chmod a+x "${TMP_DIR}/debmake_install.sh"

    cd "${TMP_DIR}"
    local src_path="./usr/share/wazuh-indexer"
    PATH_CONF="./etc/wazuh-indexer"
    PATH_BIN="${src_path}/bin"
    PATH_PLUGINS="${src_path}/plugins"

    # Extract min-package. Creates usr/, etc/ and var/ in the current directory
    echo "Extract ${ARTIFACT_BUILD_NAME} archive"
    ar xf "${ARTIFACT_BUILD_NAME}" data.tar.gz
    tar zvxf data.tar.gz

    generate_installer_version_file "${src_path}"

    # Install plugins
    install_plugins "${PRODUCT_VERSION}"
    add_demo_certs_installer
    # Swap configuration files
    add_configuration_files
    remove_unneeded_files
    add_wazuh_tools "${PRODUCT_VERSION}"

    # Configure debmake to only generate binaries
    echo 'DEBUILD_DPKG_BUILDPACKAGE_OPTS="-us -uc -ui -b"' >~/.devscripts
    # Configure debuild to skip lintian
    echo 'DEBUILD_LINTIAN_OPTS="--no-lintian"' >>~/.devscripts

    # Generate final package
    debmake \
        --fullname "Wazuh Team" \
        --email "hello@wazuh.com" \
        --invoke debuild \
        --package wazuh-indexer \
        --native \
        --revision "${REVISION}" \
        --upstreamversion "${PRODUCT_VERSION}-${REVISION}"

    # Move to the root folder, copy the package and clean.
    cd ../../..
    package_name="wazuh-indexer_${PRODUCT_VERSION}-${REVISION}_${SUFFIX}.${EXT}"
    # debmake creates the package one level above
    cp "${TMP_DIR}/../${package_name}" "${OUTPUT}/dist/$ARTIFACT_PACKAGE_NAME"

    clean
}

# ====
# Main function
# ====
function main() {
    parse_args "${@}"

    echo "Assembling wazuh-indexer for $PLATFORM-$DISTRIBUTION-$ARCHITECTURE"

    REPO_PATH="$(pwd)"

    UPSTREAM_VERSION=$(bash build-scripts/upstream-version.sh)
    PRODUCT_VERSION=$(bash build-scripts/product_version.sh)
    ### Get the commit hash ID
    INDEXER_HASH=$(git rev-parse --short HEAD)
    ARTIFACT_BUILD_NAME=$(ls "${OUTPUT}/dist/" | grep "wazuh-indexer-min.*$SUFFIX.*\.$EXT")
    ARTIFACT_PACKAGE_NAME=${ARTIFACT_BUILD_NAME/-min/}

    # Create temporal directory and copy the min package there for extraction
    TMP_DIR="${OUTPUT}/tmp/${TARGET}"
    mkdir -p "$TMP_DIR"
    cp "${OUTPUT}/dist/$ARTIFACT_BUILD_NAME" "${TMP_DIR}"
    # Copy the demo certificates generator
    cp distribution/packages/src/common/scripts/install-demo-certificates.sh "$TMP_DIR"

    case $PACKAGE in
    tar)
        assemble_tar
        ;;
    rpm)
        assemble_rpm
        ;;
    deb)
        assemble_deb
        ;;
    esac

    # Create checksum
    sha512sum "${OUTPUT}/dist/$ARTIFACT_PACKAGE_NAME" >"${OUTPUT}/dist/$ARTIFACT_PACKAGE_NAME".sha512
}

main "${@}"
