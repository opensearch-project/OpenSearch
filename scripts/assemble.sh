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

if ( $TEST )
then
		plugins=(
		    "performance-analyzer"
		    "opensearch-security"
		)
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
		    "performance-analyzer" # "opensearch-performance-analyzer"
		    "opensearch-reports-scheduler"
		    "opensearch-security"
		    "opensearch-security-analytics"
		    "opensearch-sql-plugin" # "opensearch-sql"
		)
fi

# ====
# Usage
# ====
function usage() {
    echo "Usage: $0 [args]"
    echo ""
    echo "Arguments:"
    echo -e "-v VERSION\t[Required] OpenSearch version."
    echo -e "-p PLATFORM\t[Optional] Platform, default is 'uname -s'."
    echo -e "-a ARCHITECTURE\t[Optional] Build architecture, default is 'uname -m'."
    echo -e "-d DISTRIBUTION\t[Optional] Distribution, default is 'tar'."
    echo -e "-o OUTPUT\t[Optional] Output path, default is 'artifacts'."
    echo -e "-h help"
}

# ====
# Parse arguments
# ====
function parse_args() {

    while getopts ":h:v:o:p:a:d:" arg; do
        case $arg in
        h)
            usage
            exit 1
            ;;
        v)
            VERSION=$OPTARG
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

    if [ -z "$VERSION" ]; then
        echo "Error: You must specify the OpenSearch version"
        usage
        exit 1
    fi

    [ -z "$OUTPUT" ] && OUTPUT=artifacts

    # Assemble distribution artifact
    # see https://github.com/opensearch-project/OpenSearch/blob/main/settings.gradle#L34 for other distribution targets

    [ -z "$PLATFORM" ] && PLATFORM=$(uname -s | awk '{print tolower($0)}')
    [ -z "$ARCHITECTURE" ] && ARCHITECTURE=$(uname -m)
    [ -z "$DISTRIBUTION" ] && DISTRIBUTION="tar"

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
    # swap configuration files
    cp $PATH_CONF/security/* $PATH_CONF/opensearch-security/
    cp $PATH_CONF/jvm.prod.options $PATH_CONF/jvm.options
    cp $PATH_CONF/opensearch.prod.yml $PATH_CONF/opensearch.yml

    rm -r $PATH_CONF/security
    rm $PATH_CONF/jvm.prod.options $PATH_CONF/opensearch.prod.yml

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

    curl -sL "${download_url}/config.yml" -o $PATH_PLUGINS/opensearch-security/tools/config.yml
    curl -sL "${download_url}/wazuh-passwords-tool.sh" -o $PATH_PLUGINS/opensearch-security/tools/wazuh-passwords-tool.sh
    curl -sL "${download_url}/wazuh-certs-tool.sh" -o $PATH_PLUGINS/opensearch-security/tools/wazuh-certs-tool.sh
}

# ====
# Copy performance analyzer service file
# ====
function enable_performance_analyzer() {
    mkdir -p "${TMP_DIR}"/usr/lib/systemd/system
    cp "distribution/packages/src/common/wazuh-indexer-performance-analyzer.service" "${TMP_DIR}"/usr/lib/systemd/system
}

# ====
# Move performance-analyzer-rca to its final location
# ====
function enable_performance_analyzer_rca() {
    local rca_src="${1}/plugins/opensearch-performance-analyzer/performance-analyzer-rca"
    local rca_dest="${1}"
    mv "${rca_src}" "${rca_dest}"
}

# ====
# Install plugins
# ====
function install_plugins() {
    # Install plugins from Maven repository
    echo "Install plugins"
    for plugin in "${plugins[@]}"; do
        plugin_from_maven="org.opensearch.plugin:${plugin}:$VERSION.0"
        OPENSEARCH_PATH_CONF=$PATH_CONF "${PATH_BIN}/opensearch-plugin" install --batch --verbose "${plugin_from_maven}"
    done
}

# ====
# Clean
# ====
function clean() {
    echo "Cleaning temporary ${TMP_DIR} folder"
    rm -r "${OUTPUT}/tmp"
    echo "After execution, shell path is $(pwd)"
    # Store package's name to file. Used by GH Action.
    echo "${ARTIFACT_PACKAGE_NAME}" >"${OUTPUT}/artifact_name.txt"
}

# ====
# Tar assemble
# ====
function assemble_tar() {
    cd "${TMP_DIR}"
    PATH_CONF="./config"
    PATH_BIN="./bin"
    PATH_PLUGINS="./plugins"

    # Extract
    echo "Extract ${ARTIFACT_BUILD_NAME} archive"
    tar -zvxf "${ARTIFACT_BUILD_NAME}"
    cd "$(ls -d wazuh-indexer-*/)"

    local version
    version=$(cat VERSION)

    # Install plugins
    install_plugins
    # Swap configuration files
    add_configuration_files
    remove_unneeded_files
    add_wazuh_tools "${version}"

    # Pack
    archive_name="wazuh-indexer-${version}"
    cd ..
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
    # Copy performance analyzer service file
    enable_performance_analyzer

    cd "${TMP_DIR}"
    local src_path="./usr/share/wazuh-indexer"
    PATH_CONF="./etc/wazuh-indexer"
    PATH_BIN="${src_path}/bin"
    PATH_PLUGINS="${src_path}/plugins"

    # Extract min-package. Creates usr/, etc/ and var/ in the current directory
    echo "Extract ${ARTIFACT_BUILD_NAME} archive"
    rpm2cpio "${ARTIFACT_BUILD_NAME}" | cpio -imdv

    local version
    version=$(cat ./usr/share/wazuh-indexer/VERSION)

    # Install plugins
    install_plugins
    enable_performance_analyzer_rca ${src_path}
    # Swap configuration files
    add_configuration_files
    remove_unneeded_files
    add_wazuh_tools "${version}"

    # Generate final package
    local topdir
    local spec_file="wazuh-indexer.rpm.spec"
    topdir=$(pwd)
    rpmbuild --bb \
        --define "_topdir ${topdir}" \
        --define "_version ${version}" \
        --define "_architecture ${SUFFIX}" \
        ${spec_file}

    # Move to the root folder, copy the package and clean.
    cd ../../..
    package_name="wazuh-indexer-${version}-1.${SUFFIX}.${EXT}"
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
    chmod a+x "${TMP_DIR}/debmake_install.sh"
    # Copy performance analyzer service file
    enable_performance_analyzer

    cd "${TMP_DIR}"
    local src_path="./usr/share/wazuh-indexer"
    PATH_CONF="./etc/wazuh-indexer"
    PATH_BIN="${src_path}/bin"
    PATH_PLUGINS="${src_path}/plugins"

    # Extract min-package. Creates usr/, etc/ and var/ in the current directory
    echo "Extract ${ARTIFACT_BUILD_NAME} archive"
    ar xf "${ARTIFACT_BUILD_NAME}" data.tar.gz
    tar zvxf data.tar.gz

    local version
    version=$(cat ./usr/share/wazuh-indexer/VERSION)

    # Install plugins
    install_plugins
    enable_performance_analyzer_rca ${src_path}
    # Swap configuration files
    add_configuration_files
    remove_unneeded_files
    add_wazuh_tools "${version}"

    # Generate final package
    debmake \
        --fullname "Wazuh Team" \
        --email "hello@wazuh.com" \
        --invoke debuild \
        --package wazuh-indexer \
        --native \
        --revision 1 \
        --upstreamversion "${version}"

    # Move to the root folder, copy the package and clean.
    cd ../../..
    package_name="wazuh-indexer_${version}_${SUFFIX}.${EXT}"
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

    ARTIFACT_BUILD_NAME=$(ls "${OUTPUT}/dist/" | grep "wazuh-indexer-min_.*$SUFFIX.*\.$EXT")

    ARTIFACT_PACKAGE_NAME=${ARTIFACT_BUILD_NAME/min_/}

    # Create temporal directory and copy the min package there for extraction
    TMP_DIR="${OUTPUT}/tmp/${TARGET}"
    mkdir -p "$TMP_DIR"
    cp "${OUTPUT}/dist/$ARTIFACT_BUILD_NAME" "${TMP_DIR}"

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
}

main "${@}"
