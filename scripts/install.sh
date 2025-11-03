#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

set -ex

function usage() {
    echo "Usage: $0 [args]"
    echo ""
    echo "Arguments:"
    echo -e "-v VERSION\t[Required] OpenSearch version."
    echo -e "-s SNAPSHOT\t[Optional] Build a snapshot, default is 'false'."
    echo -e "-p PLATFORM\t[Optional] Platform, default is 'uname -s'."
    echo -e "-a ARCHITECTURE\t[Optional] Build architecture, default is 'uname -m'."
    echo -e "-d DISTRIBUTION\t[Optional] Distribution, default is 'tar'."
    echo -e "-f ARTIFACTS\t[Optional] Location of build artifacts."
    echo -e "-o OUTPUT\t[Optional] Output path."
    echo -e "-h help"
}

while getopts ":h:v:s:o:p:a:d:f:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        v)
            VERSION=$OPTARG
            ;;
        s)
            SNAPSHOT=$OPTARG
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
        f)
            ARTIFACTS=$OPTARG
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
    echo "Error: missing version."
    usage
    exit 1
fi

if ! command -v yq > /dev/null; then
    echo "Error: yq not found, please install v4 version of yq"
    exit 1
fi

[ -z "$SNAPSHOT" ] && SNAPSHOT="false"
[ -z "$PLATFORM" ] && PLATFORM=$(uname -s | awk '{print tolower($0)}')
[ -z "$ARCHITECTURE" ] && ARCHITECTURE=`uname -m`
[ -z "$DISTRIBUTION" ] && DISTRIBUTION="tar"

# Make sure the cwd is where the script is located
DIR="$(dirname "$0")"
echo $DIR
cd $DIR

## Copy the tar installation script into the bundle
MAJOR_VERSION=`echo $VERSION | cut -d. -f1`
if [ "$DISTRIBUTION" = "tar" ]; then
    cp -v ../../../scripts/startup/tar/linux/opensearch-tar-install.sh "$OUTPUT/"
elif [ "$DISTRIBUTION" = "deb" ] || [ "$DISTRIBUTION" = "rpm" ]; then
    cp -va ../../../scripts/pkg/service_templates/opensearch/* "$OUTPUT/../"
    if [ "$MAJOR_VERSION" = "1" ]; then
        cp -va ../../../scripts/pkg/build_templates/1.x/opensearch/$DISTRIBUTION/* "$OUTPUT/../"
    elif [ "$MAJOR_VERSION" = "2" ]; then
        cp -va ../../../scripts/pkg/build_templates/2.x/opensearch/$DISTRIBUTION/* "$OUTPUT/../"
    else
        cp -va ../../../scripts/pkg/build_templates/current/opensearch/$DISTRIBUTION/* "$OUTPUT/../"
        OS_REF=`yq -e '.components[] | select(.name == "OpenSearch-DataFusion") | .ref' ../../../manifests/$VERSION/opensearch-$VERSION.yml`

        # DEB and RPM has different default env file location
        ENV_FILE_PATH="$OUTPUT/../etc/sysconfig/opensearch"
        if [ "$DISTRIBUTION" = "deb" ]; then
            ENV_FILE_PATH="$OUTPUT/../etc/default/opensearch"
        fi

        curl -SfL "https://raw.githubusercontent.com/opensearch-project/OpenSearch/$OS_REF/distribution/packages/src/common/env/opensearch" -o $ENV_FILE_PATH || { echo "Failed to download env file"; exit 1; }
        curl -SfL "https://raw.githubusercontent.com/opensearch-project/OpenSearch/$OS_REF/distribution/packages/src/common/systemd/opensearch.service" -o "$OUTPUT/../usr/lib/systemd/system/opensearch.service" || { echo "Failed to download env file"; exit 1; }

        # k-NN lib setups
        echo -e "\n\n################################" >> $ENV_FILE_PATH
        echo -e "# Plugin properties" >> $ENV_FILE_PATH
        echo -e "################################" >> $ENV_FILE_PATH
        echo -e "\n# k-NN Lib Path" >> $ENV_FILE_PATH
        echo "LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/share/opensearch/plugins/opensearch-knn/lib" >> $ENV_FILE_PATH
    fi
elif [ "$DISTRIBUTION" = "zip" ] && [ "$PLATFORM" = "windows" ]; then
    cp -v ../../../scripts/startup/zip/windows/opensearch-windows-install.bat "$OUTPUT/"
fi
