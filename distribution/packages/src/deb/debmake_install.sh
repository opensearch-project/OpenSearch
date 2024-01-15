#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# debmake opensearch install script

set -ex

if [ -z "$1" ]; then
    echo "Missing curdir path"
    exit 1
fi

curdir=$1
product_dir=/usr/share/wazuh-indexer
# config_dir=/etc/wazuh-indexer
data_dir=/var/lib/wazuh-indexer
log_dir=/var/log/wazuh-indexer
pid_dir=/var/run/wazuh-indexer
buildroot=${curdir}/debian/wazuh-indexer

# Create necessary directories
mkdir -p "${buildroot}"
mkdir -p "${buildroot}${pid_dir}"
mkdir -p "${buildroot}${product_dir}/plugins"

# Install directories/files
cp -a "${curdir}"/etc "${curdir}"/usr "${curdir}"/var "${buildroot}"/
chmod -c 0755 "${buildroot}${product_dir}"/bin/*
if [ -d "${buildroot}${product_dir}"/plugins/opensearch-security ]; then
    chmod -c 0755 "${buildroot}${product_dir}"/plugins/opensearch-security/tools/*
fi

# Change Permissions
chmod -Rf a+rX,u+w,g-w,o-w "${buildroot}"/*

exit 0
