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

name="wazuh-indexer"

product_dir="/usr/share/${name}"
config_dir="/etc/${name}"
# data_dir="/var/lib/${name}"
# log_dir="/var/log/${name}"
pid_dir="/run/${name}"
service_dir="/usr/lib/systemd/system"

buildroot="${curdir}/debian/${name}"

# Create necessary directories
mkdir -p "${buildroot}"
mkdir -p "${buildroot}${pid_dir}"
mkdir -p "${buildroot}${product_dir}/plugins"

# Install directories/files
cp -a "${curdir}"/etc "${curdir}"/usr "${curdir}"/var "${buildroot}"/

# General permissions for most of the package's files:
find "${buildroot}" -type d -exec chmod 750 {} \;
find "${buildroot}" -type f -exec chmod 640 {} \;

# Permissions for the Systemd files
systemd_files=()
systemd_files+=("${buildroot}/${service_dir}/${name}.service")
systemd_files+=("${buildroot}/${service_dir}/${name}-performance-analyzer.service")
systemd_files+=("${buildroot}/${service_dir}/${name}-performance-analyzer.service")
systemd_files+=("${buildroot}/etc/init.d/${name}")
systemd_files+=("${buildroot}/usr/lib/sysctl.d/${name}.conf")
systemd_files+=("${buildroot}/usr/lib/tmpfiles.d/${name}.conf")

for i in "${systemd_files[@]}"; do
	chmod -c 0644 "$i"
done

# Permissions for config files
config_files=()
config_files+=("${buildroot}/${config_dir}/log4j2.properties")
config_files+=("${buildroot}/${config_dir}/jvm.options")
config_files+=("${buildroot}/${config_dir}/opensearch.yml")

for i in "${config_files[@]}"; do
	chmod -c 0660 "$i"
done

# Plugin-related files
if [ -e "${buildroot}/${config_dir}/opensearch-observability/observability.yml" ]; then
	chmod -c 660 "${buildroot}/${config_dir}/opensearch-observability/observability.yml"
fi

if [ -e "${buildroot}/${config_dir}/opensearch-reports-scheduler/reports-scheduler.yml" ]; then
	chmod -c 660 "${buildroot}/${config_dir}/opensearch-reports-scheduler/reports-scheduler.yml"
fi

# Files that need other permissions
chmod -c 440 "${buildroot}${product_dir}/VERSION"
if [ -d "${buildroot}${product_dir}/plugins/opensearch-security" ]; then
	chmod -c 0740 "${buildroot}${product_dir}"/plugins/opensearch-security/tools/*.sh
fi

binary_files=()
binary_files+=("${buildroot}${product_dir}"/bin/*)
binary_files+=("${buildroot}${product_dir}"/jdk/bin/*)
binary_files+=("${buildroot}${product_dir}"/jdk/lib/jspawnhelper)
binary_files+=("${buildroot}${product_dir}"/jdk/lib/modules)
binary_files+=("${buildroot}${product_dir}"/performance-analyzer-rca/bin/*)

for i in "${binary_files[@]}"; do
	chmod -c 750 "$i"
done

chmod -c 660 "${buildroot}${config_dir}/wazuh-template.json"

exit 0
