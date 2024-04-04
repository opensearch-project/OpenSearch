#!/bin/bash

# Wazuh Docker Copyright (C) 2017, Wazuh Inc. (License GPLv2)
# This has to be exported to make some magic below work.
export DH_OPTIONS

export NAME=wazuh-indexer
export TARGET_DIR=${CURDIR}/debian/${NAME}

# Package build options
export LOG_DIR=/var/log/${NAME}
export LIB_DIR=/var/lib/${NAME}
export PID_DIR=/run/${NAME}
export INDEXER_HOME=/usr/share/${NAME}
export CONFIG_DIR=${INDEXER_HOME}/config
export BASE_DIR=${NAME}-*

rm -rf ${INDEXER_HOME:?}/
tar -xf "${INDEXER_TAR_NAME}"

## TOOLS

## Variables
TOOLS_PATH=${NAME}-${VERSION}/plugins/opensearch-security/tools
CERT_TOOL=${TOOLS_PATH}/wazuh-certs-tool.sh

# generate certificates
cp $CERT_TOOL .
chmod 755 wazuh-certs-tool.sh && bash wazuh-certs-tool.sh -A

# copy to target
mkdir -p ${TARGET_DIR}${INDEXER_HOME}
# mkdir -p ${TARGET_DIR}${INDEXER_HOME}/opensearch-security/ <-- empty dir
mkdir -p ${TARGET_DIR}${CONFIG_DIR}
mkdir -p ${TARGET_DIR}${LIB_DIR}
mkdir -p ${TARGET_DIR}${LOG_DIR}
mkdir -p ${TARGET_DIR}/etc/init.d
mkdir -p ${TARGET_DIR}/etc/default
mkdir -p ${TARGET_DIR}/usr/lib/tmpfiles.d
mkdir -p ${TARGET_DIR}/usr/lib/sysctl.d
mkdir -p ${TARGET_DIR}/usr/lib/systemd/system
mkdir -p ${TARGET_DIR}${CONFIG_DIR}/certs
# Copy installation files to final location
cp -pr ${BASE_DIR}/* ${TARGET_DIR}${INDEXER_HOME}
cp -pr /opensearch.yml ${TARGET_DIR}${CONFIG_DIR}
# Copy Wazuh indexer's certificates
cp -pr /wazuh-certificates/demo.indexer.pem ${TARGET_DIR}${CONFIG_DIR}/certs/indexer.pem
cp -pr /wazuh-certificates/demo.indexer-key.pem ${TARGET_DIR}${CONFIG_DIR}/certs/indexer-key.pem
cp -pr /wazuh-certificates/root-ca.key ${TARGET_DIR}${CONFIG_DIR}/certs/root-ca.key
cp -pr /wazuh-certificates/root-ca.pem ${TARGET_DIR}${CONFIG_DIR}/certs/root-ca.pem
cp -pr /wazuh-certificates/admin.pem ${TARGET_DIR}${CONFIG_DIR}/certs/admin.pem
cp -pr /wazuh-certificates/admin-key.pem ${TARGET_DIR}${CONFIG_DIR}/certs/admin-key.pem

# Set path to indexer home directory
sed -i 's/-Djava.security.policy=file:\/\/\/etc\/wazuh-indexer\/opensearch-performance-analyzer\/opensearch_security.policy/-Djava.security.policy=file:\/\/\/usr\/share\/wazuh-indexer\/opensearch-performance-analyzer\/opensearch_security.policy/g' ${TARGET_DIR}${CONFIG_DIR}/jvm.options

chmod -R 500 ${TARGET_DIR}${CONFIG_DIR}/certs
chmod -R 400 ${TARGET_DIR}${CONFIG_DIR}/certs/*

find ${TARGET_DIR} -type d -exec chmod 750 {} \;
find ${TARGET_DIR} -type f -perm 644 -exec chmod 640 {} \;
find ${TARGET_DIR} -type f -perm 664 -exec chmod 660 {} \;
find ${TARGET_DIR} -type f -perm 755 -exec chmod 750 {} \;
find ${TARGET_DIR} -type f -perm 744 -exec chmod 740 {} \;
