#!/bin/bash

# This script downloads and sets up a Wazuh indexer node.
#
# For additional information, please refer to the Wazuh documentation:
#   - https://documentation.wazuh.com/current/installation-guide/wazuh-indexer/index.html
#
# Usage: indexer_node_install.sh [version]
#
# Arguments:
# - version        [required] The version of Wazuh to install (e.g., 4.11.1).
#
# Note: Ensure that you have the necessary permissions to execute this script,
#       and that you have `curl` installed on your system.

if [ -z "$1" ]; then
    echo "Error: Version argument is required."
    exit 1
fi

# Source shared retry utility
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/retry.sh"

VERSION="$1"
MAJOR_VERSION=$(printf '%s' "$VERSION" | sed -E 's/^v?([0-9]+).*/\1/')

if ! [[ "$MAJOR_VERSION" =~ ^[0-9]+$ ]]; then
  echo "Error: Unable to extract major version from '$VERSION'."
  exit 1
fi

VERSION_SERIES="${MAJOR_VERSION}.x"

retry 3 5 curl -sO --connect-timeout 10 --max-time 120 https://packages.wazuh.com/$VERSION_SERIES/wazuh-certs-tool.sh
retry 3 5 curl -sO --connect-timeout 10 --max-time 120 https://packages.wazuh.com/$VERSION_SERIES/config.yml

# =====
# Write to config.yml
# =====

cat << EOF > config.yml
nodes:
  indexer:
    - name: node-1
      ip: "127.0.0.1"
  server:
    - name: wazuh-1
      ip: "127.0.0.1"
  dashboard:
    - name: dashboard
      ip: "127.0.0.1"
EOF


bash ./wazuh-certs-tool.sh -A
tar -cvf ./wazuh-certificates.tar -C ./wazuh-certificates/ .

if command -v apt-get &> /dev/null; then
    retry 3 10 apt-get install -y debconf adduser procps
    retry 3 10 apt-get install -y gnupg apt-transport-https
    retry 3 5 bash -c 'set -o pipefail; curl -fsSL --connect-timeout 10 --max-time 60 https://packages.wazuh.com/key/GPG-KEY-WAZUH | gpg --no-default-keyring --keyring gnupg-ring:/usr/share/keyrings/wazuh.gpg --import && chmod 644 /usr/share/keyrings/wazuh.gpg'
    echo "deb [signed-by=/usr/share/keyrings/wazuh.gpg] https://packages.wazuh.com/${VERSION_SERIES}/apt/ stable main" | tee -a /etc/apt/sources.list.d/wazuh.list
    retry 3 10 apt-get update
    retry 3 10 apt-get -y install wazuh-indexer=$1-1
else
  retry 3 10 yum install coreutils -y

  retry 3 5 rpm --import https://packages.wazuh.com/key/GPG-KEY-WAZUH
  echo -e '[wazuh]\ngpgcheck=1\ngpgkey=https://packages.wazuh.com/key/GPG-KEY-WAZUH\nenabled=1\nname=EL-$releasever - Wazuh\nbaseurl=https://packages.wazuh.com/'"$VERSION_SERIES"'/yum/\nprotect=1' | tee /etc/yum.repos.d/wazuh.repo

  retry 3 10 yum -y install wazuh-indexer-$1-1

fi

# ======
# Write to /etc/wazuh-indexer/opensearch.yml
# ======
cat << EOF > /etc/wazuh-indexer/opensearch.yml
network.host: "127.0.0.1"
node.name: "node-1"
cluster.initial_cluster_manager_nodes:
  - "node-1"
cluster.name: "wazuh-cluster"
discovery.seed_hosts:
  - "127.0.0.1"
node.max_local_storage_nodes: "3"
path.data: /var/lib/wazuh-indexer
path.logs: /var/log/wazuh-indexer

plugins.security.ssl.http.pemcert_filepath: /etc/wazuh-indexer/certs/indexer.pem
plugins.security.ssl.http.pemkey_filepath: /etc/wazuh-indexer/certs/indexer-key.pem
plugins.security.ssl.http.pemtrustedcas_filepath: /etc/wazuh-indexer/certs/root-ca.pem
plugins.security.ssl.transport.pemcert_filepath: /etc/wazuh-indexer/certs/indexer.pem
plugins.security.ssl.transport.pemkey_filepath: /etc/wazuh-indexer/certs/indexer-key.pem
plugins.security.ssl.transport.pemtrustedcas_filepath: /etc/wazuh-indexer/certs/root-ca.pem
plugins.security.ssl.http.enabled: true
plugins.security.ssl.transport.enforce_hostname_verification: false
plugins.security.ssl.transport.resolve_hostname: false

plugins.security.authcz.admin_dn:
- "CN=admin,OU=Wazuh,O=Wazuh,L=California,C=US"
plugins.security.check_snapshot_restore_write_privileges: true
plugins.security.enable_snapshot_restore_privilege: true
plugins.security.nodes_dn:
- "CN=node-1,OU=Wazuh,O=Wazuh,L=California,C=US"
plugins.security.restapi.roles_enabled:
- "all_access"
- "security_rest_api_access"

plugins.security.system_indices.enabled: true
plugins.security.system_indices.indices: [".plugins-ml-model", ".plugins-ml-task", ".opendistro-alerting-config", ".opendistro-alerting-alert*", ".opendistro-anomaly-results*", ".opendistro-anomaly-detector*", ".opendistro-anomaly-checkpoints", ".opendistro-anomaly-detection-state", ".opendistro-reports-*", ".opensearch-notifications-*", ".opensearch-notebooks", ".opensearch-observability", ".opendistro-asynchronous-search-response*", ".replication-metadata-store"]
cluster.default_number_of_replicas: 0
EOF

# =====
# Create the directory for certificates and set permissions
# =====
mkdir -p /etc/wazuh-indexer/certs
tar -xf ./wazuh-certificates.tar -C /etc/wazuh-indexer/certs/ ./node-1.pem ./node-1-key.pem ./admin.pem ./admin-key.pem ./root-ca.pem
# Differs from the install documentation replacing `mv -n` to just `mv` ensuring the files are always moved, even if they already exist.
mv /etc/wazuh-indexer/certs/node-1.pem /etc/wazuh-indexer/certs/indexer.pem
mv /etc/wazuh-indexer/certs/node-1-key.pem /etc/wazuh-indexer/certs/indexer-key.pem
chmod 500 /etc/wazuh-indexer/certs
chmod 400 /etc/wazuh-indexer/certs/*
chown -R wazuh-indexer:wazuh-indexer /etc/wazuh-indexer/certs

# =====
# Reload systemd daemon and start the service
# =====
if command -v apt-get &> /dev/null; then
    systemctl daemon-reload
    systemctl enable wazuh-indexer
    systemctl start wazuh-indexer
else
    chkconfig --add wazuh-indexer
    service wazuh-indexer start
fi


# =====
# Initialize indexer security
# =====
/usr/share/wazuh-indexer/bin/indexer-security-init.sh
