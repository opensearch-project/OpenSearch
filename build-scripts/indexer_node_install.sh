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



VERSION=${1%.*}

curl -sO https://packages.wazuh.com/$VERSION/wazuh-certs-tool.sh
curl -sO https://packages.wazuh.com/$VERSION/config.yml

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
    apt-get install -y debconf adduser procps
    apt-get install -y gnupg apt-transport-https
    curl -s https://packages.wazuh.com/key/GPG-KEY-WAZUH | gpg --no-default-keyring --keyring gnupg-ring:/usr/share/keyrings/wazuh.gpg --import && chmod 644 /usr/share/keyrings/wazuh.gpg
    echo "deb [signed-by=/usr/share/keyrings/wazuh.gpg] https://packages.wazuh.com/4.x/apt/ stable main" | tee -a /etc/apt/sources.list.d/wazuh.list
    apt-get update
    apt-get -y install wazuh-indexer=$1-1
else
  yum install coreutils -y

  rpm --import https://packages.wazuh.com/key/GPG-KEY-WAZUH
  echo -e '[wazuh]\ngpgcheck=1\ngpgkey=https://packages.wazuh.com/key/GPG-KEY-WAZUH\nenabled=1\nname=EL-$releasever - Wazuh\nbaseurl=https://packages.wazuh.com/4.x/yum/\nprotect=1' | tee /etc/yum.repos.d/wazuh.repo

  yum -y install wazuh-indexer-$1-1

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
