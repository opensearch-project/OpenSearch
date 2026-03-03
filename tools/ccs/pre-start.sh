#!/bin/bash

# This scripts creates the necesary certificates for the Wazuh ccs.
# Usage: ./pre-start.sh

if [ -z "$1" ]; then
    echo "Usage: $0 <version>"
    echo "version is the Wazuh version to use, e.g., 4.12.0"
    exit 1
fi

# Delete the last part of the version as it is not needed for the script
# For example, if the version is 4.12.0, we will use 4.12
version=$(echo "$1" | cut -d'.' -f1-2)  

curl -sO https://packages.wazuh.com/$version/wazuh-certs-tool.sh

chmod +x ./wazuh-certs-tool.sh

cat <<EOF > config.yml
nodes:
  # Wazuh indexer nodes
  indexer:
    - name: ccs-wazuh-indexer-1
      ip: "192.168.56.10"

  # Wazuh dashboard nodes
  dashboard:
    - name: ccs-wazuh-dashboard
      ip: "192.168.56.10"
EOF


# Once the config file is created, the certificates can be generated
OPENSSL_CONF="/etc/ssl/openssl.cnf" ./wazuh-certs-tool.sh -A

rm -f wazuh-certs-tool.sh config.yml
