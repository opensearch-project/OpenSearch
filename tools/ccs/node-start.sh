#!/bin/bash

# This scripts configures the enviroment for the diferent nodes in the ccs.
# Usage: ./node-start.sh <node_name> <version>
# node_name can be "ccs", "cluster_a" or "cluster_b".
#
# It is used to create the certificates and configure the nodes for the cluster.

if [ -z "$2" ]; then
    echo "Usage: $0 <node_name> <version>"
    echo "node_name can be 'ccs', 'cluster_a' or 'cluster_b'."
    echo "version is the Wazuh version to use, e.g., 4.12.0"
    exit 1
fi

NODE=$1
WAZUH_VERSION=$2

# Check if the node name is valid
if [[ "$NODE" != "ccs" && "$NODE" != "cluster_a" && "$NODE" != "cluster_b" ]]; then
    echo "Invalid node name: $NODE"
    echo "Valid node names are: ccs, cluster_a, cluster_b"
    exit 1
fi


if [ "$NODE" == "ccs" ]; then

    # Create the certificates for the Wazuh ccs node
    tar -cvf ./wazuh-certificates.tar -C ./wazuh-certificates/ .
    rm -rf ./wazuh-certificates

    # Install wazuh-indexer
    yum install -y coreutils
    rpm --import https://packages.wazuh.com/key/GPG-KEY-WAZUH
    echo -e '[wazuh]\ngpgcheck=1\ngpgkey=https://packages.wazuh.com/key/GPG-KEY-WAZUH\nenabled=1\nname=EL-$releasever - Wazuh\nbaseurl=https://packages.wazuh.com/4.x/yum/\nprotect=1' | tee /etc/yum.repos.d/wazuh.repo
    yum update -y

    yum -y install wazuh-indexer-$WAZUH_VERSION-1

    # Configure the wazuh-indexer /etc/wazuh-indexer/opensearch.yml file
    sed -i 's/node-1/ccs-wazuh-indexer-1/g' /etc/wazuh-indexer/opensearch.yml
    sed -i 's/^network\.host:.*$/network.host: "192.168.56.10"/' /etc/wazuh-indexer/opensearch.yml
    sed -i 's/^cluster\.name:.*$/cluster.name: "ccs-cluster"/' /etc/wazuh-indexer/opensearch.yml



    # Deploy the certificates
    NODE_NAME=ccs-wazuh-indexer-1
    mkdir /etc/wazuh-indexer/certs
    tar -xf ./wazuh-certificates.tar -C /etc/wazuh-indexer/certs/ ./$NODE_NAME.pem ./$NODE_NAME-key.pem ./admin.pem ./admin-key.pem ./root-ca.pem
    mv -n /etc/wazuh-indexer/certs/$NODE_NAME.pem /etc/wazuh-indexer/certs/indexer.pem
    mv -n /etc/wazuh-indexer/certs/$NODE_NAME-key.pem /etc/wazuh-indexer/certs/indexer-key.pem
    chmod 500 /etc/wazuh-indexer/certs
    chmod 400 /etc/wazuh-indexer/certs/*
    chown -R wazuh-indexer:wazuh-indexer /etc/wazuh-indexer/certs

    # Start the Wazuh indexer service
    systemctl daemon-reload
    systemctl enable wazuh-indexer
    systemctl start wazuh-indexer

    # Initialize the Wazuh indexer cluster
    /usr/share/wazuh-indexer/bin/indexer-security-init.sh

    # Install wazuh-dashboard
    yum install libcap
    yum -y install wazuh-dashboard-$WAZUH_VERSION-1

    # Configure the wazuh-dashboard /etc/wazuh-dashboard/opensearch_dashboards.yml file
    sed -i 's|^opensearch\.hosts:.*$|opensearch.hosts: "https://192.168.56.10:9200"|' /etc/wazuh-dashboard/opensearch_dashboards.yml

    # Deploy the certificates
    NODE_NAME=ccs-wazuh-dashboard
    mkdir /etc/wazuh-dashboard/certs
    tar -xf ./wazuh-certificates.tar -C /etc/wazuh-dashboard/certs/ ./$NODE_NAME.pem ./$NODE_NAME-key.pem ./root-ca.pem
    mv -n /etc/wazuh-dashboard/certs/$NODE_NAME.pem /etc/wazuh-dashboard/certs/dashboard.pem
    mv -n /etc/wazuh-dashboard/certs/$NODE_NAME-key.pem /etc/wazuh-dashboard/certs/dashboard-key.pem
    chmod 500 /etc/wazuh-dashboard/certs
    chmod 400 /etc/wazuh-dashboard/certs/*
    chown -R wazuh-dashboard:wazuh-dashboard /etc/wazuh-dashboard/certs

    # Start the Wazuh dashboard service
    systemctl daemon-reload
    systemctl enable wazuh-dashboard
    systemctl start wazuh-dashboard

    # Configure the Wazuh dashboard /usr/share/wazuh-dashboard/data/wazuh/config/wazuh.yml file
    sleep 30  # Wait for the wazuh-dashboard plugin to create the file
    cat <<EOF > /usr/share/wazuh-dashboard/data/wazuh/config/wazuh.yml
---
hosts:
  - Cluster A:
      url: https://192.168.56.11
      port: 55000
      username: wazuh-wui
      password: wazuh-wui
      run_as: true
  - Cluster B:
      url: https://192.168.56.12
      port: 55000
      username: wazuh-wui
      password: wazuh-wui
      run_as: true
EOF

    # Restart the Wazuh dashboard service to apply the changes
    systemctl restart wazuh-dashboard


    # Configure the Wazuh indexer cluster settings
    curl -XPUT -k -u admin:admin "https://192.168.56.10:9200/_cluster/settings" -H 'Content-Type: application/json' -d'
    {
    "persistent": {
        "cluster.remote": {
        "ca-wazuh-indexer-1": {
            "seeds": ["192.168.56.11:9300"]
        },
        "cb-wazuh-indexer-1": {
            "seeds": ["192.168.56.12:9300"]
        }
        }
    }
    }'
    
else
    version=$(echo "$2" | cut -d'.' -f1-2) 

    # Create the certificates for the Wazuh cluster A node
    curl -sO https://packages.wazuh.com/$version/wazuh-certs-tool.sh

    if [ "$NODE" == "cluster_a" ]; then
        # Create the config file for cluster A
        cat <<EOF > config.yml
nodes:
  # Wazuh indexer nodes
  indexer:
    - name: ca-wazuh-indexer-1
      ip: "192.168.56.11"

  # Wazuh server nodes
  server:
    - name: ca-wazuh-server-1
      ip: "192.168.56.11"
EOF

    else
        # Create the config file for cluster B
        cat <<EOF > config.yml
nodes:
  # Wazuh indexer nodes
  indexer:
    - name: cb-wazuh-indexer-1
      ip: "192.168.56.12"

  # Wazuh server nodes
  server:
    - name: cb-wazuh-server-1
      ip: "192.168.56.12"
EOF
    fi


    # Generate the certificates using the root CA from the ccs node
    bash ./wazuh-certs-tool.sh -A ./root-ca.pem ./root-ca.key
    tar -cvf ./wazuh-certificates.tar -C ./wazuh-certificates/ .
    rm -rf ./wazuh-certificates

    # Install wazuh-indexer
    yum install -y coreutils
    rpm --import https://packages.wazuh.com/key/GPG-KEY-WAZUH
    echo -e '[wazuh]\ngpgcheck=1\ngpgkey=https://packages.wazuh.com/key/GPG-KEY-WAZUH\nenabled=1\nname=EL-$releasever - Wazuh\nbaseurl=https://packages.wazuh.com/4.x/yum/\nprotect=1' | tee /etc/yum.repos.d/wazuh.repo

    yum update -y
    yum -y install wazuh-indexer-$WAZUH_VERSION-1

    # Configure the wazuh-indexer /etc/wazuh-indexer/opensearch.yml file
    if [ "$NODE" == "cluster_a" ]; then
        sed -i 's/node-1/ca-wazuh-indexer-1/g' /etc/wazuh-indexer/opensearch.yml
        sed -i 's/^network\.host:.*$/network.host: "192.168.56.11"/' /etc/wazuh-indexer/opensearch.yml
        sed -i 's/^cluster\.name:.*$/cluster.name: "ca-cluster"/' /etc/wazuh-indexer/opensearch.yml
        sed -i '/#- "CN=node-2,OU=Wazuh,O=Wazuh,L=California,C=US"/c\- "CN=ccs-wazuh-indexer-1,OU=Wazuh,O=Wazuh,L=California,C=US"' /etc/wazuh-indexer/opensearch.yml
    else
        sed -i 's/node-1/cb-wazuh-indexer-1/g' /etc/wazuh-indexer/opensearch.yml
        sed -i 's/^network\.host:.*$/network.host: "192.168.56.12"/' /etc/wazuh-indexer/opensearch.yml
        sed -i 's/^cluster\.name:.*$/cluster.name: "cb-cluster"/' /etc/wazuh-indexer/opensearch.yml
        sed -i '/#- "CN=node-2,OU=Wazuh,O=Wazuh,L=California,C=US"/c\- "CN=ccs-wazuh-indexer-1,OU=Wazuh,O=Wazuh,L=California,C=US"' /etc/wazuh-indexer/opensearch.yml
    fi

    
    # Deploy the certificates
    if [ "$NODE" == "cluster_a" ]; then
        NODE_NAME=ca-wazuh-indexer-1
    else
        NODE_NAME=cb-wazuh-indexer-1
    fi
    mkdir /etc/wazuh-indexer/certs
    tar -xf ./wazuh-certificates.tar -C /etc/wazuh-indexer/certs/ ./$NODE_NAME.pem ./$NODE_NAME-key.pem ./admin.pem ./admin-key.pem ./root-ca.pem
    mv -n /etc/wazuh-indexer/certs/$NODE_NAME.pem /etc/wazuh-indexer/certs/indexer.pem
    mv -n /etc/wazuh-indexer/certs/$NODE_NAME-key.pem /etc/wazuh-indexer/certs/indexer-key.pem
    chmod 500 /etc/wazuh-indexer/certs
    chmod 400 /etc/wazuh-indexer/certs/*
    chown -R wazuh-indexer:wazuh-indexer /etc/wazuh-indexer/certs

    # Start the Wazuh indexer service
    systemctl daemon-reload
    systemctl enable wazuh-indexer
    systemctl start wazuh-indexer

    # Initialize the Wazuh indexer cluster
    /usr/share/wazuh-indexer/bin/indexer-security-init.sh

    # Install wazuh-server
    yum -y install wazuh-manager-$WAZUH_VERSION-1
    yum -y install filebeat

    curl -so /etc/filebeat/filebeat.yml https://packages.wazuh.com/$version/tpl/wazuh/filebeat/filebeat.yml

    # Configure the /etc/filebeat/filebeat.yml file
    if [ "$NODE" == "cluster_a" ]; then
        sed -i 's|^[ \t]*hosts: \["127\.0\.0\.1:9200"\]|  hosts: ["192.168.56.11:9200"]|' /etc/filebeat/filebeat.yml
    else
        sed -i 's|^[ \t]*hosts: \["127\.0\.0\.1:9200"\]|  hosts: ["192.168.56.12:9200"]|' /etc/filebeat/filebeat.yml
    fi

    # Create filebeat keystore
    filebeat keystore create
    echo admin | filebeat keystore add username --stdin --force
    echo admin | filebeat keystore add password --stdin --force

    # Download the alerts template
    curl -so /etc/filebeat/wazuh-template.json https://raw.githubusercontent.com/wazuh/wazuh/v$2/extensions/elasticsearch/7.x/wazuh-template.json
    chmod go+r /etc/filebeat/wazuh-template.json

    # Install the Wazuh module for Filebeat
    curl -s https://packages.wazuh.com/4.x/filebeat/wazuh-filebeat-0.4.tar.gz | tar -xvz -C /usr/share/filebeat/module

    # Deploy the certificates
    if [ "$NODE" == "cluster_a" ]; then
        NODE_NAME=ca-wazuh-server-1
    else
        NODE_NAME=cb-wazuh-server-1
    fi
    mkdir /etc/filebeat/certs
    tar -xf ./wazuh-certificates.tar -C /etc/filebeat/certs/ ./$NODE_NAME.pem ./$NODE_NAME-key.pem ./root-ca.pem
    mv -n /etc/filebeat/certs/$NODE_NAME.pem /etc/filebeat/certs/filebeat.pem
    mv -n /etc/filebeat/certs/$NODE_NAME-key.pem /etc/filebeat/certs/filebeat-key.pem
    chmod 500 /etc/filebeat/certs
    chmod 400 /etc/filebeat/certs/*
    chown -R root:root /etc/filebeat/certs

    # Save the Wazuh indexer user and password in the manager keystore
    /var/ossec/bin/wazuh-keystore -f indexer -k username -v admin
    /var/ossec/bin/wazuh-keystore -f indexer -k password -v admin

    # Configure the Wazuh manager /var/ossec/etc/ossec.conf file
    if [ "$NODE" == "cluster_a" ]; then
        sed -i 's|<host>.*</host>|<host>https://192.168.56.11:9200</host>|' /var/ossec/etc/ossec.conf
    else
        sed -i 's|<host>.*</host>|<host>https://192.168.56.12:9200</host>|' /var/ossec/etc/ossec.conf
    fi


    # Start the Wazuh manager service and Filebeat
    systemctl daemon-reload
    systemctl enable wazuh-manager
    systemctl start wazuh-manager
    systemctl enable filebeat
    systemctl start filebeat
fi
