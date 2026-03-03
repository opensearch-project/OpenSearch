#!/bin/bash

# Initialize the `.opendistro_security` index.
sleep 30
bash "$INDEXER_HOME"/plugins/opensearch-security/tools/securityadmin.sh \
    -cacert "$INDEXER_HOME"/config/certs/root-ca.pem \
    -cert "$INDEXER_HOME"/config/certs/admin.pem \
    -key "$INDEXER_HOME"/config/certs/admin-key.pem \
    -cd "$INDEXER_HOME"/config/opensearch-security/ \
    -nhnv \
    -icl
