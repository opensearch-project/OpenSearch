#!/bin/sh
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# Directories
TMP_DIR="/tmp/wazuh-indexer/certs"
CERTS_DIR="/etc/wazuh-indexer/certs"

# Create directories
mkdir -p "$TMP_DIR"

# Root CA
openssl genrsa -out "$TMP_DIR/root-ca-key-temp.pem" 2048
openssl req -new -x509 -sha256 -key "$TMP_DIR/root-ca-key-temp.pem" -subj "/OU=Wazuh/O=Wazuh/L=California/" -out "$TMP_DIR/root-ca.pem" -days 3650

# Admin cert
openssl genrsa -out "$TMP_DIR/admin-key-temp.pem" 2048
openssl pkcs8 -inform PEM -outform PEM -in "$TMP_DIR/admin-key-temp.pem" -topk8 -nocrypt -v1 PBE-SHA1-3DES -out "$TMP_DIR/admin-key.pem"
openssl req -new -key "$TMP_DIR/admin-key.pem" -subj "/C=US/L=California/O=Wazuh/OU=Wazuh/CN=admin" -out "$TMP_DIR/admin.csr"
openssl x509 -req -in "$TMP_DIR/admin.csr" -CA "$TMP_DIR/root-ca.pem" -CAkey "$TMP_DIR/root-ca-key-temp.pem" -CAcreateserial -sha256 -out "$TMP_DIR/admin.pem" -days 3650

# Node cert
openssl genrsa -out "$TMP_DIR/indexer-1-key-temp.pem" 2048
openssl pkcs8 -inform PEM -outform PEM -in "$TMP_DIR/indexer-1-key-temp.pem" -topk8 -nocrypt -v1 PBE-SHA1-3DES -out "$TMP_DIR/indexer-1-key.pem"
openssl req -new -key "$TMP_DIR/indexer-1-key.pem" -subj "/C=US/L=California/O=Wazuh/OU=Wazuh/CN=node-0.wazuh.indexer" -out "$TMP_DIR/indexer.csr"
cat <<'INDEXER_EXT' >$TMP_DIR/indexer.ext
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
DNS.2 = *.wazuh.indexer
RID.1 = 1.2.3.4.5
IP.1 = 127.0.0.1
IP.2 =  0:0:0:0:0:0:0:1
INDEXER_EXT

openssl x509 -req -in "$TMP_DIR/indexer.csr" -CA "$TMP_DIR/root-ca.pem" -CAkey "$TMP_DIR/root-ca-key-temp.pem" -CAcreateserial -sha256 -out "$TMP_DIR/indexer-1.pem" -days 3650 -extfile "$TMP_DIR/indexer.ext"

# Cleanup temporary files
rm "$TMP_DIR/"*.csr "$TMP_DIR"/*.ext "$TMP_DIR"/*.srl "$TMP_DIR"/*-temp.pem

# Move certs to permanent location
mkdir -p "$CERTS_DIR"
mv "$TMP_DIR"/* "$CERTS_DIR/"

chmod 500 "$CERTS_DIR"
chmod 400 "$CERTS_DIR"/*
chown -R wazuh-indexer:wazuh-indexer "$CERTS_DIR"

# Cleanup /tmp directory
rm -r "$TMP_DIR"
