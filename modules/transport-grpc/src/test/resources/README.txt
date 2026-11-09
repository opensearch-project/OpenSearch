#!/usr/bin/env bash
#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script
#

# 1. Create server & client certificate key

openssl req  -x509  -sha256  -newkey rsa:2048  -keyout server.key  -out server.crt  -days 8192  -nodes
openssl req  -x509  -sha256  -newkey rsa:2048  -keyout client.key  -out client.crt  -days 8192  -nodes

# 2. Export the certificates in pkcs12 format

openssl pkcs12  -export  -in server.crt  -inkey server.key  -out server.p12  -name netty4-server-secure -password pass:password
openssl pkcs12  -export  -in client.crt  -inkey client.key  -out client.p12  -name netty4-client-secure -password pass:password

# 3. Import the certificate into JDK keystore (PKCS12 type)

keytool -importkeystore -srcstorepass password  -destkeystore netty4-server-secure.jks -srckeystore server.p12  -srcstoretype PKCS12  -alias netty4-server-secure  -deststorepass password
keytool -importkeystore -srcstorepass password  -destkeystore netty4-client-secure.jks -srckeystore client.p12  -srcstoretype PKCS12  -alias netty4-client-secure  -deststorepass password

# 4. Clean up - Clean up pkcs12 keystores and private keys
rm client.key
rm client.p12
rm server.key
rm server.p12
