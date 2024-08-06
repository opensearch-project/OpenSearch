#!/usr/bin/env bash
#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script
#

# 1. Create certificate key

openssl req  -x509  -sha256  -newkey rsa:2048  -keyout certificate.key  -out certificate.crt  -days 1024  -nodes

# 2. Export the certificate in pkcs12 format

openssl pkcs12  -export  -in certificate.crt  -inkey certificate.key  -out server.p12  -name netty4-secure -password pass:password

# 3. Import the certificate into JDK keystore (PKCS12 type)

keytool -importkeystore -srcstorepass password  -destkeystore netty4-secure.jks -srckeystore server.p12  -srcstoretype PKCS12  -alias netty4-secure  -deststorepass password