#!/usr/bin/env bash
#
# This README describes how the certificates in this directory were created.
# The commands below document the manual steps to generate test certificates.
#

# 1. Create server & client certificate key

```bash
export PW='password'
export LIB_PATH="/path/to/lib/folder"
```

```bash
cat <<'EOF' > openssl-san.conf
[ req ]
default_bits       = 2048
prompt             = no
default_md         = sha256
req_extensions     = SAN
distinguished_name = dn

[ dn ]
CN = OpenSearch Test Root CA
OU = opensearch
O  = org

[ SAN ]
subjectAltName = DNS:localhost, IP:127.0.0.1, IP:::1
EOF

openssl req -x509 -sha256 \
    -newkey rsa:2048 \
    -keyout server.key \
    -out server.crt \
    -days 8192 \
    -config openssl-san.conf \
    -extensions SAN \
    -passout pass:"$PW"
    
openssl req -x509 -sha256 \
    -newkey rsa:2048 \
    -keyout client.key \
    -out client.crt \
    -days 8192 \
    -config openssl-san.conf \
    -extensions SAN \
    -passout pass:"$PW"
```

# 2. Export the certificates in pkcs12 format

```bash
openssl pkcs12 -export \
    -in server.crt \
    -inkey server.key \
    -out netty4-server-secure.p12 \
    -name netty4-server-secure \
    -passout pass:"$PW" \
    -passin pass:"$PW"

openssl pkcs12 -export \
    -in client.crt \
    -inkey client.key \
    -out netty4-client-secure.p12 \
    -name netty4-client-secure \
    -passout pass:"$PW" \
    -passin pass:"$PW"
```

# 3. Import the certificate into JDK keystore (PKCS12 type)

```bash
keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-secure.jks \
    -srckeystore netty4-server-secure.p12 \
    -srcstoretype PKCS12 \
    -alias netty4-server-secure \
    -deststorepass "$PW"

keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-secure.jks \
    -srckeystore netty4-client-secure.p12 \
    -srcstoretype PKCS12 \
    -alias netty4-client-secure \
    -deststorepass "$PW"
```

# 4. Import the certificate into BCFKS keystore

```bash
keytool -importkeystore \
    -noprompt \
    -srckeystore netty4-server-secure.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-secure.bcfks \
    -deststoretype BCFKS \
    -deststorepass "$PW" \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
  
keytool -importkeystore \
    -noprompt \
    -srckeystore netty4-client-secure.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-secure.bcfks \
    -deststoretype BCFKS \
    -deststorepass "$PW" \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```
