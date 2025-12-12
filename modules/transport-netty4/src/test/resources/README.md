#!/usr/bin/env bash
#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script
#

```bash
export PW='password'
export LIB_PATH="/path/to/lib/folder"
```

# 1 Create a server & root (self-signed) CAs
Root CA
└── Server certificate
```bash
printf '%s\n' \
"[ req ]
default_bits       = 2048
prompt             = no
default_md         = sha256
req_extensions     = SAN
distinguished_name = dn

[ dn ]
CN = OpenSearch Test
OU = opensearch
O  = org

[ SAN ]
subjectAltName = DNS:localhost, IP:127.0.0.1, IP:::1
" > openssl.conf

openssl req -x509 -new -nodes -keyout root.key -out root.crt -days 8192 -subj "/CN=Root CA"

openssl req -new -nodes \
    -keyout server.key \
    -out server.csr \
    -config openssl.conf
openssl x509 -req \
    -in server.csr \
    -CA root.crt \
    -CAkey root.key \
    -CAcreateserial \
    -out server.crt \
    -days 8192 \
    -extfile openssl.conf \
    -extensions SAN \
    -sigopt rsa_padding_mode:pss \
    -sigopt rsa_pss_saltlen:-1
```

# 2. Export the certificates in pkcs12 format
```bash
keytool -import -trustcacerts \
    -file root.crt \
    -alias root-ca \
    -keystore netty4-client-truststore.p12 \
    -storepass "$PW" \
    -noprompt
openssl pkcs12 -export \
    -in server.crt \
    -inkey server.key \
    -out netty4-server-keystore.p12 \
    -name netty4-server-keystore \
    -passout pass:"$PW" \
    -passin pass:"$PW"
```

# 3. Import the certificate into JDK keystore (PKCS12 type)
```bash
keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-keystore.jks \
    -srckeystore netty4-server-keystore.p12 \
    -srcstoretype PKCS12 \
    -deststorepass "$PW"
keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-truststore.jks \
    -srckeystore netty4-client-truststore.p12 \
    -srcstoretype PKCS12 \
    -deststorepass "$PW"
```

# 4. Import the certificate into BCFKS keystore
```bash
keytool -importkeystore \
    -noprompt \
    -srckeystore netty4-server-keystore.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-keystore.bcfks \
    -deststoretype BCFKS \
    -deststorepass "$PW" \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
keytool -importkeystore \
    -noprompt \
    -srckeystore netty4-client-truststore.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-truststore.bcfks \
    -deststoretype BCFKS \
    -deststorepass "$PW" \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```
