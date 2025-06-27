#!/usr/bin/env bash
#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script
#

# 1. Create certificate key

`openssl req -x509 -sha256 -newkey rsa:2048 -keyout certificate.key -out certificate.crt -days 1024 -nodes`

# 2. Export the certificate in pkcs12 format

`openssl pkcs12 -export -in certificate.crt -inkey certificate.key -out netty4-secure.p12 -name netty4-secure -password pass:password`

# 3. Migrate from P12 to JKS keystore

```
keytool -importkeystore -noprompt \
    -srckeystore netty4-secure.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass password \
    -alias netty4-secure \
    -destkeystore netty4-secure.jks \ 
    -deststoretype JKS \
    -deststorepass password
```

# 4. Migrate from P12 to BCFIPS keystore

```
keytool -importkeystore -noprompt \
    -srckeystore netty4-secure.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass password \
    -alias netty4-secure \
    -destkeystore netty4-secure.bcfks \
    -deststoretype BCFKS \
    -deststorepass password \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```
