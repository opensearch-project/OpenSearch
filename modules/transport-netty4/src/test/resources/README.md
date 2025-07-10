#!/usr/bin/env bash
#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script
#

# 1. Create server certificate
Root CA
└── Intermediate CA
└── Server certificate


```bash
export PW='password'
export LIB_PATH="/path/to/lib/folder"
```

# 1.1 Create a Root CA (self-signed)
```bash
openssl req -x509 -new -nodes -keyout root.key -out root.crt -days 8192 -subj "/CN=Root CA"
```

# 1.2 Create an Intermediate CA CSR, sign with Root
```bash
openssl req -new -nodes \
    -keyout intermediate.key \
    -out intermediate.csr \
    -subj "/CN=Intermediate CA"
openssl x509 -req \
    -in intermediate.csr \
    -CA root.crt \
    -CAkey root.key \
    -CAcreateserial \
    -out intermediate.crt \
    -days 8192 \
    -extensions v3_ca \
    -extfile openssl-server-san.conf \
    -sigopt rsa_padding_mode:pss \
    -sigopt rsa_pss_saltlen:-1
```

# 1.3 Create Server CSR, sign with Intermediate
```bash
openssl req -new -nodes \
    -keyout server.key \
    -out server.csr \
    -config openssl-server-san.conf
openssl x509 -req \
    -in server.csr \
    -CA intermediate.crt \
    -CAkey intermediate.key \
    -CAcreateserial \
    -out server.crt \
    -days 8192 \
    -extensions req_ext \
    -extfile openssl-server-san.conf \
    -sigopt rsa_padding_mode:pss \
    -sigopt rsa_pss_saltlen:-1
```

# 2. Create a client certificate and key
```bash
openssl req -new -nodes \
    -keyout client.key \
    -out client.csr \
    -config openssl-client-san.conf
openssl x509 -req \
    -in client.csr \
    -CA intermediate.crt \
    -CAkey intermediate.key \
    -CAcreateserial \
    -out client.crt \
    -days 8192 \
    -extensions req_ext \
    -extfile openssl-client-san.conf \
    -sigopt rsa_padding_mode:pss \
    -sigopt rsa_pss_saltlen:-1
```

# 3. Export the certificates in pkcs12 format
```bash
keytool -import -trustcacerts \
    -file root.crt \
    -alias root-ca \
    -keystore netty4-client-truststore.p12 \
    -storepass "$PW" \
    -noprompt
keytool -import -trustcacerts \
    -file intermediate.crt \
    -alias intermediate-ca \
    -keystore netty4-client-truststore.p12 \
    -storepass "$PW" \
    -noprompt

keytool -import -trustcacerts \
    -file root.crt \
    -alias root \
    -keystore netty4-server-truststore.p12 \
    -storepass "$PW" \
    -noprompt
keytool -import -trustcacerts \
    -file intermediate.crt \
    -alias intermediate \
    -keystore netty4-server-truststore.p12 \
    -storepass "$PW" \
    -noprompt

cat server.crt intermediate.crt > server-fullchain.crt
openssl pkcs12 -export \
    -in server-fullchain.crt \
    -inkey server.key \
    -out netty4-server-keystore.p12 \
    -name netty4-server-keystore \
    -passout pass:"$PW" \
    -passin pass:"$PW"

cat client.crt intermediate.crt > client-fullchain.crt
openssl pkcs12 -export \
    -in client-fullchain.crt \
    -inkey client.key \
    -out netty4-client-keystore.p12 \
    -name netty4-client-keystore \
    -passout pass:"$PW" \
    -passin pass:"$PW"
```

# 4. Import the certificate into JDK keystore (PKCS12 type)
```bash
keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-keystore.jks \
    -srckeystore netty4-server-keystore.p12 \
    -srcstoretype PKCS12 \
    -deststorepass "$PW"

keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-keystore.jks \
    -srckeystore netty4-client-keystore.p12 \
    -srcstoretype PKCS12 \
    -deststorepass "$PW"

keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-truststore.jks \
    -srckeystore netty4-client-truststore.p12 \
    -srcstoretype PKCS12 \
    -deststorepass "$PW"

keytool -importkeystore \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-truststore.jks \
    -srckeystore netty4-server-truststore.p12 \
    -srcstoretype PKCS12 \
    -deststorepass "$PW"
```

# 5. Import the certificate into BCFKS keystore
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
    -srckeystore netty4-client-keystore.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass "$PW" \
    -destkeystore netty4-client-keystore.bcfks \
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

keytool -importkeystore \
    -noprompt \
    -srckeystore netty4-server-truststore.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass "$PW" \
    -destkeystore netty4-server-truststore.bcfks \
    -deststoretype BCFKS \
    -deststorepass "$PW" \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```

# 6. Verify the certicate chain is correct

```bash
keytool -list -v \
    -keystore netty4-server-keystore.bcfks \
    -storetype BCFKS \
    -storepass "$PW" \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```
