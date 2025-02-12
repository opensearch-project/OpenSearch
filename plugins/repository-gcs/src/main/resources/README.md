#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script.
# google-api-java-client provides its own trusted certificates inside google keystore which comes in JKS or PKCS#12 formats.
# Since BCFIPS requires its own BCFKS format this script creates it.
#

```
keytool -importkeystore -noprompt \
    -srckeystore $KEY_STORE_PATH/google.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass notasecret \
    -destkeystore google.bcfks \
    -deststoretype BCFKS \
    -deststorepass notasecret \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```
