This is README describes how the certificates in this directory were created.
This file can also be executed as a script.
google-api-java-client provides its own trusted certificates inside google keystore which comes in JKS or PKCS#12 formats.
Since BCFIPS requires its own BCFKS format this script creates it.

# Take exiting PKCS12 trust store from the official library 
```bash
cp $GOOGLE_LIB_PATH/com/google/api/client/googleapis/google.p12 .
```

# Import the certificate into BCFKS keystore
```bash
keytool -importkeystore -noprompt \
    -srckeystore google.p12 \
    -srcstoretype PKCS12 \
    -srcstorepass notasecret \
    -destkeystore google.bcfks \
    -deststoretype BCFKS \
    -deststorepass notasecret \
    -providername BCFIPS \
    -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
    -providerpath $LIB_PATH/bc-fips-2.0.0.jar
```
