# Create first CA PEM ("ca1")

```bash
opensearch-certutil ca --pem --out ca1.zip --days 9999 --ca-dn "CN=Test CA 1"
unzip ca1.zip
mv ca ca1
```

# Create first CA PEM ("ca2")

```bash
opensearch-certutil ca --pem --out ca2.zip --days 9999 --ca-dn "CN=Test CA 2"
unzip ca2.zip
mv ca ca2
```

# Create first CA PEM ("ca3")

```bash
opensearch-certutil ca --pem --out ca3.zip --days 9999 --ca-dn "CN=Test CA 3"
unzip ca3.zip
mv ca ca3
```

# Create "cert1-pkcs1" PEM

```bash
opensearch-certutil cert --pem --out cert1-pkcs1.zip --name cert1 --ip 127.0.0.1 --dns localhost --days 9999 --ca-key ca1/ca.key --ca-cert ca1/ca.crt
unzip cert1.zip
```

# Create "cert2-pkcs1" PEM (same as cert1, but with a password)

```bash
opensearch-certutil cert --pem --out cert2-pkcs1.zip --name cert2 --ip 127.0.0.1 --dns localhost --days 9999 --ca-key ca1/ca.key --ca-cert ca1/ca.crt --pass "c2-pass"
unzip cert2.zip
```

# Create "cert1" PEM

```bash
openssl genpkey -algorithm RSA -out cert1/cert1.key
openssl req -new \
    -key cert1/cert1.key \
    -subj "/CN=cert1" \
    -out cert1/cert1.csr
openssl x509 -req \
    -in cert1/cert1.csr \
    -CA ca1/ca.crt \
    -CAkey ca1/ca.key \
    -CAcreateserial \
    -out cert1/cert1.crt \
    -days 3650 \
    -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")
rm cert1/cert1.csr
```

# Create "cert2" PEM (same as cert1, but with a password)
export KEY_PW='6!6428DQXwPpi7@$ggeg/='
```bash
openssl genpkey -algorithm RSA -out cert2/cert2.key -aes256 -pass pass:"$KEY_PW"
openssl req -new \
-key cert2/cert2.key \
-subj "/CN=cert2" \
-out cert2/cert2.csr \
-passin pass:"$KEY_PW"
openssl x509 -req \
-in cert2/cert2.csr \
-CA ca1/ca.crt \
-CAkey ca1/ca.key \
-CAcreateserial \
-out cert2/cert2.crt \
-days 3650 \
-sha256 \
-extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
-passin pass:"$KEY_PW"
rm cert2/cert2.csr
```

# Convert Certs to PKCS#12

```bash
for n in 1 2 3
do
    keytool -importcert -file ca${n}/ca.crt -alias ca -keystore ca${n}/ca.p12 -storetype PKCS12 -storepass p12-pass -v
    keytool -importcert -file ca${n}/ca.crt -alias ca${n} -keystore ca-all/ca.p12 -storetype PKCS12 -storepass p12-pass -v
done
```

# Convert Certs to JKS

```bash
for n in 1 2 3
do
    keytool -importcert -file ca${n}/ca.crt -alias ca${n} -keystore ca-all/ca.jks -storetype jks -storepass jks-pass -v
done
```

# Convert Certs to PKCS#12

```bash
for Cert in cert1 cert2
do
    openssl pkcs12 -export -out $Cert/$Cert.p12 -inkey $Cert/$Cert.key -in $Cert/$Cert.crt -name $Cert -passout pass:p12-pass
done
```

# Convert Certs to PKCS#12 without MAC

```bash
cat ca1/ca.crt ca2/ca.crt ca3/ca.crt >> chain.crt
openssl pkcs12 -export \
        -out ca-all/ca_nomac.p12 \
        -in chain.crt \
        -caname "Test CA 1" \
        -caname "Test CA 2" \
        -caname "Test CA 3" \
        -passout pass: \
        -nomac \
        -nokeys \
        -jdktrust anyExtendedKeyUsage
rm -f chain.crt
```

# Convert Certs to BCFKS

```bash
for n in 1 2 3
do
    keytool -importcert -file ca${n}/ca.crt -alias ca -keystore ca${n}/ca.bcfks -storetype BCFKS -storepass bcfks-pass -providername BCFIPS -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider -providerpath $LIB_PATH/bc-fips-2.0.0.jar -v
    keytool -importcert -file ca${n}/ca.crt -alias ca${n} -keystore ca-all/ca.bcfks -storetype BCFKS -storepass bcfks-pass -providername BCFIPS -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider -providerpath $LIB_PATH/bc-fips-2.0.0.jar -v
done
```

# Import Certs into single PKCS#12 keystore

```bash
for Cert in cert1 cert2
do
    keytool -importkeystore -noprompt \
            -srckeystore $Cert/$Cert.p12 -srcstoretype PKCS12 -srcstorepass p12-pass  \
            -destkeystore cert-all/certs.p12 -deststoretype PKCS12 -deststorepass p12-pass
done
```

# Import Certs into single JKS keystore with separate key-password

```bash
for Cert in cert1 cert2
do
    keytool -importkeystore -noprompt \
            -srckeystore $Cert/$Cert.p12 -srcstoretype PKCS12 -srcstorepass p12-pass  \
            -destkeystore cert-all/certs.jks -deststoretype jks -deststorepass jks-pass
    keytool -keypasswd -keystore cert-all/certs.jks -alias $Cert -keypass p12-pass -new key-pass -storepass jks-pass
done
```

# Import Certs into single BCFKS keystore with separate key-password

```bash
for Cert in cert1 cert2
do
    keytool -importkeystore -noprompt \
            -srckeystore $Cert/$Cert.p12 \
            -srcstoretype PKCS12 \
            -srcstorepass p12-pass \
            -destkeystore cert-all/certs.bcfks \
            -deststoretype BCFKS \
            -deststorepass bcfks-pass \
            -providername BCFIPS \
            -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
            -providerpath $LIB_PATH/bc-fips-2.0.0.jar
    keytool -keypasswd -noprompt \
            -keystore cert-all/certs.bcfks \
            -alias $Cert \
            -keypass p12-pass \
            -new bcfks-pass \
            -storepass bcfks-pass \
            -storetype BCFKS \
            -providername BCFIPS \
            -provider org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider \
            -providerpath $LIB_PATH/bc-fips-2.0.0.jar
done
```

# Create a mimic of the first CA ("ca1b") for testing certificates with the same name but different keys

```bash
opensearch-certutil ca --pem --out ${PWD}/ca1-b.zip --days 9999 --ca-dn "CN=Test CA 1"
unzip ca1-b.zip
mv ca ca1-b
```

# Create empty KeyStore

```bash
keytool -genkeypair \
        -alias temp \
        -storetype JKS \
        -keyalg rsa \
        -storepass storePassword \
        -keypass secretPassword \
        -keystore cert-all/empty.jks \
        -dname "CN=foo,DC=example,DC=com"
keytool -delete \
        -alias temp \
        -storepass storePassword \
        -keystore cert-all/empty.jks
```
