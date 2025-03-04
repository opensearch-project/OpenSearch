# Instructions on generating self-signed certificates

The certificates in this directory have been generated using the
following openssl configuration and commands.

OpenSSL Configuration File is `openssl_config.cnf`.

The `alt_names` section provides the Subject Alternative Names for each
certificate. This is necessary for testing with hostname verification
enabled.

```bash
    openssl req -new -x509 -extensions v3_req -out <NAME>.cert -keyout <NAME>.pem -days 1460 -config openssl_config.cnf
```

When prompted the password is always set to the value of &lt;NAME&gt;.

Because we intend to import these certificates into a Java Keystore
file, they certificate and private key must be combined in a PKCS12
certificate.


```bash
    openssl pkcs12 -export -name <NAME> -in <NAME>.cert -inkey <NAME>.pem -out <NAME>.p12
```

# Creating the Keystore

We need to create a keystore from the created PKCS12 certificate.

```bash
    keytool -importkeystore -destkeystore <NAME>.jks -srckeystore <NAME>.p12 -srcstoretype pkcs12 -alias <NAME>
```

The keystore is now created and has the private/public key pair. You can
import additional trusted certificates using `keytool -importcert`. When
doing so make sure to specify an alias so that others can recreate the
keystore if necessary.

## Changes and additions for removing Bouncy Castle Dependency

`testnode-unprotected.pem` is simply the decrypted `testnode.pem`

```bash
    openssl rsa -in testnode.pem -out testnode-unprotected.pem
```

`rsa_key_pkcs8_plain.pem` is the same plaintext key encoded in `PKCS#8`

```bash
    openssl pkcs8 -topk8 -inform PEM -outform PEM -in testnode-unprotected.pem -out rsa_key_pkcs8_plain.pem -nocrypt
```

`testnode-aes{128,192,256}.pem` is the testnode.pem private key,
encrypted with `AES-128`, `AES-192` and `AES-256` respectively, encoded
in `PKCS#1`

```bash
    openssl rsa -aes128 -in testnode-unprotected.pem -out testnode-aes128.pem

    openssl rsa -aes192 -in testnode-unprotected.pem -out testnode-aes192.pem

    openssl rsa -aes256 -in testnode-unprotected.pem -out testnode-aes256.pem
```

# Adding `DSA` and `EC` Keys to the Keystore

```bash
    keytool -genkeypair -keyalg DSA -alias testnode_dsa -keystore testnode.jks -storepass testnode \
            -keypass testnode -validity 10000 -keysize 2048 -dname "CN=OpenSearch Test Node" \
            -ext SAN=dns:localhost,dns:localhost.localdomain,dns:localhost4,dns:localhost4.localdomain4,dns:localhost6,dns:localhost6.localdomain6,ip:127.0.0.1,ip:0:0:0:0:0:0:0:1

    keytool -genkeypair -keyalg EC -alias testnode_ec -keystore testnode.jks -storepass testnode \
            -keypass testnode -validity 10000 -groupname secp256r1 -dname "CN=OpenSearch Test Node" \
            -ext SAN=dns:localhost,dns:localhost.localdomain,dns:localhost4,dns:localhost4.localdomain4,dns:localhost6,dns:localhost6.localdomain6,ip:127.0.0.1,ip:0:0:0:0:0:0:0:1
```

# Export the `DSA` and `EC` private keys from `JKS` to `PKCS#12`

```bash
    keytool -importkeystore -srckeystore testnode.jks -destkeystore dsa.p12 -deststoretype PKCS12 \
            -srcalias testnode_dsa -deststorepass testnode -destkeypass testnode

    keytool -importkeystore -srckeystore testnode.jks -destkeystore ec.p12 -deststoretype PKCS12 \
            -srcalias testnode_ec -deststorepass testnode -destkeypass testnode
```

# Export the `DSA` and `EC` private keys from `PKCS#12` keystore into `PKCS#8` format

```bash
    openssl pkcs12 -in dsa.p12 -nodes -nocerts | openssl pkcs8 -topk8 -nocrypt -outform pem \
            -out dsa_key_pkcs8_plain.pem

    openssl pkcs12 -in dsa.p12 -nodes -nocerts | openssl pkcs8 -topk8 -outform pem \
            -out dsa_key_pkcs8_encrypted.pem

    openssl pkcs12 -in ec.p12 -nodes -nocerts | openssl pkcs8 -topk8 -nocrypt -outform pem \
            -out ec_key_pkcs8_plain.pem

    openssl pkcs12 -in ec.p12 -nodes -nocerts | openssl pkcs8 -topk8 -outform pem \
            -out ec_key_pkcs8_encrypted.pem
```

# Export the `DSA` and `EC` private keys from `PKCS#12` keystore into `PKCS#1` format

```bash
    openssl pkcs12 -in dsa.p12 -nodes -nocerts | openssl dsa -out dsa_key_openssl_plain.pem

    openssl pkcs12 -in dsa.p12 -nodes -nocerts | openssl dsa -des3 -out dsa_key_openssl_encrypted.pem

    openssl pkcs12 -in ec.p12 -nodes -nocerts | openssl ec -out ec_key_openssl_plain.pem

    openssl pkcs12 -in ec.p12 -nodes -nocerts | openssl ec -des3 -out ec_key_openssl_encrypted.pem
```

# Create SSH key
```bash
    ssh-keygen -t ed25519 -f key_unsupported.pem
```

# Generate the keys and self-signed certificates in `nodes/self/` :
```bash
    openssl req -newkey rsa:2048 -keyout n1.c1.key -x509 -days 3650 -subj "/CN=n1.c1" -reqexts SAN \
            -extensions SAN -config <(cat /etc/ssl/openssl.cnf \
            <(printf "[SAN]\nsubjectAltName=otherName.1:2.5.4.3;UTF8:node1.cluster1")) -out n1.c1.crt
```

# Create a `CA` keypair for testing
```bash
    openssl req -newkey rsa:2048 -nodes -keyout ca.key -x509 -subj "/CN=certAuth" -days 10000 -out ca.crt
```

# Generate Certificates signed with our CA for testing
```bash
    openssl req -new -newkey rsa:2048 -keyout n2.c2.key -reqexts SAN -extensions SAN \
             -config <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=otherName.1:2.5.4.3;UTF8:node2.cluster2"))\
             -out n2.c2.csr
    openssl x509 -req -in n2.c2.csr -extensions SAN -CA ca.crt -CAkey ca.key -CAcreateserial \
           -extfile <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=otherName.1:2.5.4.3;UTF8:node2.cluster2"))\
           -out n2.c2.crt -days 10000
```

# Generate EC keys using various curves for testing
```bash
    openssl ecparam -list_curves
```

will list all the available curves in a given system. For the purposes
of the tests here, the following curves were used to generate ec keys
named accordingly:
```bash
    openssl ecparam -name secp256r1 -genkey -out private_secp256r1.pem
    openssl ecparam -name secp384r1 -genkey -out private_secp384r1.pem
    openssl ecparam -name secp521r1 -genkey -out private_secp521r1.pem
```

and the respective certificates
```bash
    openssl req -x509 -extensions v3_req -key private_secp256r1.pem -out certificate_secp256r1.pem -days 1460 -config openssl_config.cnf
    openssl req -x509 -extensions v3_req -key private_secp384r1.pem -out certificate_secp384r1.pem -days 1460 -config openssl_config.cnf
    openssl req -x509 -extensions v3_req -key private_secp521r1.pem -out certificate_secp521r1.pem -days 1460 -config openssl_config.cnf
```

# Generate encrypted keys with `PBKDF2` standard

## RSA PKCS#8
```bash
    openssl genrsa -out key-temp.pem 2048
    openssl pkcs8 -in key-temp.pem -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA512 -out key_PKCS8_enc_pbkdf2.pem
```

## DSA
```bash
    openssl genpkey -genparam -algorithm DSA -out param_temp.pem -pkeyopt pbits:2048 -pkeyopt qbits:224 -pkeyopt digest:SHA256 -pkeyopt gindex:1 -text
    openssl genpkey -paramfile param_temp.pem -out key_DSA_enc_pbkdf2.pem -aes256 -pass stdin
```

## EC
```bash
    openssl genpkey -algorithm EC -out key_EC_enc_pbkdf2.pem -pkeyopt ec_paramgen_curve:secp384r1 -pkeyopt ec_param_enc:named_curve -pass stdin
```
