All openssl commands use the same configuration file, albeit different sections of it. The OpenSSL Configuration file is located in this directory as `openssl_config.cnf`.

# Instructions on generating self-signed Root CA

The self-signed Root CA, *testRootCA.crt*, and its associated private key in this directory have been generated using the following openssl commands.

    openssl genrsa -out testRootCA.key 2048
    openssl req -x509 -new -key testRootCA.key -days 1460 -subj "/CN=OpenSearch Test Root CA/OU=opensearch/O=org" -out testRootCA.crt -config ./openssl_config.cnf

# Instructions on generating the Intermediate CA

The `testIntermediateCA.crt` CA certificate is "issued" by the `testRootCA.crt`.

    openssl genrsa -out testIntermediateCA.key 2048
    openssl req -new -key testIntermediateCA.key -subj "/CN=OpenSearch Test Intermediate CA/OU=OpenSearch/O=org" -out testIntermediateCA.csr -config ./openssl_config.cnf
    openssl x509 -req -in testIntermediateCA.csr -CA testRootCA.crt -CAkey testRootCA.key -CAcreateserial -out testIntermediateCA.crt -days 1460 -sha256 -extensions v3_ca -extfile ./openssl_config.cnf

# Instructions on generating the Client Certificate

The `testClient.crt` end entity certificate is "issued" by the `testIntermediateCA.crt`.

    openssl genrsa -out testClient.key 2048
    openssl req -new -key testClient.key -subj "/CN=OpenSearch Test Client/OU=OpenSearch/O=org" -out testClient.csr -config ./openssl_config.cnf
    openssl x509 -req -in testClient.csr -CA testIntermediateCA.crt -CAkey testIntermediateCA.key -CAcreateserial -out testClient.crt -days 1460 -sha256 -extensions usr_cert -extfile ./openssl_config.cnf
