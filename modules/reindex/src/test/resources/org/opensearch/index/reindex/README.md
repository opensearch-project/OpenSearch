# generate self-signed CA key + cert
```bash
export KEY_PW='6!6428DQXwPpi7@$ggeg/='
openssl genpkey -algorithm RSA -out ca.key -aes256 -pass pass:"$KEY_PW"
openssl req -x509 -key ca.key -sha256 -days 3650 -subj "/CN=OpenSearch Test Node" -passin pass:"$KEY_PW" \
    -addext "subjectAltName=DNS:localhost,DNS:localhost.localdomain,DNS:localhost4,DNS:localhost4.localdomain4,DNS:localhost6,DNS:localhost6.localdomain6,IP:127.0.0.1,IP:0:0:0:0:0:0:0:1" \
    -out ca.pem
```
# generate client key + cert
```bash
export NAME='client'
openssl genpkey -algorithm RSA -out "$NAME".key -aes256 -pass pass:"$KEY_PW"
openssl req -new \
    -key "$NAME".key \
    -subj "/C=CA/ST=ONTARIO/L=TORONTO/O=ORG/OU=UNIT/CN=localhost" \
    -out "$NAME".csr \
    -passin pass:"$KEY_PW"
openssl x509 -req \
    -in "$NAME".csr \
    -CA ../ca.pem \
    -CAkey ../ca.key \
    -CAcreateserial \
    -out "$NAME".crt \
    -days 3650 \
    -sha256 \
    -passin pass:"$KEY_PW"
rm "$NAME".csr
```
# repeat the same for server key + cert
```bash
export NAME='http'
openssl genpkey -algorithm RSA -out "$NAME".key -aes256 -pass pass:"$KEY_PW"
openssl req -new \
    -key "$NAME".key \
    -subj "/C=CA/ST=ONTARIO/L=TORONTO/O=ORG/OU=UNIT/CN=localhost" \
    -out "$NAME".csr \
    -passin pass:"$KEY_PW"
openssl x509 -req \
    -in "$NAME".csr \
    -CA ../ca.pem \
    -CAkey ../ca.key \
    -CAcreateserial \
    -out "$NAME".crt \
    -days 3650 \
    -sha256 \
    -passin pass:"$KEY_PW"
rm "$NAME".csr
```
