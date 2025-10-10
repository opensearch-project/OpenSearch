# generate RSA 1024-bit certificate
```bash
openssl req -new -x509 -days 3650 -nodes -out cert1024.pem -keyout key1024.pem \
    -subj "/C=CA/ST=ONTARIO/L=TORONTO/O=ORG/OU=UNIT/CN=localhost" -newkey rsa:1024
```

# generate certificate with MD5 signature
```bash
openssl req -new -x509 -days 3650 -nodes -out certmd5.pem -keyout keymd5.pem \
    -subj "/C=CA/ST=ONTARIO/L=TORONTO/O=ORG/OU=UNIT/CN=localhost" -md5
```

# generate certificate with a weak curve
```bash
openssl ecparam -genkey -name secp112r1 -noout -out keyecc.pem
openssl req -new -x509 -key keyecc.pem -out certecc.pem -days 3650 \
    -subj "/C=CA/ST=ONTARIO/L=TORONTO/O=ORG/OU=UNIT/CN=localhost"
```
