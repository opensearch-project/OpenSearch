# ca.p12


# ca.pem

openssl pkcs12 -info -in ./ca.p12 -nokeys -out ca.pem -passin "pass:ca-password"

# http.p12

unzip http.zip
rm http.zip

# client.p12

unzip client.zip
rm client.zip
