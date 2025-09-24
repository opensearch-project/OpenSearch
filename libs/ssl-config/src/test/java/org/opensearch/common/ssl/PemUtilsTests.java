/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.ssl;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.opensearch.test.BouncyCastleThreadFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.interfaces.DSAParams;
import java.security.interfaces.DSAPrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.util.Locale;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;

@ThreadLeakFilters(filters = BouncyCastleThreadFilter.class)
public class PemUtilsTests extends OpenSearchTestCase {

    private static final Supplier<char[]> EMPTY_PASSWORD = () -> new char[0];
    private static final Supplier<char[]> TESTNODE_PASSWORD = "testnode"::toCharArray;
    private static final Supplier<char[]> STRONG_PRIVATE_SECRET = "6!6428DQXwPpi7@$ggeg/="::toCharArray;

    public void testReadPKCS8RsaKey() throws Exception {
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("RSA");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/rsa_key_pkcs8_plain.pem"),
            EMPTY_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadPKCS8RsaKeyWithBagAttrs() throws Exception {
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("RSA");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/testnode_with_bagattrs.pem"),
            EMPTY_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadPKCS8DsaKey() throws Exception {
        var key = (DSAPrivateKey) getKeyFromKeystore("DSA");
        var privateKey = (DSAPrivateKey) PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_pkcs8_plain.pem"), EMPTY_PASSWORD);

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedPKCS8DsaKey() throws Exception {
        var key = (DSAPrivateKey) getKeyFromKeystore("DSA");
        var privateKey = (DSAPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/dsa_key_pkcs8_encrypted.pem"),
            STRONG_PRIVATE_SECRET
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEcKeyCurves() throws Exception {
        String curve = randomFrom("secp256r1", "secp384r1", "secp521r1");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/private_" + curve + ".pem"), EMPTY_PASSWORD);
        ECParameterSpec parameterSpec = ((ECPrivateKey) privateKey).getParams();
        ECGenParameterSpec algorithmParameterSpec = new ECGenParameterSpec(curve);
        AlgorithmParameters algoParameters = AlgorithmParameters.getInstance("EC");
        algoParameters.init(algorithmParameterSpec);
        assertThat(parameterSpec, equalTo(algoParameters.getParameterSpec(ECParameterSpec.class)));
    }

    public void testReadPKCS8EcKey() throws Exception {
        var key = (ECPrivateKey) getKeyFromKeystore("EC");
        var privateKey = (ECPrivateKey) PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_pkcs8_plain.pem"), EMPTY_PASSWORD);

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedPKCS8EcKey() throws Exception {
        var key = (ECPrivateKey) getKeyFromKeystore("EC");
        var privateKey = (ECPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/ec_key_pkcs8_encrypted.pem"),
            STRONG_PRIVATE_SECRET
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedPKCS8Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("RSA");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/key_pkcs8_encrypted.pem"),
            TESTNODE_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadDESEncryptedPKCS1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("RSA");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode.pem"), TESTNODE_PASSWORD);

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadAESEncryptedPKCS1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("RSA");
        String bits = randomFrom("128", "192", "256");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/testnode-aes" + bits + ".pem"),
            TESTNODE_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadPKCS1RsaKey() throws Exception {
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("RSA");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/testnode-unprotected.pem"),
            TESTNODE_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadOpenSslDsaKey() throws Exception {
        var key = (DSAPrivateKey) getKeyFromKeystore("DSA");
        var privateKey = (DSAPrivateKey) PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_openssl_plain.pem"), EMPTY_PASSWORD);

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadOpenSslDsaKeyWithParams() throws Exception {
        var key = (DSAPrivateKey) getKeyFromKeystore("DSA");
        var privateKey = (DSAPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/dsa_key_openssl_plain_with_params.pem"),
            EMPTY_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedOpenSslDsaKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        var key = (DSAPrivateKey) getKeyFromKeystore("DSA");
        var privateKey = (DSAPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/dsa_key_openssl_encrypted.pem"),
            TESTNODE_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadOpenSslEcKey() throws Exception {
        var key = (ECPrivateKey) getKeyFromKeystore("EC");
        var privateKey = (ECPrivateKey) PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_openssl_plain.pem"), EMPTY_PASSWORD);

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadOpenSslEcKeyWithParams() throws Exception {
        var key = (ECPrivateKey) getKeyFromKeystore("EC");
        var privateKey = (ECPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/ec_key_openssl_plain_with_params.pem"),
            EMPTY_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedOpenSslEcKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        var key = (ECPrivateKey) getKeyFromKeystore("EC");
        var privateKey = (ECPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/ec_key_openssl_encrypted.pem"),
            TESTNODE_PASSWORD
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedPKCS8KeyWithPBKDF2() throws Exception {
        var key = (RSAPrivateCrtKey) getKeyFromKeystore("PKCS8_PBKDF2");
        var privateKey = (RSAPrivateCrtKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/key_PKCS8_enc_pbkdf2.pem"),
            STRONG_PRIVATE_SECRET
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedDsaKeyWithPBKDF2() throws Exception {
        var key = (DSAPrivateKey) getKeyFromKeystore("DSA_PBKDF2");
        var privateKey = (DSAPrivateKey) PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/key_DSA_enc_pbkdf2.pem"),
            STRONG_PRIVATE_SECRET
        );

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadEncryptedEcKeyWithPBKDF2() throws Exception {
        var key = (ECPrivateKey) getKeyFromKeystore("EC_PBKDF2");
        var privateKey = (ECPrivateKey) PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/key_EC_enc_pbkdf2.pem"), EMPTY_PASSWORD);

        assertTrue(isCryptographicallyEqual(key, privateKey));
    }

    public void testReadUnsupportedKey() {
        final Path path = getDataPath("/certs/pem-utils/key_unsupported.pem");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("Error parsing Private Key"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
        assertThat(e.getMessage(), containsString("file is empty"));
    }

    public void testReadPemCertificateAsKey() {
        final Path path = getDataPath("/certs/pem-utils/testnode.crt");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("invalid encrypted private key class"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
    }

    public void testReadCorruptedKey() {
        final Path path = getDataPath("/certs/pem-utils/corrupted_key_pkcs8_plain.pem");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("Error parsing Private Key"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
        assertThat(e.getMessage(), containsString("file is empty"));
    }

    public void testReadEmptyFile() {
        final Path path = getDataPath("/certs/pem-utils/empty.pem");
        SslConfigException e = expectThrows(SslConfigException.class, () -> PemUtils.readPrivateKey(path, TESTNODE_PASSWORD));
        assertThat(e.getMessage(), containsString("file is empty"));
        assertThat(e.getMessage(), containsString(path.toAbsolutePath().toString()));
    }

    private Key getKeyFromKeystore(String algo) throws Exception {
        var keystorePath = getDataPath("/certs/pem-utils/testnode.jks");
        var alias = "testnode_" + algo.toLowerCase(Locale.ROOT);
        var password = "testnode".toCharArray();
        try (InputStream in = Files.newInputStream(keystorePath)) {
            KeyStore keyStore = KeyStore.getInstance("jks");
            keyStore.load(in, password);
            return keyStore.getKey(alias, password);
        }
    }

    private boolean isCryptographicallyEqual(ECPrivateKey key1, ECPrivateKey key2) throws IOException {
        var pki1 = PrivateKeyInfo.getInstance(key1.getEncoded());
        var pki2 = PrivateKeyInfo.getInstance(key2.getEncoded());

        var privateKey1 = org.bouncycastle.asn1.sec.ECPrivateKey.getInstance(pki1.parsePrivateKey()).getKey();
        var privateKey2 = org.bouncycastle.asn1.sec.ECPrivateKey.getInstance(pki2.parsePrivateKey()).getKey();

        var oid1 = ASN1ObjectIdentifier.getInstance(pki1.getPrivateKeyAlgorithm().getParameters());
        var oid2 = ASN1ObjectIdentifier.getInstance(pki2.getPrivateKeyAlgorithm().getParameters());

        return privateKey1.equals(privateKey2) && oid1.equals(oid2);
    }

    private boolean isCryptographicallyEqual(RSAPrivateCrtKey key1, RSAPrivateCrtKey key2) {
        return key1.getModulus().equals(key2.getModulus())
            && key1.getPrivateExponent().equals(key2.getPrivateExponent())
            && key1.getPublicExponent().equals(key2.getPublicExponent())
            && key1.getPrimeP().equals(key2.getPrimeP())
            && key1.getPrimeQ().equals(key2.getPrimeQ())
            && key1.getPrimeExponentP().equals(key2.getPrimeExponentP())
            && key1.getPrimeExponentQ().equals(key2.getPrimeExponentQ())
            && key1.getCrtCoefficient().equals(key2.getCrtCoefficient());
    }

    private boolean isCryptographicallyEqual(DSAPrivateKey key1, DSAPrivateKey key2) {
        if (!key1.getX().equals(key2.getX())) {
            return false;
        }

        DSAParams params1 = key1.getParams();
        DSAParams params2 = key2.getParams();

        return params1.getP().equals(params2.getP()) && params1.getQ().equals(params2.getQ()) && params1.getG().equals(params2.getG());
    }

}
