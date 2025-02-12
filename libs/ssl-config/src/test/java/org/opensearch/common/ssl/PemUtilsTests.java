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

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AlgorithmParameters;
import java.security.Key;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;
import java.util.Locale;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.StringContains.containsString;

public class PemUtilsTests extends OpenSearchTestCase {

    private static final Supplier<char[]> EMPTY_PASSWORD = () -> new char[0];
    private static final Supplier<char[]> TESTNODE_PASSWORD = "testnode"::toCharArray;
    private static final Supplier<char[]> STRONG_PRIVATE_SECRET = "6!6428DQXwPpi7@$ggeg/="::toCharArray; // has to be at least 112 bit long.

    public void testInstantiateWithDefaultConstructor() {
        assertThrows("Utility class should not be instantiated", IllegalStateException.class, PemUtils::new);
    }

    public void testReadPKCS8RsaKey() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("RSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/rsa_key_pkcs8_plain.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8RsaKeyWithBagAttrs() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("RSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode_with_bagattrs.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS8DsaKey() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("DSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_pkcs8_plain.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEcKeyCurves() throws Exception {
        String curve = randomFrom("secp256r1", "secp384r1", "secp521r1");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/private_" + curve + ".pem"), ""::toCharArray);
        assertThat(privateKey, instanceOf(ECPrivateKey.class));
        ECParameterSpec parameterSpec = ((ECPrivateKey) privateKey).getParams();
        ECGenParameterSpec algorithmParameterSpec = new ECGenParameterSpec(curve);
        AlgorithmParameters algoParameters = AlgorithmParameters.getInstance("EC");
        algoParameters.init(algorithmParameterSpec);
        assertThat(parameterSpec, equalTo(algoParameters.getParameterSpec(ECParameterSpec.class)));
    }

    public void testReadPKCS8EcKey() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("EC", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_pkcs8_plain.pem"), EMPTY_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedPKCS8Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("RSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/key_pkcs8_encrypted.pem"), TESTNODE_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadDESEncryptedPKCS1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("RSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode.pem"), TESTNODE_PASSWORD);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadAESEncryptedPKCS1Key() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("RSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        String bits = randomFrom("128", "192", "256");
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode-aes" + bits + ".pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadPKCS1RsaKey() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("RSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/testnode-unprotected.pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslDsaKey() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("DSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_openssl_plain.pem"), EMPTY_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslDsaKeyWithParams() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("DSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/dsa_key_openssl_plain_with_params.pem"),
            EMPTY_PASSWORD
        );

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedOpenSslDsaKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("DSA", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/dsa_key_openssl_encrypted.pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslEcKey() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("EC", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_openssl_plain.pem"), EMPTY_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadOpenSslEcKeyWithParams() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        Key key = getKeyFromKeystore("EC", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(
            getDataPath("/certs/pem-utils/ec_key_openssl_plain_with_params.pem"),
            EMPTY_PASSWORD
        );

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedOpenSslEcKey() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBKDF-OPENSSL KeySpec is not available", inFipsJvm());
        Key key = getKeyFromKeystore("EC", KeyStoreType.JKS);
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/ec_key_openssl_encrypted.pem"), TESTNODE_PASSWORD);

        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedPKCS8KeyWithPBKDF2() throws Exception {
        Key key = getKeyFromKeystore("PKCS8_PBKDF2");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/key_PKCS8_enc_pbkdf2.pem"), STRONG_PRIVATE_SECRET);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedDsaKeyWithPBKDF2() throws Exception {
        Key key = getKeyFromKeystore("DSA_PBKDF2");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/key_DSA_enc_pbkdf2.pem"), STRONG_PRIVATE_SECRET);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
    }

    public void testReadEncryptedEcKeyWithPBKDF2() throws Exception {
        Key key = getKeyFromKeystore("EC_PBKDF2");
        assertThat(key, notNullValue());
        assertThat(key, instanceOf(PrivateKey.class));
        PrivateKey privateKey = PemUtils.readPrivateKey(getDataPath("/certs/pem-utils/key_EC_enc_pbkdf2.pem"), STRONG_PRIVATE_SECRET);
        assertThat(privateKey, notNullValue());
        assertThat(privateKey, equalTo(key));
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
        return getKeyFromKeystore(algo, inFipsJvm() ? KeyStoreType.BCFKS : KeyStoreType.JKS);
    }

    private Key getKeyFromKeystore(String algo, KeyStoreType keyStoreType) throws Exception {
        var keystorePath = getDataPath("/certs/pem-utils/testnode" + KeyStoreType.TYPE_TO_EXTENSION_MAP.get(keyStoreType).get(0));
        var alias = "testnode_" + algo.toLowerCase(Locale.ROOT);
        var password = "testnode".toCharArray();
        try (var in = Files.newInputStream(keystorePath)) {
            var keyStore = KeyStoreFactory.getInstance(keyStoreType);
            keyStore.load(in, password);
            return keyStore.getKey(alias, password);
        }
    }
}
