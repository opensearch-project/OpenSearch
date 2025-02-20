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
import org.hamcrest.Matchers;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import static org.opensearch.common.crypto.KeyStoreType.BCFKS;
import static org.opensearch.common.crypto.KeyStoreType.JKS;
import static org.opensearch.common.crypto.KeyStoreType.PKCS_12;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StoreKeyConfigTests extends OpenSearchTestCase {

    private static final int IP_NAME = 7;
    private static final int DNS_NAME = 2;

    private static final char[] P12_PASS = "p12-pass".toCharArray();
    private static final char[] JKS_PASS = "jks-pass".toCharArray();
    private static final char[] BCFKS_PASS = "bcfks-pass".toCharArray();

    public void testLoadSingleKeyPKCS12() throws Exception {
        assumeFalse("Can't use PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path p12 = getDataPath("/certs/cert1/cert1.p12");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(p12, P12_PASS, PKCS_12, P12_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(p12));
        assertKeysLoaded(keyConfig, "cert1");
    }

    public void testLoadMultipleKeyPKCS12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path p12 = getDataPath("/certs/cert-all/certs.p12");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(p12, P12_PASS, PKCS_12, P12_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(p12));
        assertKeysLoaded(keyConfig, "cert1", "cert2");
    }

    public void testLoadMultipleKeyJksWithSeparateKeyPassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path jks = getDataPath("/certs/cert-all/certs.jks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(
            jks,
            JKS_PASS,
            JKS,
            "key-pass".toCharArray(),
            KeyManagerFactory.getDefaultAlgorithm()
        );
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(jks));
        assertKeysLoaded(keyConfig, "cert1", "cert2");
    }

    public void testLoadMultipleKeyBcfks() throws CertificateParsingException {
        final Path bcfks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(bcfks, BCFKS_PASS, BCFKS, BCFKS_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(bcfks));
        assertKeysLoaded(keyConfig, "cert1", "cert2");
    }

    public void testKeyManagerFailsWithIncorrectJksStorePassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path jks = getDataPath("/certs/cert-all/certs.jks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(
            jks,
            P12_PASS,
            JKS,
            "key-pass".toCharArray(),
            KeyManagerFactory.getDefaultAlgorithm()
        );
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(jks));
        assertPasswordIsIncorrect(keyConfig, jks);
    }

    public void testKeyManagerFailsWithIncorrectBcfksStorePassword() throws Exception {
        final Path bcfks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(bcfks, P12_PASS, BCFKS, BCFKS_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(bcfks));
        assertPasswordIsIncorrect(keyConfig, bcfks);
    }

    public void testKeyManagerFailsWithIncorrectJksKeyPassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path jks = getDataPath("/certs/cert-all/certs.jks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(jks, JKS_PASS, JKS, JKS_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(jks));
        assertPasswordIsIncorrect(keyConfig, jks);
    }

    public void testKeyManagerFailsWithIncorrectBcfksKeyPassword() throws Exception {
        final Path bcfks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(
            bcfks,
            BCFKS_PASS,
            BCFKS,
            "nonsense".toCharArray(),
            KeyManagerFactory.getDefaultAlgorithm()
        );
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(bcfks));
        assertPasswordIsIncorrect(keyConfig, bcfks);
    }

    public void testKeyManagerFailsWithMissingKeystoreFile() throws Exception {
        final Path path = getDataPath("/certs/cert-all/certs.jks").getParent().resolve("dne.jks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(path, JKS_PASS, JKS, JKS_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(path));
        assertFileNotFound(keyConfig, path);
    }

    public void testMissingKeyEntriesFailsForJksWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path ks = getDataPath("/certs/ca-all/ca.jks");
        final char[] password = JKS_PASS;
        final StoreKeyConfig keyConfig = new StoreKeyConfig(ks, password, JKS, password, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoPrivateKeyEntries(keyConfig, ks);
    }

    public void testMissingKeyEntriesFailsForP12WithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path ks = getDataPath("/certs/ca-all/ca.p12");
        final char[] password = P12_PASS;
        final StoreKeyConfig keyConfig = new StoreKeyConfig(ks, password, PKCS_12, password, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoPrivateKeyEntries(keyConfig, ks);
    }

    public void testMissingKeyEntriesFailsForBcfksWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/ca-all/ca.bcfks");
        final char[] password = BCFKS_PASS;
        final StoreKeyConfig keyConfig = new StoreKeyConfig(ks, password, BCFKS, password, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoPrivateKeyEntries(keyConfig, ks);
    }

    public void testKeyConfigReloadsFileContents() throws Exception {
        assumeFalse("Can't use PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Path cert1 = getDataPath("/certs/cert1/cert1.p12");
        final Path cert2 = getDataPath("/certs/cert2/cert2.p12");
        final Path jks = getDataPath("/certs/cert-all/certs.jks");

        final Path p12 = createTempFile("cert", ".p12");

        final StoreKeyConfig keyConfig = new StoreKeyConfig(p12, P12_PASS, PKCS_12, P12_PASS, KeyManagerFactory.getDefaultAlgorithm());

        Files.copy(cert1, p12, StandardCopyOption.REPLACE_EXISTING);
        assertKeysLoaded(keyConfig, "cert1");
        assertKeysNotLoaded(keyConfig, "cert2");

        Files.copy(jks, p12, StandardCopyOption.REPLACE_EXISTING);
        // Because (a) cannot load a JKS as a PKCS12 & (b) the password is wrong.
        assertBadKeyStore(keyConfig, p12);

        Files.copy(cert2, p12, StandardCopyOption.REPLACE_EXISTING);
        assertKeysLoaded(keyConfig, "cert2");
        assertKeysNotLoaded(keyConfig, "cert1");

        Files.delete(p12);
        assertFileNotFound(keyConfig, p12);
    }

    private void assertKeysLoaded(StoreKeyConfig keyConfig, String... names) throws CertificateParsingException {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        assertThat(keyManager, notNullValue());

        for (String name : names) {
            final PrivateKey privateKey = keyManager.getPrivateKey(name);
            assertThat(privateKey, notNullValue());
            assertThat(privateKey.getAlgorithm(), is("RSA"));

            final X509Certificate[] chain = keyManager.getCertificateChain(name);
            assertThat(chain, notNullValue());
            assertThat(chain, arrayWithSize(1));
            final X509Certificate certificate = chain[0];
            assertThat(certificate.getIssuerDN().getName(), is("CN=Test CA 1"));
            assertThat(certificate.getSubjectDN().getName(), is("CN=" + name));
            assertThat(certificate.getSubjectAlternativeNames(), iterableWithSize(2));
            assertThat(
                certificate.getSubjectAlternativeNames(),
                containsInAnyOrder(Arrays.asList(DNS_NAME, "localhost"), Arrays.asList(IP_NAME, "127.0.0.1"))
            );
        }
    }

    private void assertKeysNotLoaded(StoreKeyConfig keyConfig, String... names) throws CertificateParsingException {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        assertThat(keyManager, notNullValue());

        for (String name : names) {
            final PrivateKey privateKey = keyManager.getPrivateKey(name);
            assertThat(privateKey, nullValue());
        }
    }

    private void assertPasswordIsIncorrect(StoreKeyConfig keyConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        if (exception.getCause() instanceof GeneralSecurityException) {
            assertThat(exception.getMessage(), containsString("password"));
        } else {
            assertThat(exception.getCause(), instanceOf(IOException.class));
            assertThat(
                exception.getCause().getMessage(),
                anyOf(containsString("Keystore was tampered with, or password was incorrect"), containsString("BCFKS KeyStore corrupted"))
            );
        }
    }

    private void assertBadKeyStore(StoreKeyConfig keyConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getCause(), instanceOf(IOException.class));
    }

    private void assertFileNotFound(StoreKeyConfig keyConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not exist"));
        assertThat(exception.getCause(), nullValue());
    }

    private void assertNoPrivateKeyEntries(StoreKeyConfig keyConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, keyConfig::createKeyManager);
        assertThat(exception.getMessage(), containsString("keystore"));
        assertThat(exception.getMessage(), containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not contain a private key entry"));
    }
}
