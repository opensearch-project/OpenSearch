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

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;

public class StoreTrustConfigTests extends OpenSearchTestCase {

    protected static final char[] P12_PASS = "p12-pass".toCharArray();
    protected static final char[] JKS_PASS = "jks-pass".toCharArray();
    protected static final String PKCS12 = "PKCS12";
    protected static final String JKS = "JKS";
    protected static final String DEFAULT_ALGORITHM = TrustManagerFactory.getDefaultAlgorithm();

    public void testBuildTrustConfigFromP12() throws Exception {
        final Path ks = getDataPath("/certs/ca1/ca.p12");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, P12_PASS, PKCS12, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertCertificateChain(trustConfig, "CN=Test CA 1");
    }

    public void testBuildTrustConfigFromJks() throws Exception {
        final Path ks = getDataPath("/certs/ca-all/ca.jks");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, JKS_PASS, JKS, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    public void testBadKeyStoreFormatFails() throws Exception {
        final Path ks = createTempFile("ca", ".p12");
        Files.write(ks, randomByteArrayOfLength(128), StandardOpenOption.APPEND);
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, new char[0], randomFrom(PKCS12, JKS), DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertInvalidFileFormat(trustConfig, ks);
    }

    public void testMissingKeyStoreFailsWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/ca-all/ca.p12").getParent().resolve("keystore.dne");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, new char[0], randomFrom(PKCS12, JKS), DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertFileNotFound(trustConfig, ks);
    }

    public void testIncorrectPasswordFailsForP12WithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/ca1/ca.p12");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, new char[0], PKCS12, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertPasswordIsIncorrect(trustConfig, ks);
    }

    public void testMissingTrustEntriesFailsForJksKeystoreWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/cert-all/certs.jks");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, JKS_PASS, JKS, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoCertificateEntries(trustConfig, ks);
    }

    public void testMissingTrustEntriesFailsForP12KeystoreWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/cert-all/certs.p12");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, P12_PASS, PKCS12, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertMissingCertificateEntries(trustConfig, ks);
    }

    public void testTrustConfigReloadsKeysStoreContentsForP12Keystore() throws Exception {
        final Path ks1 = getDataPath("/certs/ca1/ca.p12");
        final Path ksAll = getDataPath("/certs/ca-all/ca.p12");

        final Path ks = createTempFile("ca", ".p12");

        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, P12_PASS, PKCS12, DEFAULT_ALGORITHM);

        Files.copy(ks1, ks, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1");

        Files.delete(ks);
        assertFileNotFound(trustConfig, ks);

        Files.write(ks, randomByteArrayOfLength(128), StandardOpenOption.CREATE);
        assertInvalidFileFormat(trustConfig, ks);

        Files.copy(ksAll, ks, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    protected void assertCertificateChain(StoreTrustConfig trustConfig, String... caNames) {
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        final X509Certificate[] issuers = trustManager.getAcceptedIssuers();
        final Set<String> issuerNames = Stream.of(issuers)
            .map(X509Certificate::getSubjectX500Principal)
            .map(Principal::getName)
            .collect(Collectors.toSet());

        assertThat(issuerNames, Matchers.containsInAnyOrder(caNames));
    }

    protected void assertInvalidFileFormat(StoreTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), Matchers.containsString("cannot read"));
        assertThat(exception.getMessage(), Matchers.containsString("keystore"));
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
    }

    protected void assertFileNotFound(StoreTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), Matchers.containsString("file does not exist"));
        assertThat(exception.getMessage(), Matchers.containsString("keystore"));
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getCause(), nullValue());
    }

    protected void assertPasswordIsIncorrect(StoreTrustConfig trustConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("keystore password was incorrect"));
    }

    protected void assertCannotCreatePasswordManager(StoreTrustConfig trustConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("incorrect password or corrupt file"));
    }

    protected void assertNoCertificateEntries(StoreTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), Matchers.containsString("does not contain any trusted certificate entries"));
        assertThat(exception.getMessage(), Matchers.containsString("truststore"));
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
    }

    protected void assertMissingCertificateEntries(StoreTrustConfig trustConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("does not contain any trusted certificate entries"));
    }

}
