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

import org.opensearch.test.BouncyCastleThreadFilter;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import javax.net.ssl.X509ExtendedTrustManager;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ThreadLeakFilters(filters = BouncyCastleThreadFilter.class)
public class PemTrustConfigTests extends OpenSearchTestCase {

    public void testBuildTrustConfigFromSinglePemFile() throws Exception {
        final Path cert = getDataPath("/certs/ca1/ca.crt");
        final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(cert));
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert));
        assertCertificateChain(trustConfig, "CN=Test CA 1");
    }

    public void testBuildTrustConfigFromMultiplePemFiles() throws Exception {
        final Path cert1 = getDataPath("/certs/ca1/ca.crt");
        final Path cert2 = getDataPath("/certs/ca2/ca.crt");
        final Path cert3 = getDataPath("/certs/ca3/ca.crt");
        final PemTrustConfig trustConfig = new PemTrustConfig(Arrays.asList(cert1, cert2, cert3));
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert1, cert2, cert3));
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    public void testBadFileFormatFails() throws Exception {
        {
            final Path ca = createTempFile("ca", ".crt");
            Files.write(ca, "This is definitely not a PEM certificate".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
            final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(ca));
            assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ca));
            assertFailedToParse(trustConfig, ca);
        }

        {
            final Path ca = createTempFile("ca", ".crt");
            Files.write(ca, generateInvalidPemBytes(), StandardOpenOption.CREATE);
            final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(ca));
            assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ca));
            assertCannotCreateTrust(trustConfig, ca);
        }

        { // test DER-encoded sequence
            final Path ca = createTempFile("ca", ".crt");
            Files.write(ca, generateInvalidDerEncodedPemBytes(), StandardOpenOption.CREATE);
            final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(ca));
            assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ca));
            assertCannotCreateTrust(trustConfig, ca);
        }
    }

    public void testEmptyFileFails() throws Exception {
        final Path ca = createTempFile("ca", ".crt");
        final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(ca));
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ca));
        assertFailedToParse(trustConfig, ca);
    }

    public void testMissingFileFailsWithMeaningfulMessage() throws Exception {
        final Path cert = getDataPath("/certs/ca1/ca.crt").getParent().resolve("dne.crt");
        final PemTrustConfig trustConfig = new PemTrustConfig(Collections.singletonList(cert));
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert));
        assertFileNotFound(trustConfig, cert);
    }

    public void testOneMissingFileFailsWithMeaningfulMessageEvenIfOtherFileExist() throws Exception {
        final Path cert1 = getDataPath("/certs/ca1/ca.crt");
        final Path cert2 = getDataPath("/certs/ca2/ca.crt").getParent().resolve("dne.crt");
        final Path cert3 = getDataPath("/certs/ca3/ca.crt");
        final PemTrustConfig trustConfig = new PemTrustConfig(Arrays.asList(cert1, cert2, cert3));
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(cert1, cert2, cert3));
        assertFileNotFound(trustConfig, cert2);
    }

    public void testTrustConfigReloadsFileContents() throws Exception {
        final Path cert1 = getDataPath("/certs/ca1/ca.crt");
        final Path cert2 = getDataPath("/certs/ca2/ca.crt");
        final Path cert3 = getDataPath("/certs/ca3/ca.crt");

        final Path ca1 = createTempFile("ca1", ".crt");
        final Path ca2 = createTempFile("ca2", ".crt");

        final PemTrustConfig trustConfig = new PemTrustConfig(Arrays.asList(ca1, ca2));

        Files.copy(cert1, ca1, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(cert2, ca2, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2");

        Files.copy(cert3, ca2, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 3");

        Files.delete(ca1);
        assertFileNotFound(trustConfig, ca1);

        Files.write(ca1, generateInvalidPemBytes(), StandardOpenOption.CREATE);
        assertCannotCreateTrust(trustConfig, ca1);
    }

    private void assertCertificateChain(PemTrustConfig trustConfig, String... caNames) {
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        final X509Certificate[] issuers = trustManager.getAcceptedIssuers();
        final Set<String> issuerNames = Stream.of(issuers)
            .map(X509Certificate::getSubjectX500Principal)
            .map(Principal::getName)
            .collect(Collectors.toSet());

        assertThat(issuerNames, Matchers.containsInAnyOrder(caNames));
    }

    // The parser returns an empty collection when no valid sequence is found,
    // but our implementation requires an exception to be thrown in this case
    private void assertFailedToParse(PemTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        logger.info("failure", exception);
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), Matchers.containsString("Failed to parse any certificate from"));
    }

    // The parser encounters malformed PEM data
    private void assertCannotCreateTrust(PemTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        logger.info("failure", exception);
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), Matchers.containsString("cannot create trust using PEM certificates"));
    }

    private void assertFileNotFound(PemTrustConfig trustConfig, Path file) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), Matchers.containsString("files do not exist"));
        assertThat(exception.getMessage(), Matchers.containsString("PEM"));
        assertThat(exception.getMessage(), Matchers.containsString(file.toAbsolutePath().toString()));
        assertThat(exception.getCause(), Matchers.instanceOf(NoSuchFileException.class));
    }

    private byte[] generateInvalidPemBytes() {
        String invalidPem = "-----BEGIN CERTIFICATE-----\nINVALID_CONTENT\n-----END CERTIFICATE-----";
        return invalidPem.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] generateInvalidDerEncodedPemBytes() {
        byte[] shortFormZeroLength = { 0x30, 0x00 };
        byte[] longFormZeroLength = { 0x30, (byte) 0x81, 0x00 };
        byte[] indefiniteForm = { 0x30, (byte) 0x80, 0x01, 0x02, 0x00, 0x00 };
        return randomFrom(shortFormZeroLength, longFormZeroLength, indefiniteForm);
    }
}
