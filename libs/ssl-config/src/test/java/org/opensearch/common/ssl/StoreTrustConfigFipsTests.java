/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import org.hamcrest.Matchers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.Matchers.containsString;

public class StoreTrustConfigFipsTests extends StoreTrustConfigTests {

    protected static final char[] BCFKS_PASS = "bcfks-pass".toCharArray();
    protected static final String BCFKS = "BCFKS";

    public void testBuildTrustConfigFromP12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testBuildTrustConfigFromJks() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testBadKeyStoreFormatFails() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testIncorrectPasswordFailsForP12WithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testMissingTrustEntriesFailsForJksKeystoreWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testMissingTrustEntriesFailsForP12KeystoreWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testTrustConfigReloadsKeysStoreContentsForP12Keystore() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testBuildTrustConfigFromBcfks() throws Exception {
        final Path ks = getDataPath("/certs/ca-all/ca.bcfks");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, BCFKS_PASS, BCFKS, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    public void testIncorrectPasswordFailsForBcfksWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(
            ks,
            randomAlphaOfLengthBetween(6, 8).toCharArray(),
            BCFKS,
            DEFAULT_ALGORITHM
        );
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertCannotCreatePasswordManager(trustConfig, ks);
    }

    public void testMissingTrustEntriesFailsForBcfksKeystoreWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, BCFKS_PASS, BCFKS, DEFAULT_ALGORITHM);
        assertThat(trustConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoCertificateEntries(trustConfig, ks);
    }

    public void testTrustConfigReloadsKeysStoreContentsForBcfksKeystore() throws Exception {
        final Path ks1 = getDataPath("/certs/ca1/ca.bcfks");
        final Path ksAll = getDataPath("/certs/ca-all/ca.bcfks");

        final Path ks = createTempFile("ca", ".bcfks");

        final StoreTrustConfig trustConfig = new StoreTrustConfig(ks, BCFKS_PASS, BCFKS, DEFAULT_ALGORITHM);

        Files.copy(ks1, ks, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1");

        Files.delete(ks);
        assertFileNotFound(trustConfig, ks);

        Files.write(ks, randomByteArrayOfLength(128), StandardOpenOption.CREATE);
        assertInvalidFileFormat(trustConfig, ks);

        Files.copy(ksAll, ks, StandardCopyOption.REPLACE_EXISTING);
        assertCertificateChain(trustConfig, "CN=Test CA 1", "CN=Test CA 2", "CN=Test CA 3");
    }

    protected void assertMissingCertificateEntries(StoreTrustConfig trustConfig, Path key) {
        final SslConfigException exception = expectThrows(SslConfigException.class, trustConfig::createTrustManager);
        assertThat(exception.getMessage(), containsString(key.toAbsolutePath().toString()));
        assertThat(exception.getMessage(), containsString("keystore password was incorrect"));
    }

}
