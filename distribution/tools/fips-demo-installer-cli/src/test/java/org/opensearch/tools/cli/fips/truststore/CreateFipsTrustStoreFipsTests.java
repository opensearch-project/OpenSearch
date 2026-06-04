/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

public class CreateFipsTrustStoreFipsTests extends CreateFipsTrustStoreTests {

    public void testConvertToBCFKS() throws Exception {
        // given
        KeyStore sourceKeyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);
        assertTrue("Source keystore should have certificates", sourceKeyStore.size() > 0);

        CommonOptions options = new CommonOptions();
        options.force = false;
        String password = "testPassword123";

        // when
        Path result = CreateFipsTrustStore.convertToBCFKS(spec, sourceKeyStore, options, password, confDir);

        // then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(result.toString().endsWith("opensearch-fips-truststore.bcfks"));

        // Verify the converted keystore has the same certificates
        KeyStore bcfksStore = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var is = Files.newInputStream(result)) {
            bcfksStore.load(is, password.toCharArray());
        }
        assertEquals("Converted keystore should have same number of certificates", sourceKeyStore.size(), bcfksStore.size());
    }

    public void testConvertToBCFKSFileExistsWithoutForce() throws Exception {
        // given
        KeyStore sourceKeyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);
        assertTrue("Source keystore should have certificates", sourceKeyStore.size() > 0);

        CommonOptions options = new CommonOptions();
        options.force = false;
        String password = "testPassword123";

        // Create file first to simulate existing truststore
        Path trustStorePath = confDir.resolve("opensearch-fips-truststore.bcfks");
        Files.createFile(trustStorePath);

        assertTrue("Test setup: file should exist", Files.exists(trustStorePath));

        // when/then
        RuntimeException exception = expectThrows(
            RuntimeException.class,
            () -> CreateFipsTrustStore.convertToBCFKS(spec, sourceKeyStore, options, password, confDir)
        );
        assertEquals("Operation cancelled. Trust store file already exists.", exception.getMessage());
    }

    public void testConvertToBCFKSFileExistsWithForce() throws Exception {
        // given
        KeyStore sourceKeyStore = CreateFipsTrustStore.loadJvmDefaultTrustStore(spec, JAVA_HOME);
        assertTrue("Source keystore should have certificates", sourceKeyStore.size() > 0);

        CommonOptions options = new CommonOptions();
        options.force = true;
        String password = "testPassword123";

        // Create file first
        Path trustStorePath = confDir.resolve("opensearch-fips-truststore.bcfks");
        Files.createFile(trustStorePath);

        assertTrue(Files.exists(trustStorePath));

        // when
        Path result = CreateFipsTrustStore.convertToBCFKS(spec, sourceKeyStore, options, password, confDir);

        // then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the converted keystore has actual certificates
        KeyStore bcfksStore = KeyStore.getInstance("BCFKS", "BCFIPS");
        try (var is = Files.newInputStream(result)) {
            bcfksStore.load(is, password.toCharArray());
        }
        assertTrue("Converted keystore should have certificates", bcfksStore.size() > 0);
        assertEquals("Converted keystore should have same number of certificates", sourceKeyStore.size(), bcfksStore.size());
    }

}
