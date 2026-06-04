/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.cli.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyStoreException;

@SuppressForbidden(reason = "the java.io.File is exposed by TemporaryFolder")
public class FipsTrustStoreValidatorTests extends OpenSearchTestCase {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    public void testTrustStoreWithoutConfiguration() throws Exception {
        var exception = expectThrows(IllegalStateException.class, () -> FipsTrustStoreValidator.validate("", "", "", ""));

        assertEquals(
            "Trust store type must be specified using the '-Djavax.net.ssl.trustStoreType' JVM option. Accepted values are PKCS11 and BCFKS.",
            exception.getMessage()
        );
    }

    public void testValidateAcceptsPKCS11() throws Exception {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> FipsTrustStoreValidator.validate("", "PKCS11", "NonExistentPKCS11Provider", "")
        );

        // Proves PKCS11 is accepted as from valid type (didn't throw "must be PKCS11 or BCFKS")
        assertTrue(exception.getMessage().contains("Trust store provider not available"));
    }

    public void testTrustStoreWithInvalidType() throws Exception {
        var exception = expectThrows(
            IllegalStateException.class,
            () -> FipsTrustStoreValidator.validate("test-truststore.p12", "PKCS12", "SUN", "changeit")
        );

        assertTrue(exception.getMessage().contains("Trust store type must be PKCS11 or BCFKS for FIPS compliance"));
    }

    public void testBCFKSWithMissingProperties() {
        {
            var exception = expectThrows(IllegalStateException.class, () -> FipsTrustStoreValidator.validate("", "BCFKS", "BCFIPS", ""));
            assertEquals(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\njavax.net.ssl.trustStore",
                exception.getMessage()
            );
        }

        {
            var exception = expectThrows(IllegalStateException.class, () -> FipsTrustStoreValidator.validate("  ", "BCFKS", "BCFIPS", ""));
            assertEquals(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\njavax.net.ssl.trustStore",
                exception.getMessage()
            );
        }

        {
            var exception = expectThrows(IllegalStateException.class, () -> FipsTrustStoreValidator.validate("/path", "BCFKS", "", ""));
            assertEquals(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\njavax.net.ssl.trustStoreProvider",
                exception.getMessage()
            );
        }
    }

    public void testPKCS11WithMissingProperties() {
        {
            var exception = expectThrows(IllegalStateException.class, () -> FipsTrustStoreValidator.validate("", "PKCS11", "", ""));
            assertEquals(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\njavax.net.ssl.trustStoreProvider",
                exception.getMessage()
            );
        }

        {
            var exception = expectThrows(IllegalStateException.class, () -> FipsTrustStoreValidator.validate("", "PKCS11", "  ", ""));
            assertEquals(
                "FIPS trust store is not set-up. Cannot find following JRE properties:\njavax.net.ssl.trustStoreProvider",
                exception.getMessage()
            );
        }
    }

    public void testFileDoesNotExist() throws IOException {
        var nonExistentPath = tempDir.newFolder().toPath().resolve("non-existent-truststore.p12");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> FipsTrustStoreValidator.validate(nonExistentPath.toString(), "BCFKS", "BCFIPS", "changeit")
        );

        assertTrue(exception.getMessage().contains("Trust store file does not exist"));
        assertTrue(exception.getMessage().contains(nonExistentPath.toString()));
    }

    public void testEmptyTrustStore() throws Exception {
        var emptyFile = tempDir.newFolder().toPath().resolve("empty-truststore.p12");
        Files.createFile(emptyFile);

        var exception = expectThrows(
            IllegalStateException.class,
            () -> FipsTrustStoreValidator.validate(emptyFile.toString(), "BCFKS", "BCFIPS", "changeit")
        );

        assertTrue(exception.getMessage().contains("Trust store file is empty"));
        assertTrue(exception.getMessage().contains(emptyFile.toString()));
    }

    public void testTrustStoreWithInvalidProvider() throws Exception {
        var trustStorePath = tempDir.newFolder().toPath().resolve("test-truststore.bcfks");
        var password = "changeit";
        Files.write(trustStorePath, new byte[100]); // Create a dummy file

        var exception = expectThrows(
            IllegalStateException.class,
            () -> FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "NON_EXISTENT_PROVIDER", password)
        );

        assertTrue(exception.getMessage().contains("Trust store provider not available"));
        assertTrue(exception.getMessage().contains("NON_EXISTENT_PROVIDER"));
    }

    public void testWithWrongProvider() throws Exception {
        var trustStorePath = tempDir.newFolder().toPath().resolve("test-truststore.bcfks");
        Files.write(trustStorePath, new byte[100]); // Create a dummy file

        var exception = expectThrows(
            IllegalStateException.class,
            () -> FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "SUN", "changeit")
        );

        assertTrue(exception.getMessage().contains("Invalid trust store type or provider"));
        assertTrue(exception.getCause() instanceof KeyStoreException);
    }

}
