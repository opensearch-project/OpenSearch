/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.cli.SuppressForbidden;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;
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

    public void testValidateAcceptsBCFKSType() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        var trustStoreResource = getClass().getResource("/org/opensearch/bootstrap/truststore/opensearch-fips-truststore.bcfks");
        assertNotNull(trustStoreResource);
        var trustStorePath = Path.of(trustStoreResource.toURI());

        // Should not throw exception
        FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "BCFIPS", "notarealpasswordphrase");
    }

    public void testBCFKSEmptyTrustStoreWarning() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        var trustStorePath = tempDir.newFolder().toPath().resolve("empty_trust_store.bcfks");
        var password = "testPassword";

        var keyStore = java.security.KeyStore.getInstance("BCFKS", "BCFIPS");
        keyStore.load(null, password.toCharArray());

        try (var fos = Files.newOutputStream(trustStorePath)) {
            keyStore.store(fos, password.toCharArray());
        }

        FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "BCFIPS", password);
    }

    public void testTrustStoreWithWrongPassword() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        var trustStoreResource = getClass().getResource("/org/opensearch/bootstrap/truststore/opensearch-fips-truststore.bcfks");
        assertNotNull("FIPS truststore resource not found", trustStoreResource);
        var trustStorePath = Path.of(trustStoreResource.toURI());

        var exception = expectThrows(
            java.io.UncheckedIOException.class,
            () -> FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "BCFIPS", "wrongPassword")
        );

        assertTrue(exception.getMessage().contains("possibly wrong password"));
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

    public void testWithoutReadPermission() throws Exception {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());
        assumeTrue("requires POSIX file permissions", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        var tempBcfksFile = Files.createTempFile(createTempDir(), "no-write", ".bcfks");
        Files.setPosixFilePermissions(tempBcfksFile, PosixFilePermissions.fromString("---------"));

        try {
            var keyStore = KeyStore.getInstance("BCFKS", "BCFIPS");
            keyStore.load(null, "changeit".toCharArray());

            var throwable = assertThrows(
                IllegalStateException.class,
                () -> FipsTrustStoreValidator.validate(tempBcfksFile.toAbsolutePath().toString(), "BCFKS", "BCFIPS", "changeit")
            );
            assertTrue(throwable.getMessage().startsWith("Trust store file is not readable: " + tempBcfksFile));
        } finally {
            Files.setPosixFilePermissions(tempBcfksFile, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
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
