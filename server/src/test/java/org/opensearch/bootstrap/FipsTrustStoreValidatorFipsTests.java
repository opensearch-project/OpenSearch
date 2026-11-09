/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.util.io.IOUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStore;

public class FipsTrustStoreValidatorFipsTests extends FipsTrustStoreValidatorTests {

    public void testValidateAcceptsBCFKSType() throws Exception {
        var trustStoreResource = getClass().getResource("/org/opensearch/bootstrap/truststore/opensearch-fips-truststore.bcfks");
        assertNotNull(trustStoreResource);
        var trustStorePath = Path.of(trustStoreResource.toURI());

        // Should not throw exception
        FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "BCFIPS", "notarealpasswordphrase");
    }

    public void testBCFKSEmptyTrustStoreWarning() throws Exception {
        var trustStorePath = createTempFile("empty-trust_store", ".bcfks");
        var password = "testPassword";

        var keyStore = java.security.KeyStore.getInstance("BCFKS", "BCFIPS");
        keyStore.load(null, password.toCharArray());

        try (var fos = Files.newOutputStream(trustStorePath)) {
            keyStore.store(fos, password.toCharArray());
        }

        FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "BCFIPS", password);
    }

    public void testTrustStoreWithWrongPassword() throws Exception {
        var trustStoreResource = getClass().getResource("/org/opensearch/bootstrap/truststore/opensearch-fips-truststore.bcfks");
        assertNotNull("FIPS truststore resource not found", trustStoreResource);
        var trustStorePath = Path.of(trustStoreResource.toURI());

        var exception = expectThrows(
            java.io.UncheckedIOException.class,
            () -> FipsTrustStoreValidator.validate(trustStorePath.toString(), "BCFKS", "BCFIPS", "wrongPassword")
        );

        assertTrue(exception.getMessage().contains("possibly wrong password"));
    }

    public void testWithoutReadPermission() throws Exception {
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

}
