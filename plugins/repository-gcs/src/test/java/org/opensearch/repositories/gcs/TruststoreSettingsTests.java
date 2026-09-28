/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.core.common.settings.SecureString;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

public class TruststoreSettingsTests extends OpenSearchTestCase {

    public void testConfiguredTruststore() {
        final var path = Path.of("/path/to/truststore.bcfks");
        final var type = "BCFKS";
        final var password = new SecureString("password".toCharArray());

        final var settings = new TruststoreSettings(path, type, password);

        assertEquals(path, settings.path());
        assertEquals(type, settings.type());
        assertEquals(password, settings.password());
        assertTrue(settings.isConfigured());
    }

    public void testNotConfiguredWithNullPath() {
        final var settings = new TruststoreSettings(null, "BCFKS", new SecureString("password".toCharArray()));
        assertFalse(settings.isConfigured());
    }

    public void testNotConfiguredWithNullType() {
        final var settings = new TruststoreSettings(Path.of("/path/to/truststore.bcfks"), null, new SecureString("password".toCharArray()));
        assertFalse(settings.isConfigured());
    }

    public void testNotConfiguredWithEmptyType() {
        final var settings = new TruststoreSettings(Path.of("/path/to/truststore.bcfks"), "", new SecureString("password".toCharArray()));
        assertFalse(settings.isConfigured());
    }

    public void testConfiguredWithEmptyPassword() {
        final var settings = new TruststoreSettings(Path.of("/path/to/truststore.bcfks"), "BCFKS", null);
        assertTrue(settings.isConfigured());
    }

    public void testNoTruststoreSettings() {
        final var settings = TruststoreSettings.NO_TRUSTSTORE_SETTINGS;
        assertNull(settings.path());
        assertNull(settings.type());
        assertNull(settings.password());
        assertFalse(settings.isConfigured());
    }
}
