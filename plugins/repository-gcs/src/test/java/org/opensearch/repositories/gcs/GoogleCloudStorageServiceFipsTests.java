/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;

public class GoogleCloudStorageServiceFipsTests extends GoogleCloudStorageServiceTests {

    private final Path truststorePath = getDataPath("/google.bcfks");

    protected Settings.Builder newSettingsBuilder(MockSecureSettings secureSettings, String... clientNames) {
        final var builder = super.newSettingsBuilder(secureSettings, clientNames);
        for (var clientName : clientNames) {
            builder.put(
                GoogleCloudStorageClientSettings.TRUSTSTORE_PATH_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                truststorePath.toString()
            );
            builder.put(
                GoogleCloudStorageClientSettings.TRUSTSTORE_TYPE_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                "BCFKS"
            );
            secureSettings.setString(
                GoogleCloudStorageClientSettings.TRUSTSTORE_PASSWORD_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                "notasecret"
            );
        }
        return builder;
    }

    public void testCustomTruststoreConfiguration() throws Exception {
        final var clientName = "gcs1";
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString("gcs.client.gcs1.truststore.password", "notasecret");

        final var settings = Settings.builder()
            .setSecureSettings(secureSettings)
            .put("gcs.client.gcs1.truststore.path", truststorePath.toString())
            .put("gcs.client.gcs1.truststore.type", "BCFKS")
            .build();

        final var service = new GoogleCloudStorageService();
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));

        final var clientSettings = GoogleCloudStorageClientSettings.getClientSettings(settings, clientName);
        final var truststoreSettings = clientSettings.getTruststoreSettings();
        assertNotNull(truststoreSettings);
        assertTrue(truststoreSettings.isConfigured());
        assertEquals(truststorePath.toString(), truststoreSettings.path().toString());
        assertEquals("notasecret", truststoreSettings.password().toString());
        assertEquals("BCFKS", truststoreSettings.type());

        // Verify that a client can be created with the custom truststore
        final var statsCollector = new GoogleCloudStorageOperationsStats("bucket");
        final var storage = service.client(clientName, "repo", statsCollector);
        assertNotNull(storage);
    }

    public void testFipsModeEnforcesTruststoreConfiguration() throws Exception {
        final var service = new GoogleCloudStorageService();
        final var statsCollector = new GoogleCloudStorageOperationsStats("bucket");

        { // Scenario 1: No truststore configuration -> should throw exception
            final var secureSettings = new MockSecureSettings();
            final var settings = Settings.builder().setSecureSettings(secureSettings).build();
            service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));

            final var exception = expectThrows(IOException.class, () -> service.client("default", "repo", statsCollector));
            assertThat(exception.getMessage(), containsString("FIPS mode is active but no custom truststore is configured"));
        }

        { // Scenario 2: Path + password configured (missing type) -> should throw exception
            final var secureSettings = new MockSecureSettings();
            final var settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .put("gcs.client.default.truststore.path", truststorePath.toString())
                .build();

            final var exception = expectThrows(SettingsException.class, () -> GoogleCloudStorageClientSettings.load(settings));
            assertThat(exception.getMessage(), containsString("truststore type is missing"));
        }

        { // Scenario 3: Path + type + password configured -> should work
            final var secureSettings = new MockSecureSettings();
            secureSettings.setString("gcs.client.default.truststore.password", "notasecret");
            final var settings = Settings.builder()
                .setSecureSettings(secureSettings)
                .put("gcs.client.default.truststore.path", truststorePath.toString())
                .put("gcs.client.default.truststore.type", "BCFKS")
                .build();
            service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(settings));
            final var storage = service.client("default", "repo", statsCollector);
            assertNotNull("Storage client should be created successfully with full truststore configuration", storage);
        }
    }

}
