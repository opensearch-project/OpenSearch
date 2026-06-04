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

import java.nio.file.Path;

import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.TRUSTSTORE_PASSWORD_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.TRUSTSTORE_PATH_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.TRUSTSTORE_TYPE_SETTING;

public class GoogleCloudStorageBlobContainerRetriesFipsTests extends GoogleCloudStorageBlobContainerRetriesTests {

    private final Path truststorePath = getDataPath("/google.bcfks");

    @Override
    protected void configureClientSettings(Settings.Builder settings, String clientName) {
        settings.put(TRUSTSTORE_PATH_SETTING.getConcreteSettingForNamespace(clientName).getKey(), truststorePath.toString());
        settings.put(TRUSTSTORE_TYPE_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "BCFKS");
    }

    @Override
    protected void configureSecureSettings(MockSecureSettings secureSettings, String clientName) {
        secureSettings.setString(TRUSTSTORE_PASSWORD_SETTING.getConcreteSettingForNamespace(clientName).getKey(), "notasecret");
    }
}
