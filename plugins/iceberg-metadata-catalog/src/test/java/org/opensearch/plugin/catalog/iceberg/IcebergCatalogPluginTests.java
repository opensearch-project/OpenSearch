/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.common.settings.Setting;
import org.opensearch.plugin.catalog.iceberg.credentials.IcebergClientSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergCatalogPluginTests extends OpenSearchTestCase {

    public void testPluginCreation() throws Exception {
        try (IcebergCatalogPlugin plugin = new IcebergCatalogPlugin()) {
            assertNotNull(plugin);
            assertFalse(plugin.getSettings().isEmpty());
        }
    }

    public void testAllSettingsAreRegistered() throws Exception {
        try (IcebergCatalogPlugin plugin = new IcebergCatalogPlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            Set<String> keys = settings.stream().map(Setting::getKey).collect(Collectors.toSet());

            // Repository-scoped.
            assertTrue(keys.contains(IcebergCatalogRepository.BUCKET_ARN_SETTING.getKey()));
            assertTrue(keys.contains(IcebergCatalogRepository.REGION_SETTING.getKey()));
            assertTrue(keys.contains(IcebergCatalogRepository.CATALOG_ENDPOINT_SETTING.getKey()));
            assertTrue(keys.contains(IcebergCatalogRepository.ROLE_ARN_SETTING.getKey()));
            assertTrue(keys.contains(IcebergCatalogRepository.ROLE_SESSION_NAME_SETTING.getKey()));
            assertTrue(keys.contains(IcebergCatalogRepository.IDENTITY_TOKEN_FILE_SETTING.getKey()));

            // Keystore credentials.
            assertTrue(keys.contains(IcebergClientSettings.ACCESS_KEY_SETTING.getKey()));
            assertTrue(keys.contains(IcebergClientSettings.SECRET_KEY_SETTING.getKey()));
            assertTrue(keys.contains(IcebergClientSettings.SESSION_TOKEN_SETTING.getKey()));
        }
    }
}
