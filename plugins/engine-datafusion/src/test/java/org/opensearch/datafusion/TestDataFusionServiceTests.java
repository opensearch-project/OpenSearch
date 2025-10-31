/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import java.util.HashSet;
import java.util.Set;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class TestDataFusionServiceTests extends OpenSearchTestCase {

    private DataFusionService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);

        service = new DataFusionService(Collections.emptyMap(),clusterSettings);
        service.doStart();
    }

    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        // The service returns codec information in JSON format
        assertTrue("Version should contain codecs", version.contains("codecs"));
        assertTrue("Version should contain CsvDataSourceCodec", version.contains("CsvDataSourceCodec"));
    }

    public void testCreateAndCloseContext() {
        service.registerDirectory("/Users/gbh/Documents", List.of("parquet-nested.csv"));
        long contextId = service.createSessionContext().join();
        // Create context
        assertTrue(contextId > 0);

        service.getVersion();
    }

    public void testCodecDiscovery() {
        // Test that the CSV codec can be discovered via SPI
        // TODO : test with dummy plugin and dummy codec
    }
}
