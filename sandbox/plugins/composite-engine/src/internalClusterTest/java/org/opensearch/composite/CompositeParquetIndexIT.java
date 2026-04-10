/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

/**
 * Integration test that validates a composite index with parquet as the primary data format
 * can be created and its settings are correctly persisted.
 *
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:test \
 *   --tests "*.CompositeParquetIndexIT" \
 *   -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeParquetIndexIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-parquet";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeEnginePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    public void testCreateCompositeParquetIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(INDEX_NAME).get();
        Settings actual = settingsResponse.getIndexToSettings().get(INDEX_NAME);

        assertEquals("1", actual.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
        assertEquals("0", actual.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertEquals("true", actual.get("index.pluggable.dataformat.enabled"));
        assertEquals("parquet", actual.get("index.composite.primary_data_format"));

        ensureGreen(INDEX_NAME);
    }
}
