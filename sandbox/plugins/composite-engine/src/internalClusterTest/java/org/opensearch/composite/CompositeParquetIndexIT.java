/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

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
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    public void testCreateCompositeParquetIndex() throws IOException {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(indexSettings)
            .setMapping("field_text", "type=text")
            .setMapping("field_keyword", "type=keyword")
            .setMapping("field_number", "type=integer")
            .get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(INDEX_NAME).get();
        Settings actual = settingsResponse.getIndexToSettings().get(INDEX_NAME);

        ensureGreen(INDEX_NAME);

        assertEquals("1", actual.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS));
        assertEquals("0", actual.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS));
        assertEquals("true", actual.get("index.pluggable.dataformat.enabled"));
        assertEquals("parquet", actual.get("index.composite.primary_data_format"));

        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", 10)
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        ensureGreen(INDEX_NAME);

        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        assertEquals(1, refreshResponse.getSuccessfulShards());
        assertEquals(1, refreshResponse.getTotalShards());
        assertEquals(0, refreshResponse.getShardFailures().length);

        ensureGreen(INDEX_NAME);

        FlushResponse flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).get();
        assertEquals(RestStatus.OK, flushResponse.getStatus());
        assertEquals(1, flushResponse.getSuccessfulShards());
        assertEquals(1, flushResponse.getTotalShards());
        assertEquals(0, flushResponse.getShardFailures().length);

        IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .prepareStats(INDEX_NAME)
            .clear()
            .setIndexing(true)
            .setRefresh(true)
            .setDocs(true)
            .setStore(true)
            .get();

        ShardStats shardStats = statsResponse.getIndex(INDEX_NAME).getShards()[0];

        assertEquals(10, shardStats.getStats().indexing.getTotal().getIndexCount());

        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertNotNull(commitStats.getUserData());
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        assertTrue(commitStats.getUserData().containsKey(CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY));

        DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        ensureGreen(INDEX_NAME);
    }

    public void testCompositeParquetWithLuceneSecondary() throws IOException {
        String indexName = "test-composite-parquet-lucene";

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("field_text", "type=text", "field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());

        ensureGreen(indexName);

        // Index documents with text, keyword, and integer fields
        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex()
                .setIndex(indexName)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", randomInt(100))
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        ensureGreen(indexName);

        // Refresh
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(indexName).get();
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        assertEquals(1, refreshResponse.getSuccessfulShards());
        assertEquals(1, refreshResponse.getTotalShards());
        assertEquals(0, refreshResponse.getShardFailures().length);

        ensureGreen(indexName);

        // Flush
        FlushResponse flushResponse = client().admin().indices().prepareFlush(indexName).get();
        assertEquals(RestStatus.OK, flushResponse.getStatus());
        assertEquals(1, flushResponse.getSuccessfulShards());
        assertEquals(1, flushResponse.getTotalShards());
        assertEquals(0, flushResponse.getShardFailures().length);

        // Verify commit stats contain a catalog snapshot
        IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setIndexing(true)
            .setRefresh(true)
            .setDocs(true)
            .setStore(true)
            .get();

        ShardStats shardStats = statsResponse.getIndex(indexName).getShards()[0];

        assertEquals(10, shardStats.getStats().indexing.getTotal().getIndexCount());

        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertNotNull(commitStats.getUserData());
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        assertTrue(commitStats.getUserData().containsKey(CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY));

        // Deserialize the catalog snapshot and verify it contains BOTH parquet AND lucene data formats
        DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
        assertEquals(Set.of("parquet", "lucene"), snapshot.getDataFormats());

        // Verify segment count and that each segment has files for both formats
        assertFalse("Snapshot should have segments", snapshot.getSegments().isEmpty());
        for (org.opensearch.index.engine.exec.Segment segment : snapshot.getSegments()) {
            assertTrue("Each segment should have parquet files", segment.dfGroupedSearchableFiles().containsKey("parquet"));
            assertTrue("Each segment should have lucene files", segment.dfGroupedSearchableFiles().containsKey("lucene"));
        }

        ensureGreen(indexName);
    }

    public void testCompositeIndexUsesClusterDefaultFormatsWhenOverridesAbsent() throws IOException {
        String indexName = "test-composite-cluster-default";

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CompositeDataFormatPlugin.CLUSTER_DEFAULT_PRIMARY_DATA_FORMAT.getKey(), "parquet")
                    .putList(CompositeDataFormatPlugin.CLUSTER_DEFAULT_SECONDARY_DATA_FORMATS.getKey(), "lucene")
            )
            .get();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("field_text", "type=text", "field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());

        ensureGreen(indexName);

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
        Settings actual = settingsResponse.getIndexToSettings().get(indexName);
        assertEquals("parquet", actual.get(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey()));
        assertEquals("lucene", actual.getAsList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey()).get(0));

        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex()
                .setIndex(indexName)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", randomInt(100))
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(indexName).get().getStatus());
        assertEquals(RestStatus.OK, client().admin().indices().prepareFlush(indexName).get().getStatus());

        IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setIndexing(true)
            .setRefresh(true)
            .setDocs(true)
            .setStore(true)
            .get();
        ShardStats shardStats = statsResponse.getIndex(indexName).getShards()[0];
        assertEquals(10, shardStats.getStats().indexing.getTotal().getIndexCount());

        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));

        DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
        assertEquals(Set.of("parquet", "lucene"), snapshot.getDataFormats());

        ensureGreen(indexName);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .putNull(CompositeDataFormatPlugin.CLUSTER_DEFAULT_PRIMARY_DATA_FORMAT.getKey())
                    .putNull(CompositeDataFormatPlugin.CLUSTER_DEFAULT_SECONDARY_DATA_FORMATS.getKey())
            )
            .get();
    }

    public void testCompositeIndexRequestOverrideBeatsClusterDefault() throws IOException {
        String indexName = "test-composite-request-override";

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CompositeDataFormatPlugin.CLUSTER_DEFAULT_PRIMARY_DATA_FORMAT.getKey(), "parquet")
                    .putList(CompositeDataFormatPlugin.CLUSTER_DEFAULT_SECONDARY_DATA_FORMATS.getKey(), "lucene")
            )
            .get();

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings)
            .setMapping("field_text", "type=text", "field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        assertTrue("Index creation should be acknowledged", response.isAcknowledged());

        ensureGreen(indexName);

        GetSettingsResponse settingsResponse = client().admin().indices().prepareGetSettings(indexName).get();
        Settings actual = settingsResponse.getIndexToSettings().get(indexName);
        assertEquals("lucene", actual.get(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey()));
        assertTrue(actual.getAsList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey()).isEmpty());

        for (int i = 0; i < 10; i++) {
            IndexResponse indexResponse = client().prepareIndex()
                .setIndex(indexName)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", randomInt(100))
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
        }

        assertEquals(RestStatus.OK, client().admin().indices().prepareRefresh(indexName).get().getStatus());
        assertEquals(RestStatus.OK, client().admin().indices().prepareFlush(indexName).get().getStatus());

        IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setIndexing(true)
            .setRefresh(true)
            .setDocs(true)
            .setStore(true)
            .get();
        ShardStats shardStats = statsResponse.getIndex(indexName).getShards()[0];
        assertEquals(10, shardStats.getStats().indexing.getTotal().getIndexCount());

        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));

        DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
        assertEquals(Set.of("lucene"), snapshot.getDataFormats());

        ensureGreen(indexName);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .putNull(CompositeDataFormatPlugin.CLUSTER_DEFAULT_PRIMARY_DATA_FORMAT.getKey())
                    .putNull(CompositeDataFormatPlugin.CLUSTER_DEFAULT_SECONDARY_DATA_FORMATS.getKey())
            )
            .get();
    }
}
