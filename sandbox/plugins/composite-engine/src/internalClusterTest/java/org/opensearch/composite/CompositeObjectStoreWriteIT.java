/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.native_store.FsNativeObjectStorePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

/**
 * E2E integration test validating that Parquet files are written through the
 * native ObjectStore (FS-backed) with correct data.
 *
 * <p>Tests the full production flow:
 * FS repo → native ObjectStore → PrefixStore scoping → AsyncArrowWriter → Parquet files on disk.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeObjectStoreWriteIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-objectstore-write";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            FsNativeObjectStorePlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /**
     * Tests that indexing documents into a composite-parquet index produces valid
     * Parquet data that survives flush and is reflected in commit metadata.
     */
    public void testParquetWriteThroughObjectStore() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(INDEX_NAME)
                .setSettings(indexSettings)
                .setMapping("field_keyword", "type=keyword", "field_number", "type=integer")
                .get()
                .isAcknowledged()
        );
        ensureGreen(INDEX_NAME);

        // Index documents
        int docCount = 10;
        for (int i = 0; i < docCount; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource("field_keyword", "value_" + i, "field_number", i * 100)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        // Flush to force Parquet file write
        FlushResponse flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).get();
        assertEquals(RestStatus.OK, flushResponse.getStatus());
        assertEquals(1, flushResponse.getSuccessfulShards());
        assertEquals(0, flushResponse.getShardFailures().length);

        // Verify commit metadata contains parquet data format
        var statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setIndexing(true).get();

        var shardStats = statsResponse.getIndex(INDEX_NAME).getShards()[0];
        assertEquals(docCount, shardStats.getStats().indexing.getTotal().getIndexCount());

        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));

        DataformatAwareCatalogSnapshot snapshot = DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        ensureGreen(INDEX_NAME);
    }
}
