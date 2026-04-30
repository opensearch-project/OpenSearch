/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

/**
 * Integration test verifying that native heap tracking works correctly
 * during real indexing operations on a composite parquet index.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class HeapTrackingWithIndexingIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-heap-tracking";

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

    public void testHeapRegistrationAfterClusterStartup() {
        int count = NativeLibraryLoader.heapCount();
        assertTrue("Expected at least 2 heaps, got " + count, count >= 2);
        assertTrue("parquet heap should be registered", findHeapIndex("parquet") >= 0);
        assertTrue("datafusion heap should be registered", findHeapIndex("datafusion") >= 0);

        for (int i = 0; i < count; i++) {
            assertTrue("used >= 0 for heap " + i, NativeLibraryLoader.heapUsed(i) >= 0);
            assertTrue("committed >= 0 for heap " + i, NativeLibraryLoader.heapCommitted(i) >= 0);
        }
    }

    public void testIndexingWithHeapTracking() {
        createParquetIndex();

        int parquetIdx = findHeapIndex("parquet");
        long parquetBefore = NativeLibraryLoader.heapUsed(parquetIdx);

        // Index documents
        for (int i = 0; i < 50; i++) {
            IndexResponse resp = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource("message", "heap tracking test document " + i, "count", i)
                .get();
            assertEquals(RestStatus.CREATED, resp.status());
        }

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        ShardStats shardStats = client().admin()
            .indices()
            .prepareStats(INDEX_NAME)
            .clear()
            .setIndexing(true)
            .get()
            .getIndex(INDEX_NAME)
            .getShards()[0];
        assertEquals(50, shardStats.getStats().indexing.getTotal().getIndexCount());

        long parquetAfter = NativeLibraryLoader.heapUsed(parquetIdx);
        long growth = parquetAfter - parquetBefore;
        assertTrue("parquet heap should grow by at least 512 bytes after indexing 50 docs, actual growth=" + growth, growth >= 512);
    }

    public void testGlobalCommittedCoversPluginHeaps() {
        createParquetIndex();

        for (int i = 0; i < 20; i++) {
            client().prepareIndex().setIndex(INDEX_NAME).setSource("message", "data " + i, "count", i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        long globalCommitted = NativeLibraryLoader.globalCommitted();
        assertTrue("global committed should be > 0", globalCommitted > 0);

        long sumCommitted = 0;
        for (int i = 0; i < NativeLibraryLoader.heapCount(); i++) {
            sumCommitted += NativeLibraryLoader.heapCommitted(i);
        }
        assertTrue("sum committed (" + sumCommitted + ") <= global (" + globalCommitted + ")", sumCommitted <= globalCommitted);
    }

    public void testCrossPluginHeapIsolation() {
        int pqIdx = findHeapIndex("parquet");
        int dfIdx = findHeapIndex("datafusion");

        long pqBefore = NativeLibraryLoader.heapUsed(pqIdx);
        long dfBefore = NativeLibraryLoader.heapUsed(dfIdx);

        long size = 256 * 1024;
        long ptr = RustBridge.allocateTestBuffer(size);
        assertTrue("allocateTestBuffer should return non-zero pointer", ptr != 0);

        long pqAfterAlloc = NativeLibraryLoader.heapUsed(pqIdx);
        long dfAfterAlloc = NativeLibraryLoader.heapUsed(dfIdx);

        assertTrue("parquet used should increase after alloc", pqAfterAlloc > pqBefore);
        // mimalloc may retain small thread-local metadata, so allow up to 64KB slack
        assertTrue(
            "df used should not significantly change after parquet alloc, delta=" + (dfAfterAlloc - dfBefore),
            dfAfterAlloc - dfBefore < 64 * 1024
        );

        NativeBridge.freeTestBuffer(ptr, size);

        long pqAfterFree = NativeLibraryLoader.heapUsed(pqIdx);
        long dfAfterFree = NativeLibraryLoader.heapUsed(dfIdx);

        assertTrue("parquet used should decrease after free", pqAfterFree < pqAfterAlloc);
        assertTrue(
            "df used should remain stable after freeing parquet's buffer, delta=" + (dfAfterFree - dfBefore),
            dfAfterFree - dfBefore < 64 * 1024
        );
    }

    private void createParquetIndex() {
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
                .setMapping("message", "type=text", "count", "type=integer")
                .get()
                .isAcknowledged()
        );
        ensureGreen(INDEX_NAME);
    }

    private int findHeapIndex(String name) {
        int count = NativeLibraryLoader.heapCount();
        for (int i = 0; i < count; i++) {
            if (name.equals(NativeLibraryLoader.heapName(i))) return i;
        }
        return -1;
    }
}
