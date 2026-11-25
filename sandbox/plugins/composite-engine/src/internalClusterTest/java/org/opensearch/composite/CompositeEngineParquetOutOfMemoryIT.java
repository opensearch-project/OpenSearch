/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

/**
 * Verifies the composite engine's graceful handling of an Arrow {@code OutOfMemoryException}
 * raised while allocating the VectorSchemaRoot for the parquet primary format.
 *
 * <p>The Arrow ingest pool is constrained (via the dynamic
 * {@code native.allocator.pool.ingest.max} setting) so tightly that populating the VSR for
 * even a single document throws an {@link org.apache.arrow.memory.OutOfMemoryException} inside
 * {@code VSRManager.addDocument}. {@code ParquetWriter.addDoc} must translate this into a
 * {@code WriteResult.Failure} (rather than propagating the throw); the {@code CompositeWriter}
 * then rolls the doc back and the shard/engine stays open. The write surfaces as a per-document
 * failure and the cluster must remain {@link ClusterHealthStatus#GREEN}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeEngineParquetOutOfMemoryIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "composite-parquet-oom-idx";
    private static final String INGEST_MIN = "native.allocator.pool.ingest.min";
    private static final String INGEST_MAX = "native.allocator.pool.ingest.max";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // ParquetDataFormatPlugin (via ParquetOnlyDataFormatPlugin) sources its allocator from
        // the unified native-allocator framework's ingest pool, so ArrowBasePlugin is required.
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings compositeParquetIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    /** Sets the ingest pool min/max via dynamic transient cluster settings. */
    private void setIngestPoolLimit(long minBytes, long maxBytes) {
        ClusterUpdateSettingsResponse resp = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(INGEST_MIN, minBytes).put(INGEST_MAX, maxBytes))
            .get();
        assertTrue("ingest pool limit update must be acknowledged", resp.isAcknowledged());
    }

    public void testSingleDocOutOfMemoryKeepsClusterGreen() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeParquetIndexSettings())
            .setMapping("f1", "type=keyword")
            .get();
        ensureGreen(INDEX_NAME);

        // Constrain the ingest pool so hard that the first VSR buffer allocation for a single
        // document throws an Arrow OutOfMemoryException. No writer/VSR exists yet, so no ingest
        // memory is in use at this point and lowering the limit is safe.
        setIngestPoolLimit(0L, 1L);

        // The document must surface as a per-item failure, not a thrown/tragic error.
        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setSource("f1", "value-1")).get();
        assertEquals(1, bulk.getItems().length);
        assertTrue(
            "single doc must fail with an OOM-driven per-item failure: " + bulk.getItems()[0].getFailureMessage(),
            bulk.getItems()[0].isFailed()
        );

        // The engine must still be open — commitStats() throws AlreadyClosedException if the
        // engine has failed/closed. A per-doc OOM must not be treated as a tragic event.
        DataFormatAwareEngine engine = CompositeEngineHelper.getEngine(clusterService(), internalCluster(), INDEX_NAME);
        assertNotNull("engine must remain open after a single-doc OOM", engine.commitStats());

        // The cluster must remain GREEN.
        ClusterHealthStatus status = client().admin().cluster().prepareHealth(INDEX_NAME).get().getStatus();
        assertEquals("cluster must remain GREEN after a per-doc OOM", ClusterHealthStatus.GREEN, status);
        ensureGreen(INDEX_NAME);

        // Relieve the limit and confirm the engine still accepts writes.
        setIngestPoolLimit(0L, Long.MAX_VALUE);
        IndexResponse ok = client().prepareIndex(INDEX_NAME).setSource("f1", "value-2").get();
        assertEquals(RestStatus.CREATED, ok.status());
        ensureGreen(INDEX_NAME);
    }
}
