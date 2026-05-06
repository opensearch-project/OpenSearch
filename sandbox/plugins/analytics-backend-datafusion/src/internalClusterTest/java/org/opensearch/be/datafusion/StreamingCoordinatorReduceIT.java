/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Streaming variant of {@link CoordinatorReduceIT}: same 2-shard parquet-backed index and deterministic
 * dataset, but with Arrow Flight RPC enabled via {@link FeatureFlags#STREAM_TRANSPORT}. Exercises the
 * shard-fragment → Flight → {@code DatafusionReduceSink.feed} handoff that previously failed with
 * {@code "A buffer can only be associated between two allocators that share the same root"} on
 * multi-shard queries.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class StreamingCoordinatorReduceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "coord_reduce_streaming_e2e";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 10;
    /** Constant `value` for every doc — deterministic assertion independent of shard routing. */
    private static final int VALUE = 7;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
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
     * {@code source = T} on a 2-shard parquet-backed index with streaming enabled exercises the
     * coordinator reduce sink's cross-plugin VectorSchemaRoot handoff. Before the allocator-root
     * unification fix, this failed with an Arrow {@code associate} mismatch.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testBaselineScanAcrossShards() throws Exception {
        createParquetBackedIndex();
        indexDeterministicDocs();

        PPLResponse response = executePPL("source = " + INDEX);

        assertNotNull("PPLResponse must not be null", response);
        assertTrue("columns must contain 'value', got " + response.getColumns(), response.getColumns().contains("value"));

        int expectedRows = NUM_SHARDS * DOCS_PER_SHARD;
        assertEquals("all docs across shards must be returned", expectedRows, response.getRows().size());

        int idx = response.getColumns().indexOf("value");
        for (Object[] row : response.getRows()) {
            Object cell = row[idx];
            assertNotNull("value cell must not be null", cell);
            assertEquals("every doc has value=" + VALUE, (long) VALUE, ((Number) cell).longValue());
        }
    }

    private void createParquetBackedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);
    }

    private void indexDeterministicDocs() {
        int total = NUM_SHARDS * DOCS_PER_SHARD;
        for (int i = 0; i < total; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
