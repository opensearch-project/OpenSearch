/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
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

/**
 * Integration tests for the {@code datafusion.reduce.target_partitions} cluster setting,
 * which controls the number of partitions the coordinator-reduce DataFusion session uses.
 *
 * <p>Verifies:
 * <ul>
 *   <li>The setting is dynamically updatable and acknowledged.</li>
 *   <li>Out-of-range values (below 1, above 32) are rejected by validation.</li>
 *   <li>A coordinator-reduce query (GROUP BY) still produces correct results after the
 *       partition count is changed at runtime — exercising the setting end-to-end through
 *       the native bridge into a freshly-created reduce session.</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class ReduceTargetPartitionsIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "reduce_target_partitions_test";
    private static final String SETTING = "datafusion.reduce.target_partitions";
    private static final int NUM_DOCS = 2000;
    private static final int NUM_GROUPS = 50;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, TestPPLPlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
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
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    private void createIndexAndIngest() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        assertTrue(
            client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).setMapping("g", "type=integer").get().isAcknowledged()
        );

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("g", i % NUM_GROUPS));
        }
        BulkResponse bulkResponse = bulk.get();
        assertFalse("Bulk ingest should not have errors", bulkResponse.hasFailures());

        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    private PPLResponse executePPL(String query) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(query)).actionGet();
    }

    private void setReduceTargetPartitions(int value) {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(SETTING, value).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals(String.valueOf(value), response.getTransientSettings().get(SETTING));
    }

    public void testReduceTargetPartitionsSettable() {
        setReduceTargetPartitions(8);
    }

    public void testReduceTargetPartitionsRejectsBelowMin() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(SETTING, 0).build())
                .get()
        );
    }

    public void testReduceTargetPartitionsRejectsAboveMax() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(SETTING, 33).build())
                .get()
        );
    }

    /**
     * A coordinator-reduce GROUP BY must produce the correct number of groups regardless of the
     * configured reduce partition count. Running the same query at two different settings proves
     * the value flows through the native bridge into the reduce session without changing results.
     */
    public void testReduceCorrectAcrossPartitionCounts() throws Exception {
        createIndexAndIngest();

        for (int partitions : new int[] { 1, 4, 16 }) {
            setReduceTargetPartitions(partitions);
            PPLResponse response = executePPL("source = " + INDEX_NAME + " | stats count() by g");
            assertEquals(
                "GROUP BY g must yield " + NUM_GROUPS + " groups at reduce.target_partitions=" + partitions,
                NUM_GROUPS,
                response.getRows().size()
            );
        }
    }
}
