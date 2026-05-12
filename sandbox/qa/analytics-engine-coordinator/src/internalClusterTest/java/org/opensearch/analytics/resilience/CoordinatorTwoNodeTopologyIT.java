/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Two-node topology coverage. With two data nodes and a 4-shard index the
 * primaries are typically distributed (~2 per node), so the coordinator-reduce
 * stage receives streamed batches from both same-node and peer-node fragment
 * executions. Exercises the cross-node FragmentExecutionAction streaming path.
 *
 * <p>Lives in its own IT class because the Tokio runtime manager is JVM-global;
 * mid-test node stops would shut it down for the whole test JVM. Using a
 * class-level {@code @ClusterScope(numDataNodes = 2)} avoids that.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class CoordinatorTwoNodeTopologyIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "topology_two_node_idx";
    private static final int NUM_SHARDS = 4;
    private static final int VALUE = 7;
    private static final int DOCS = 40;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestPPLPlugin.class,
            FlightStreamPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            MockCommitterEnginePlugin.class
        );
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
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    /**
     * Two nodes, four shards. Forces both same-node and peer-node fragment dispatch
     * through the streaming transport, with the coordinator merging at least one
     * remote stream into the reduce-stage output.
     */
    public void testTwoNodeFourShardSum() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
        CreateIndexResponse cr = client().admin().indices().prepareCreate(INDEX).setSettings(indexSettings).setMapping("value", "type=integer").get();
        assertTrue(cr.isAcknowledged());
        ensureGreen(INDEX);

        for (int i = 0; i < DOCS; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        long expected = (long) DOCS * VALUE;
        try {
            assertBusy(() -> {
                PPLResponse r = client().execute(
                    UnifiedPPLExecuteAction.INSTANCE,
                    new PPLRequest("source = " + INDEX + " | stats sum(value) as total")
                ).actionGet(QUERY_TIMEOUT);
                int idx = r.getColumns().indexOf("total");
                assertEquals(1, r.getRows().size());
                long actual = ((Number) r.getRows().get(0)[idx]).longValue();
                assertThat(actual, equalTo(expected));
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("two-node four-shard query failed", e);
        }
    }
}
