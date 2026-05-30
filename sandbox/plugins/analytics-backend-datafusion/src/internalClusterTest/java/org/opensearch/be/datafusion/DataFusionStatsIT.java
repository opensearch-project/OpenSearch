/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsActionType;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodeResponse;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodesRequest;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodesResponse;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests for the DataFusion cluster stats endpoint.
 *
 * <p>Validates node-level and cluster-level stats retrieval via the transport action,
 * including stat section filtering.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class DataFusionStatsIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
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

    // ---- Test: Cluster-level stats returns all nodes ----

    public void testClusterLevelStatsReturnsAllNodes() {
        DataFusionStatsNodesRequest request = new DataFusionStatsNodesRequest(new String[0], Collections.emptySet());
        DataFusionStatsNodesResponse response = client().execute(DataFusionStatsActionType.INSTANCE, request).actionGet();

        assertNotNull(response);
        assertEquals("Should return stats from all 2 data nodes", 2, response.getNodes().size());
        assertTrue("Should have no failures", response.failures().isEmpty());

        for (DataFusionStatsNodeResponse nodeResponse : response.getNodes()) {
            assertNotNull("Each node should have a DiscoveryNode", nodeResponse.getNode());
            assertNotNull("Each node should have stats", nodeResponse.getStats());
        }
    }

    // ---- Test: Local node stats returns single node ----

    public void testLocalNodeStatsReturnsSingleNode() {
        DataFusionStatsNodesRequest request = new DataFusionStatsNodesRequest(new String[] { "_local" }, Collections.emptySet());
        DataFusionStatsNodesResponse response = client().execute(DataFusionStatsActionType.INSTANCE, request).actionGet();

        assertNotNull(response);
        assertEquals("_local should return exactly 1 node", 1, response.getNodes().size());
        assertTrue("Should have no failures", response.failures().isEmpty());
        assertNotNull("Local node should have stats", response.getNodes().get(0).getStats());
    }

    // ---- Test: Single stat filter returns only that section ----

    @SuppressWarnings("unchecked")
    public void testSingleStatFilterReturnsOnlyRequestedSection() throws Exception {
        DataFusionStatsNodesRequest request = new DataFusionStatsNodesRequest(
            new String[] { "_local" },
            Set.of("io_runtime")
        );
        DataFusionStatsNodesResponse response = client().execute(DataFusionStatsActionType.INSTANCE, request).actionGet();

        assertEquals(1, response.getNodes().size());
        assertNotNull(response.getNodes().get(0).getStats());

        // Render to JSON and verify structure
        Map<String, Object> nodeStats = renderNodeStats(response.getNodes().get(0));

        assertThat("io_runtime should be present", nodeStats, hasKey("io_runtime"));
        assertThat("cpu_runtime should NOT be present", nodeStats, not(hasKey("cpu_runtime")));
        assertThat("coordinator_reduce should NOT be present", nodeStats, not(hasKey("coordinator_reduce")));
        assertThat("query_execution should NOT be present", nodeStats, not(hasKey("query_execution")));
        assertThat("stream_next should NOT be present", nodeStats, not(hasKey("stream_next")));
        assertThat("plan_setup should NOT be present", nodeStats, not(hasKey("plan_setup")));
        assertThat("datanode_gate should NOT be present", nodeStats, not(hasKey("datanode_gate")));
        assertThat("coordinator_gate should NOT be present", nodeStats, not(hasKey("coordinator_gate")));
    }

    // ---- Test: Multiple stat filter returns only requested sections ----

    @SuppressWarnings("unchecked")
    public void testMultipleStatFilterReturnsOnlyRequestedSections() throws Exception {
        DataFusionStatsNodesRequest request = new DataFusionStatsNodesRequest(
            new String[] { "_local" },
            Set.of("io_runtime", "datanode_gate")
        );
        DataFusionStatsNodesResponse response = client().execute(DataFusionStatsActionType.INSTANCE, request).actionGet();

        assertEquals(1, response.getNodes().size());
        assertNotNull(response.getNodes().get(0).getStats());

        Map<String, Object> nodeStats = renderNodeStats(response.getNodes().get(0));

        assertThat("io_runtime should be present", nodeStats, hasKey("io_runtime"));
        assertThat("datanode_gate should be present", nodeStats, hasKey("datanode_gate"));
        assertThat("cpu_runtime should NOT be present", nodeStats, not(hasKey("cpu_runtime")));
        assertThat("coordinator_reduce should NOT be present", nodeStats, not(hasKey("coordinator_reduce")));
        assertThat("coordinator_gate should NOT be present", nodeStats, not(hasKey("coordinator_gate")));
    }

    // ---- Test: No filter returns all stat sections ----

    @SuppressWarnings("unchecked")
    public void testNoFilterReturnsAllSections() throws Exception {
        DataFusionStatsNodesRequest request = new DataFusionStatsNodesRequest(new String[] { "_local" }, Collections.emptySet());
        DataFusionStatsNodesResponse response = client().execute(DataFusionStatsActionType.INSTANCE, request).actionGet();

        assertEquals(1, response.getNodes().size());
        assertNotNull(response.getNodes().get(0).getStats());

        Map<String, Object> nodeStats = renderNodeStats(response.getNodes().get(0));

        assertThat("io_runtime should be present", nodeStats, hasKey("io_runtime"));
        assertThat("cpu_runtime should be present", nodeStats, hasKey("cpu_runtime"));
        assertThat("coordinator_reduce should be present", nodeStats, hasKey("coordinator_reduce"));
        assertThat("query_execution should be present", nodeStats, hasKey("query_execution"));
        assertThat("stream_next should be present", nodeStats, hasKey("stream_next"));
        assertThat("plan_setup should be present", nodeStats, hasKey("plan_setup"));
        assertThat("datanode_gate should be present", nodeStats, hasKey("datanode_gate"));
        assertThat("coordinator_gate should be present", nodeStats, hasKey("coordinator_gate"));
    }

    // ---- Test: Full response XContent structure ----

    @SuppressWarnings("unchecked")
    public void testResponseXContentStructure() throws Exception {
        DataFusionStatsNodesRequest request = new DataFusionStatsNodesRequest(new String[0], Collections.emptySet());
        DataFusionStatsNodesResponse response = client().execute(DataFusionStatsActionType.INSTANCE, request).actionGet();

        // Render full response
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> fullMap = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);

        // Verify top-level structure
        assertThat("_nodes should be present", fullMap, hasKey("_nodes"));
        assertThat("cluster_name should be present", fullMap, hasKey("cluster_name"));
        assertThat("nodes should be present", fullMap, hasKey("nodes"));

        // Verify _nodes counts
        Map<String, Object> nodesHeader = (Map<String, Object>) fullMap.get("_nodes");
        assertEquals(2, nodesHeader.get("total"));
        assertEquals(2, nodesHeader.get("successful"));
        assertEquals(0, nodesHeader.get("failed"));

        // Verify nodes object has 2 entries
        Map<String, Object> nodes = (Map<String, Object>) fullMap.get("nodes");
        assertEquals("Should have 2 node entries", 2, nodes.size());

        // Verify no IP leakage in node entries
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            Map<String, Object> nodeEntry = (Map<String, Object>) entry.getValue();
            assertFalse("Node entry should NOT contain 'name'", nodeEntry.containsKey("name"));
            assertFalse("Node entry should NOT contain 'host'", nodeEntry.containsKey("host"));
            assertFalse("Node entry should NOT contain 'transport_address'", nodeEntry.containsKey("transport_address"));
        }
    }

    // ---- Helper: render a node response's stats to a Map ----

    private Map<String, Object> renderNodeStats(DataFusionStatsNodeResponse nodeResponse) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        nodeResponse.getStats().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }
}
