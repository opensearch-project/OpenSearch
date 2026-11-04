/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.sandbox;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;

/**
 * End-to-end verification of the sandbox engine-stack wiring installed by {@link OpenSearchIntegTestCase} when
 * {@code -Dsandbox.enabled=true}. Runs only when the sandbox modules are on the internalClusterTest classpath, so it is
 * written WITHOUT any compile dependency on them: every sandbox plugin is referenced by its String FQCN.
 * <p>
 * It asserts the sandbox stack wiring together on a live node:
 * <ol>
 *   <li>all 7 stack plugins are loaded on every node via NodesInfo; the stack is delivered as {@link PluginInfo}s
 *       carrying {@code extendedPlugins} metadata (extension metadata);</li>
 *   <li>the stack deliberately EXCLUDES dsl-query-executor, so no node reports that plugin and every IT search runs on
 *       the normal OpenSearch (fallback) path (DSL excluded);</li>
 *   <li>a plain {@code _search} still returns its hit on the normal path;</li>
 *   <li>both analytics backends (datafusion + lucene) are registered with AnalyticsPlugin on every node, and both
 *       backend plugins report {@code analytics-engine} in their {@code extendedPlugins} metadata. This is the
 *       extension wiring that works under the shared test classloader because {@link OpenSearchIntegTestCase}'s
 *       {@code SANDBOX_STACK_PLUGINS} carries the AnalyticsPlugin edge and {@code PluginsService} tolerates the sibling
 *       {@code AnalyticsSearchBackendPlugin} SPI entries both backends expose on that one loader (backends registered).</li>
 * </ol>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SandboxStackWiringIT extends OpenSearchIntegTestCase {

    private static final String ARROW_BASE_PLUGIN = "org.opensearch.arrow.allocator.ArrowBasePlugin";
    private static final String FLIGHT_STREAM_PLUGIN = "org.opensearch.arrow.flight.transport.FlightStreamPlugin";
    private static final String ANALYTICS_PLUGIN = "org.opensearch.analytics.AnalyticsPlugin";
    private static final String COMPOSITE_DATAFORMAT_PLUGIN = "org.opensearch.composite.CompositeDataFormatPlugin";
    private static final String PARQUET_DATAFORMAT_PLUGIN = "org.opensearch.parquet.ParquetDataFormatPlugin";
    private static final String DATAFUSION_PLUGIN = "org.opensearch.be.datafusion.DataFusionPlugin";
    private static final String LUCENE_PLUGIN = "org.opensearch.be.lucene.LucenePlugin";

    /** All 7 sandbox stack plugin FQCNs that must be loaded on every node when the stack is enabled. */
    private static final List<String> STACK_PLUGINS = List.of(
        ARROW_BASE_PLUGIN,
        FLIGHT_STREAM_PLUGIN,
        ANALYTICS_PLUGIN,
        COMPOSITE_DATAFORMAT_PLUGIN,
        PARQUET_DATAFORMAT_PLUGIN,
        DATAFUSION_PLUGIN,
        LUCENE_PLUGIN
    );

    /** The DSL query-executor plugin FQCN; deliberately NOT part of the stack, so it must not be loaded on any node. */
    private static final String DSL_QUERY_EXECUTOR_PLUGIN = "org.opensearch.dsl.DslQueryExecutorPlugin";

    /** The two analytics backend ids that must be registered with AnalyticsPlugin once DataFusion and Lucene extend it. */
    private static final Set<String> EXPECTED_BACKENDS = Set.of("datafusion", "lucene");

    /** Skip entirely unless the build forwarded -Dsandbox.enabled=true (the only mode where the stack is on the classpath). */
    @Before
    public void skipWithoutSandbox() {
        assumeTrue("requires -Dsandbox.enabled=true", Boolean.parseBoolean(System.getProperty("sandbox.enabled", "false")));
    }

    private NodesInfoResponse nodesInfoWithPlugins() {
        return client().admin().cluster().prepareNodesInfo().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName()).get();
    }

    /** (1) Every node reports all 7 stack plugins loaded. */
    public void testStackPluginsLoadedOnEveryNode() {
        NodesInfoResponse response = nodesInfoWithPlugins();
        assertFalse("NodesInfo returned no nodes", response.getNodes().isEmpty());
        for (NodeInfo nodeInfo : response.getNodes()) {
            PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
            assertNotNull("node " + nodeInfo.getNode().getName() + " reported no plugins metric", plugins);
            Set<String> names = plugins.getPluginInfos().stream().map(PluginInfo::getName).collect(Collectors.toSet());
            for (String fqcn : STACK_PLUGINS) {
                assertTrue(
                    "node " + nodeInfo.getNode().getName() + " missing stack plugin " + fqcn + "; loaded=" + names,
                    names.contains(fqcn)
                );
            }
        }
    }

    /** (2) The stack excludes dsl-query-executor, so no node reports that plugin; every IT search takes the fallback path. */
    public void testDslQueryExecutorNotLoadedOnAnyNode() {
        NodesInfoResponse response = nodesInfoWithPlugins();
        assertFalse("NodesInfo returned no nodes", response.getNodes().isEmpty());
        for (NodeInfo nodeInfo : response.getNodes()) {
            PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
            assertNotNull("node " + nodeInfo.getNode().getName() + " reported no plugins metric", plugins);
            Set<String> names = plugins.getPluginInfos().stream().map(PluginInfo::getName).collect(Collectors.toSet());
            assertFalse(
                "node " + nodeInfo.getNode().getName() + " unexpectedly loaded " + DSL_QUERY_EXECUTOR_PLUGIN + "; loaded=" + names,
                names.contains(DSL_QUERY_EXECUTOR_PLUGIN)
            );
        }
    }

    /** (3) A plain _search on an ordinary index still returns its hit with the full stack loaded (normal search path). */
    public void testPlainSearchReturnsHitWithStackLoaded() {
        String index = "sandbox-wiring-idx";
        createIndex(index);
        client().prepareIndex(index).setId("1").setSource("field", "value").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        SearchResponse response = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(response, 1L);
        assertSearchHits(response, "1");
    }

    /**
     * (4) Every node registers BOTH analytics backends (datafusion + lucene) with AnalyticsPlugin. The backend list is
     * read through {@code AnalyticsSearchService#getRegisteredBackendNames} — the engine's node-level component, looked
     * up reflectively so this IT stays free of any compile dependency on the sandbox plugins.
     */
    public void testAnalyticsBackendsRegisteredOnEveryNode() throws Exception {
        Class<?> serviceClass = SandboxStackTestUtils.analyticsSearchServiceClass();
        for (String nodeName : internalCluster().getNodeNames()) {
            Object service = internalCluster().getInstance(serviceClass, nodeName);
            Set<String> backendNames = SandboxStackTestUtils.getRegisteredBackendNames(service);
            assertEquals("node " + nodeName + " registered backends " + backendNames, EXPECTED_BACKENDS, backendNames);
        }
    }

    /** (5) Both backend plugins report analytics-engine in their extendedPlugins metadata on every node. */
    public void testBackendPluginsReportExtendedAnalyticsPlugin() {
        NodesInfoResponse response = nodesInfoWithPlugins();
        assertFalse("NodesInfo returned no nodes", response.getNodes().isEmpty());
        for (NodeInfo nodeInfo : response.getNodes()) {
            PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
            assertNotNull("node " + nodeInfo.getNode().getName() + " reported no plugins metric", plugins);
            assertExtendsAnalyticsPlugin(nodeInfo, plugins, DATAFUSION_PLUGIN);
            assertExtendsAnalyticsPlugin(nodeInfo, plugins, LUCENE_PLUGIN);
        }
    }

    /** Asserts the given backend plugin is present on the node and lists {@link #ANALYTICS_PLUGIN} in its extendedPlugins. */
    private static void assertExtendsAnalyticsPlugin(NodeInfo nodeInfo, PluginsAndModules plugins, String pluginFqcn) {
        PluginInfo info = plugins.getPluginInfos().stream().filter(p -> pluginFqcn.equals(p.getName())).findFirst().orElse(null);
        assertNotNull("node " + nodeInfo.getNode().getName() + " missing plugin " + pluginFqcn, info);
        assertTrue(
            "node " + nodeInfo.getNode().getName() + " plugin " + pluginFqcn + " extendedPlugins=" + info.getExtendedPlugins(),
            info.getExtendedPlugins().contains(ANALYTICS_PLUGIN)
        );
    }
}
