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
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;

/**
 * Single-node mirror of {@link SandboxStackWiringIT}: verifies the sandbox engine-stack wiring installed by
 * {@link OpenSearchSingleNodeTestCase} when {@code -Dsandbox.enabled=true}, so the server internal-cluster suites based
 * on the single-node base class also exercise the stack (PR #22698 review finding 4). Like its sister IT it runs only
 * when the sandbox modules are on the internalClusterTest classpath, so it is written WITHOUT any compile dependency on
 * them: every sandbox plugin is referenced by its String FQCN.
 * <p>
 * It asserts, on the one background node:
 * <ol>
 *   <li>all 7 stack plugins are loaded, delivered as {@link PluginInfo}s carrying {@code extendedPlugins} metadata;</li>
 *   <li>the stack deliberately EXCLUDES dsl-query-executor, so the node does not report that plugin;</li>
 *   <li>both backend plugins (datafusion + lucene) report {@code analytics-engine} in their {@code extendedPlugins};</li>
 *   <li>both analytics backends are registered with AnalyticsPlugin (via {@link SandboxStackTestUtils});</li>
 *   <li>a plain {@code _search} still returns its hit on the normal (fallback) search path.</li>
 * </ol>
 */
public class SandboxStackSingleNodeWiringIT extends OpenSearchSingleNodeTestCase {

    private static final String ARROW_BASE_PLUGIN = "org.opensearch.arrow.allocator.ArrowBasePlugin";
    private static final String FLIGHT_STREAM_PLUGIN = "org.opensearch.arrow.flight.transport.FlightStreamPlugin";
    private static final String ANALYTICS_PLUGIN = "org.opensearch.analytics.AnalyticsPlugin";
    private static final String COMPOSITE_DATAFORMAT_PLUGIN = "org.opensearch.composite.CompositeDataFormatPlugin";
    private static final String PARQUET_DATAFORMAT_PLUGIN = "org.opensearch.parquet.ParquetDataFormatPlugin";
    private static final String DATAFUSION_PLUGIN = "org.opensearch.be.datafusion.DataFusionPlugin";
    private static final String LUCENE_PLUGIN = "org.opensearch.be.lucene.LucenePlugin";

    /** All 7 sandbox stack plugin FQCNs that must be loaded on the node when the stack is enabled. */
    private static final List<String> STACK_PLUGINS = List.of(
        ARROW_BASE_PLUGIN,
        FLIGHT_STREAM_PLUGIN,
        ANALYTICS_PLUGIN,
        COMPOSITE_DATAFORMAT_PLUGIN,
        PARQUET_DATAFORMAT_PLUGIN,
        DATAFUSION_PLUGIN,
        LUCENE_PLUGIN
    );

    /** The DSL query-executor plugin FQCN; deliberately NOT part of the stack, so it must not be loaded on the node. */
    private static final String DSL_QUERY_EXECUTOR_PLUGIN = "org.opensearch.dsl.DslQueryExecutorPlugin";

    /** The two analytics backend ids that must be registered with AnalyticsPlugin once DataFusion and Lucene extend it. */
    private static final Set<String> EXPECTED_BACKENDS = Set.of("datafusion", "lucene");

    /** Skip entirely unless the build forwarded -Dsandbox.enabled=true (the only mode where the stack is on the classpath). */
    @Before
    public void skipWithoutSandbox() {
        assumeTrue("requires -Dsandbox.enabled=true", Boolean.parseBoolean(System.getProperty("sandbox.enabled", "false")));
    }

    private PluginsAndModules pluginsOnNode() {
        NodesInfoResponse response = client().admin()
            .cluster()
            .prepareNodesInfo()
            .addMetric(NodesInfoRequest.Metric.PLUGINS.metricName())
            .get();
        assertFalse("NodesInfo returned no nodes", response.getNodes().isEmpty());
        NodeInfo nodeInfo = response.getNodes().get(0);
        PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
        assertNotNull("node " + nodeInfo.getNode().getName() + " reported no plugins metric", plugins);
        return plugins;
    }

    /** (1) The single node reports all 7 stack plugins loaded. */
    public void testStackPluginsLoadedOnNode() {
        Set<String> names = pluginsOnNode().getPluginInfos().stream().map(PluginInfo::getName).collect(Collectors.toSet());
        for (String fqcn : STACK_PLUGINS) {
            assertTrue("node missing stack plugin " + fqcn + "; loaded=" + names, names.contains(fqcn));
        }
    }

    /** (2) The stack excludes dsl-query-executor, so the node does not report that plugin. */
    public void testDslQueryExecutorNotLoaded() {
        Set<String> names = pluginsOnNode().getPluginInfos().stream().map(PluginInfo::getName).collect(Collectors.toSet());
        assertFalse(
            "node unexpectedly loaded " + DSL_QUERY_EXECUTOR_PLUGIN + "; loaded=" + names,
            names.contains(DSL_QUERY_EXECUTOR_PLUGIN)
        );
    }

    /** (3) Both backend plugins report analytics-engine in their extendedPlugins metadata. */
    public void testBackendPluginsReportExtendedAnalyticsPlugin() {
        PluginsAndModules plugins = pluginsOnNode();
        assertExtendsAnalyticsPlugin(plugins, DATAFUSION_PLUGIN);
        assertExtendsAnalyticsPlugin(plugins, LUCENE_PLUGIN);
    }

    /** (4) The node registers BOTH analytics backends (datafusion + lucene) with AnalyticsPlugin. */
    public void testAnalyticsBackendsRegisteredOnNode() throws Exception {
        Object service = node().injector().getInstance(SandboxStackTestUtils.analyticsSearchServiceClass());
        Set<String> backendNames = SandboxStackTestUtils.getRegisteredBackendNames(service);
        assertEquals("node registered backends " + backendNames, EXPECTED_BACKENDS, backendNames);
    }

    /** (5) A plain _search on an ordinary index still returns its hit with the full stack loaded (normal search path). */
    public void testPlainSearchReturnsHitWithStackLoaded() {
        String index = "sandbox-singlenode-wiring-idx";
        createIndex(index);
        client().prepareIndex(index).setId("1").setSource("field", "value").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        SearchResponse response = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(response, 1L);
        assertSearchHits(response, "1");
    }

    /** Asserts the given backend plugin is present on the node and lists {@link #ANALYTICS_PLUGIN} in its extendedPlugins. */
    private static void assertExtendsAnalyticsPlugin(PluginsAndModules plugins, String pluginFqcn) {
        PluginInfo info = plugins.getPluginInfos().stream().filter(p -> pluginFqcn.equals(p.getName())).findFirst().orElse(null);
        assertNotNull("node missing plugin " + pluginFqcn, info);
        assertTrue(
            "node plugin " + pluginFqcn + " extendedPlugins=" + info.getExtendedPlugins(),
            info.getExtendedPlugins().contains(ANALYTICS_PLUGIN)
        );
    }
}
