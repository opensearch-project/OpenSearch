/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE;
import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Transport Action tests for Query Insights Plugin
 */

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class QueryInsightsPluginTransportIT extends OpenSearchIntegTestCase {

    private final int TOTAL_NUMBER_OF_NODES = 2;
    private final int TOTAL_SEARCH_REQUESTS = 5;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(QueryInsightsPlugin.class);
    }

    /**
     * Test Query Insights Plugin is installed
     */
    public void testQueryInsightPluginInstalled() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());
        Assert.assertTrue(
            pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("org.opensearch.plugin.insights.QueryInsightsPlugin"))
        );
    }

    /**
     * Test get top queries when feature disabled
     */
    public void testGetTopQueriesWhenFeatureDisabled() {
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();
        Assert.assertNotEquals(0, response.failures().size());
        Assert.assertEquals(
            "Cannot get top n queries for [latency] when it is not enabled.",
            response.failures().get(0).getCause().getCause().getMessage()
        );
    }

    /**
     * Test update top query record when feature enabled
     */
    public void testUpdateRecordWhenFeatureDisabledThenEnabled() throws ExecutionException, InterruptedException {
        Settings commonSettings = Settings.builder().put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "false").build();

        logger.info("--> starting nodes for query insight testing");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 2))
        );
        ensureStableCluster(2);
        logger.info("--> creating indices for query insight testing");
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex("test_" + i).setId("" + i).setSource("field_" + i, "value_" + i).get();
            assertEquals("CREATED", response.status().toString());
        }
        // making search requests to get top queries
        for (int i = 0; i < TOTAL_SEARCH_REQUESTS; i++) {
            SearchResponse searchResponse = internalCluster().client(randomFrom(nodes))
                .prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(searchResponse.getFailedShards(), 0);
        }

        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();
        Assert.assertNotEquals(0, response.failures().size());
        Assert.assertEquals(
            "Cannot get top n queries for [latency] when it is not enabled.",
            response.failures().get(0).getCause().getCause().getMessage()
        );

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true").build()
        );
        assertAcked(internalCluster().client().admin().cluster().updateSettings(updateSettingsRequest).get());
        TopQueriesRequest request2 = new TopQueriesRequest(MetricType.LATENCY);
        TopQueriesResponse response2 = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request2).actionGet();
        Assert.assertEquals(0, response2.failures().size());
        Assert.assertEquals(TOTAL_NUMBER_OF_NODES, response2.getNodes().size());
        for (int i = 0; i < TOTAL_NUMBER_OF_NODES; i++) {
            Assert.assertEquals(0, response2.getNodes().get(i).getTopQueriesRecord().size());
        }

        internalCluster().stopAllNodes();
    }

    /**
     * Test get top queries when feature enabled
     */
    public void testGetTopQueriesWhenFeatureEnabled() throws InterruptedException {
        Settings commonSettings = Settings.builder()
            .put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true")
            .put(TOP_N_LATENCY_QUERIES_SIZE.getKey(), "100")
            .put(TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "600s")
            .build();

        logger.info("--> starting nodes for query insight testing");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 2))
        );
        ensureStableCluster(2);
        logger.info("--> creating indices for query insight testing");
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex("test_" + i).setId("" + i).setSource("field_" + i, "value_" + i).get();
            assertEquals("CREATED", response.status().toString());
        }
        // making search requests to get top queries
        for (int i = 0; i < TOTAL_SEARCH_REQUESTS; i++) {
            SearchResponse searchResponse = internalCluster().client(randomFrom(nodes))
                .prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(searchResponse.getFailedShards(), 0);
        }
        // Sleep to wait for queue drained to top queries store
        Thread.sleep(6000);
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();
        Assert.assertEquals(0, response.failures().size());
        Assert.assertEquals(TOTAL_NUMBER_OF_NODES, response.getNodes().size());
        Assert.assertEquals(TOTAL_SEARCH_REQUESTS, response.getNodes().stream().mapToInt(o -> o.getTopQueriesRecord().size()).sum());

        internalCluster().stopAllNodes();
    }

    /**
     * Test get top queries with small top n size
     */
    public void testGetTopQueriesWithSmallTopN() throws InterruptedException {
        Settings commonSettings = Settings.builder()
            .put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true")
            .put(TOP_N_LATENCY_QUERIES_SIZE.getKey(), "1")
            .put(TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "600s")
            .build();

        logger.info("--> starting nodes for query insight testing");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 2))
        );
        ensureStableCluster(2);
        logger.info("--> creating indices for query insight testing");
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex("test_" + i).setId("" + i).setSource("field_" + i, "value_" + i).get();
            assertEquals("CREATED", response.status().toString());
        }
        // making search requests to get top queries
        for (int i = 0; i < TOTAL_SEARCH_REQUESTS; i++) {
            SearchResponse searchResponse = internalCluster().client(randomFrom(nodes))
                .prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(searchResponse.getFailedShards(), 0);
        }
        Thread.sleep(6000);
        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();
        Assert.assertEquals(0, response.failures().size());
        Assert.assertEquals(TOTAL_NUMBER_OF_NODES, response.getNodes().size());
        Assert.assertEquals(2, response.getNodes().stream().mapToInt(o -> o.getTopQueriesRecord().size()).sum());

        internalCluster().stopAllNodes();
    }

    /**
     * Test get top queries with small window size
     */
    public void testGetTopQueriesWithSmallWindowSize() throws InterruptedException {
        Settings commonSettings = Settings.builder()
            .put(TOP_N_LATENCY_QUERIES_ENABLED.getKey(), "true")
            .put(TOP_N_LATENCY_QUERIES_SIZE.getKey(), "100")
            .put(TOP_N_LATENCY_QUERIES_WINDOW_SIZE.getKey(), "1m")
            .build();

        logger.info("--> starting nodes for query insight testing");
        List<String> nodes = internalCluster().startNodes(TOTAL_NUMBER_OF_NODES, Settings.builder().put(commonSettings).build());

        logger.info("--> waiting for nodes to form a cluster");
        ClusterHealthResponse health = client().admin().cluster().prepareHealth().setWaitForNodes("2").execute().actionGet();
        assertFalse(health.isTimedOut());

        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 2))
        );
        ensureStableCluster(2);
        logger.info("--> creating indices for query insight testing");
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex("test_" + i).setId("" + i).setSource("field_" + i, "value_" + i).get();
            assertEquals("CREATED", response.status().toString());
        }
        // making search requests to get top queries
        for (int i = 0; i < TOTAL_SEARCH_REQUESTS; i++) {
            SearchResponse searchResponse = internalCluster().client(randomFrom(nodes))
                .prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            assertEquals(searchResponse.getFailedShards(), 0);
        }

        TopQueriesRequest request = new TopQueriesRequest(MetricType.LATENCY);
        TopQueriesResponse response = OpenSearchIntegTestCase.client().execute(TopQueriesAction.INSTANCE, request).actionGet();
        Assert.assertEquals(0, response.failures().size());
        Assert.assertEquals(TOTAL_NUMBER_OF_NODES, response.getNodes().size());
        Thread.sleep(6000);
        internalCluster().stopAllNodes();
    }
}
