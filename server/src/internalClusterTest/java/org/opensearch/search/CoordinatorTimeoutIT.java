/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchTransportService.CoordinatorTimeoutStrategy;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.NO_TIMEOUT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.instanceOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class CoordinatorTimeoutIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String INDEX = "test";
    private static final TimeValue COORDINATOR_TIMEOUT = TimeValue.timeValueMillis(500);

    private String coordinatorNode;

    public CoordinatorTimeoutIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(ScriptedBlockPlugin.class);
    }

    @Before
    public void startCluster() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.total_shards_per_node", 1)
            )
        );
        ensureGreen(INDEX);
        ensureStableCluster(4, coordinatorNode);
    }

    public void testRequestCoordinatorTimeoutStrategyReturnsPartialResultsAfterTimeout() throws Exception {
        indexTestData();
        assertSearchReturnsPartialResultsAfterCoordinatorTimeout(newSearchRequest(true, true, true));
    }

    public void testFailsWhenPartialResultsAreDisallowed() throws Exception {
        indexTestData();
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        try {
            client(coordinatorNode).search(newSearchRequest(false, true, true), future);

            awaitForBlock(plugins);
            sleepForAtLeast(COORDINATOR_TIMEOUT.getMillis());
            disableBlocks(plugins);

            SearchPhaseExecutionException exception = expectThrows(
                SearchPhaseExecutionException.class,
                () -> future.actionGet(10, TimeUnit.SECONDS)
            );
            assertEquals(1, exception.shardFailures().length);
            assertThat(exception.shardFailures()[0].getCause(), instanceOf(ReceiveTimeoutTransportException.class));
        } finally {
            disableBlocks(plugins);
        }
    }

    public void testNoCoordinatorTimeoutStrategyWaitsForBlockedShard() throws Exception {
        indexTestData();
        assertSearchWaitsForBlockedShard(newSearchRequest(true, true, false));
    }

    public void testNoTimeoutWaitsForBlockedShard() throws Exception {
        indexTestData();
        assertSearchWaitsForBlockedShard(newSearchRequest(true, false, true));
    }

    public void testMultiSearchRequestCoordinatorTimeoutStrategyReturnsPartialResults() throws Exception {
        indexTestData();
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        PlainActionFuture<MultiSearchResponse> future = PlainActionFuture.newFuture();
        try {
            client(coordinatorNode).multiSearch(new MultiSearchRequest().add(newSearchRequest(true, true, true)), future);

            awaitForBlock(plugins);
            sleepForAtLeast(COORDINATOR_TIMEOUT.getMillis());
            disableBlocks(plugins);

            MultiSearchResponse response = future.actionGet(10, TimeUnit.SECONDS);
            assertEquals(1, response.getResponses().length);
            assertFalse(response.getResponses()[0].isFailure());
            SearchResponse searchResponse = response.getResponses()[0].getResponse();
            assertEquals(2, searchResponse.getTotalShards());
            assertEquals(1, searchResponse.getSuccessfulShards());
            assertEquals(1, searchResponse.getFailedShards());
            verifyFailedException(searchResponse.getShardFailures());
        } finally {
            disableBlocks(plugins);
        }
    }

    private void verifyFailedException(ShardSearchFailure[] shardFailures) {
        for (ShardSearchFailure shardFailure : shardFailures) {
            final Throwable topFailureCause = shardFailure.getCause();
            assertTrue(shardFailure.toString(), topFailureCause instanceof ReceiveTimeoutTransportException);
        }
    }

    private SearchRequest newSearchRequest(boolean allowPartialResults, boolean timeoutEnabled, boolean failStrategy) {
        TimeValue timeout = timeoutEnabled ? COORDINATOR_TIMEOUT : NO_TIMEOUT;
        SearchRequest request = new SearchRequest(INDEX).allowPartialSearchResults(allowPartialResults)
            .source(
                new SearchSourceBuilder().query(
                    scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                ).timeout(timeout)
            );
        if (failStrategy) {
            request.setCoordinatorTimeoutStrategy(CoordinatorTimeoutStrategy.FAIL.getType());
        }
        return request;
    }

    private void assertSearchReturnsPartialResultsAfterCoordinatorTimeout(SearchRequest request) throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        try {
            client(coordinatorNode).search(request, future);

            awaitForBlock(plugins);
            sleepForAtLeast(COORDINATOR_TIMEOUT.getMillis());
            disableBlocks(plugins);

            SearchResponse searchResponse = future.actionGet(10, TimeUnit.SECONDS);
            assertEquals(2, searchResponse.getTotalShards());
            assertEquals(1, searchResponse.getSuccessfulShards());
            assertEquals(1, searchResponse.getFailedShards());
            verifyFailedException(searchResponse.getShardFailures());
        } finally {
            disableBlocks(plugins);
        }
    }

    private void assertSearchWaitsForBlockedShard(SearchRequest request) throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        PlainActionFuture<SearchResponse> future = PlainActionFuture.newFuture();
        try {
            client(coordinatorNode).search(request, future);

            awaitForBlock(plugins);
            assertFalse(future.isDone());
            disableBlocks(plugins);

            SearchResponse searchResponse = future.actionGet(10, TimeUnit.SECONDS);
            assertEquals(2, searchResponse.getTotalShards());
            assertEquals(2, searchResponse.getSuccessfulShards());
            assertEquals(0, searchResponse.getFailedShards());
        } finally {
            disableBlocks(plugins);
        }
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        boolean notBlockFirst = true;
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            List<ScriptedBlockPlugin> scriptedBlockPlugins = pluginsService.filterPlugins(ScriptedBlockPlugin.class);
            for (ScriptedBlockPlugin plugin : scriptedBlockPlugins) {
                plugin.reset();
                // just block one data node
                if (notBlockFirst) {
                    notBlockFirst = false;
                    // default is enable block
                    plugin.disableBlock();
                } else {
                    plugin.enableBlock();
                }
            }
            plugins.addAll(scriptedBlockPlugins);
        }
        return plugins;
    }

}
