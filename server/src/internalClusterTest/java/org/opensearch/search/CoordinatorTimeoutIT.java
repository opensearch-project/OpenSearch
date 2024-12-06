/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.ReceiveTimeoutTransportException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class CoordinatorTimeoutIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private long coordinatorTimeoutMills = 500;

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

    public void testTimeoutDuringQueryPhase() throws Exception {
        int dataNumber = internalCluster().numDataNodes();
        createIndex("test", Settings.builder().put("index.number_of_shards", dataNumber).put("index.number_of_replicas", 0).build());

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        TimeValue coordinatorTimeout = new TimeValue(coordinatorTimeoutMills, TimeUnit.MILLISECONDS);
        ActionFuture<SearchResponse> searchResponseFuture = client().prepareSearch("test")
            .setCoordinatorTimeout(coordinatorTimeout)
            .setAllowPartialSearchResults(true)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();
        awaitForBlock(plugins);
        logger.info("begin to sleep for " + coordinatorTimeout.getMillis() + " ms");
        Thread.sleep(coordinatorTimeout.getMillis() + 100);
        logger.info("wake up");
        disableBlocks(plugins);
        SearchResponse searchResponse = searchResponseFuture.get();
        assertEquals(1, searchResponse.getSuccessfulShards());
        verifyFailedException(searchResponse.getShardFailures());
        // wait in-flight contexts to finish
        Thread.sleep(100);
    }

    public void testMSearchChildRequestTimeout() throws Exception {
        int dataNumber = internalCluster().numDataNodes();
        createIndex("test", Settings.builder().put("index.number_of_shards", dataNumber).put("index.number_of_replicas", 0).build());

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        TimeValue coordinatorTimeout = new TimeValue(coordinatorTimeoutMills, TimeUnit.MILLISECONDS);
        ActionFuture<MultiSearchResponse> mSearchResponse = client().prepareMultiSearch()
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(true)
                    .setRequestCache(false)
                    .setCoordinatorTimeout(coordinatorTimeout)
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(true)
                    .setRequestCache(false)
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .execute();
        awaitForBlock(plugins);
        Thread.sleep(coordinatorTimeout.getMillis() + 100);
        // unblock the search thread
        disableBlocks(plugins);
        // one child request is expected to fail
        final Set<Integer> expectedFailedRequests = new HashSet<>();
        expectedFailedRequests.add(0);
        ensureMSearchThrowException(mSearchResponse, expectedFailedRequests);
        // wait in-flight contexts to finish
        Thread.sleep(100);
    }

    private void verifyFailedException(ShardSearchFailure[] shardFailures) {
        for (ShardSearchFailure shardFailure : shardFailures) {
            final Throwable topFailureCause = shardFailure.getCause();
            assertTrue(shardFailure.toString(), topFailureCause instanceof ReceiveTimeoutTransportException);
        }
    }

    private void ensureMSearchThrowException(ActionFuture<MultiSearchResponse> mSearchResponse, Set<Integer> expectedFailedChildRequests) {
        MultiSearchResponse response = mSearchResponse.actionGet();
        Set<Integer> actualFailedChildRequests = new HashSet<>();
        for (int i = 0; i < response.getResponses().length; ++i) {
            SearchResponse sResponse = response.getResponses()[i].getResponse();
            // check if response is null means all the shard failed for this search request
            if (sResponse == null) {
                Exception ex = response.getResponses()[i].getFailure();
                assertTrue(ex instanceof SearchPhaseExecutionException);
                verifyFailedException(((SearchPhaseExecutionException) ex).shardFailures());
                actualFailedChildRequests.add(i);

            } else if (sResponse.getShardFailures().length > 0) {
                verifyFailedException(sResponse.getShardFailures());
                actualFailedChildRequests.add(i);
            }
        }
        assertEquals(
            "Actual child request with timeout failure is different that expected",
            expectedFailedChildRequests,
            actualFailedChildRequests
        );
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        boolean notBlockFirst = true;
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            List<ScriptedBlockPlugin> scriptedBlockPlugins = pluginsService.filterPlugins(ScriptedBlockPlugin.class);
            for (ScriptedBlockPlugin plugin : scriptedBlockPlugins) {
                plugin.reset();
                // just block the first node
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
