/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollAction;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.TransportException;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opensearch.action.search.TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.NO_TIMEOUT;
import static org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase.ScriptedBlockPlugin;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class SearchCancellationIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private TimeValue requestCancellationTimeout = TimeValue.timeValueSeconds(1);
    private TimeValue clusterCancellationTimeout = TimeValue.timeValueMillis(1500);
    private TimeValue keepAlive = TimeValue.timeValueSeconds(5);

    public SearchCancellationIT(Settings settings) {
        super(settings);
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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        boolean lowLevelCancellation = randomBoolean();
        logger.info("Using lowLevelCancellation: {}", lowLevelCancellation);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), lowLevelCancellation)
            .build();
    }

    @After
    public void cleanup() {
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(Settings.builder().putNull("*")).get();
    }

    private List<ScriptedBlockPlugin> initBlockFactory() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }
        return plugins;
    }

    private void cancelSearch(String action) {
        ListTasksResponse listTasksResponse = client().admin().cluster().prepareListTasks().setActions(action).get();
        assertThat(listTasksResponse.getTasks(), hasSize(1));
        TaskInfo searchTask = listTasksResponse.getTasks().get(0);

        logger.info("Cancelling search");
        CancelTasksResponse cancelTasksResponse = client().admin().cluster().prepareCancelTasks().setTaskId(searchTask.getTaskId()).get();
        assertThat(cancelTasksResponse.getTasks(), hasSize(1));
        assertThat(cancelTasksResponse.getTasks().get(0).getTaskId(), equalTo(searchTask.getTaskId()));
    }

    private SearchResponse ensureSearchWasCancelled(ActionFuture<SearchResponse> searchResponse) {
        try {
            SearchResponse response = searchResponse.actionGet();
            logger.info("Search response {}", response);
            assertNotEquals("At least one shard should have failed", 0, response.getFailedShards());
            verifyCancellationException(response.getShardFailures());
            return response;
        } catch (SearchPhaseExecutionException ex) {
            logger.info("All shards failed with", ex);
            verifyCancellationException(ex.shardFailures());
            return null;
        }
    }

    private void ensureMSearchWasCancelled(ActionFuture<MultiSearchResponse> mSearchResponse, Set<Integer> expectedFailedChildRequests) {
        MultiSearchResponse response = mSearchResponse.actionGet();
        Set<Integer> actualFailedChildRequests = new HashSet<>();
        for (int i = 0; i < response.getResponses().length; ++i) {
            SearchResponse sResponse = response.getResponses()[i].getResponse();
            // check if response is null means all the shard failed for this search request
            if (sResponse == null) {
                Exception ex = response.getResponses()[i].getFailure();
                assertTrue(ex instanceof SearchPhaseExecutionException);
                verifyCancellationException(((SearchPhaseExecutionException) ex).shardFailures());
                actualFailedChildRequests.add(i);

            } else if (sResponse.getShardFailures().length > 0) {
                verifyCancellationException(sResponse.getShardFailures());
                actualFailedChildRequests.add(i);
            }
        }
        assertEquals(
            "Actual child request with cancellation failure is different that expected",
            expectedFailedChildRequests,
            actualFailedChildRequests
        );
    }

    private void verifyCancellationException(ShardSearchFailure[] failures) {
        for (ShardSearchFailure searchFailure : failures) {
            // failure may happen while executing the search or while sending shard request for next phase.
            // Below assertion is handling both the cases
            final Throwable topFailureCause = searchFailure.getCause();
            assertTrue(
                searchFailure.toString(),
                topFailureCause instanceof TransportException || topFailureCause instanceof TaskCancelledException
            );
            if (topFailureCause instanceof TransportException) {
                assertTrue(topFailureCause.getCause() instanceof TaskCancelledException);
            }
        }
    }

    public void testCancellationDuringQueryPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(MediaTypeRegistry.JSON, client().admin().indices().prepareSegments("test").get()));
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringQueryPhaseUsingRequestParameter() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setCancelAfterTimeInterval(requestCancellationTimeout)
            .setAllowPartialSearchResults(randomBoolean())
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(requestCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringQueryPhaseUsingClusterSetting() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, clusterCancellationTimeout).build()
            )
            .get();
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setAllowPartialSearchResults(randomBoolean())
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(clusterCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringFetchPhase() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .addScriptField(
                "test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())
            )
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        logger.info("Segments {}", Strings.toString(MediaTypeRegistry.JSON, client().admin().indices().prepareSegments("test").get()));
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationDuringFetchPhaseUsingRequestParameter() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setCancelAfterTimeInterval(requestCancellationTimeout)
            .addScriptField(
                "test_field",
                new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())
            )
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(requestCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        ensureSearchWasCancelled(searchResponse);
    }

    public void testCancellationOfScrollSearches() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        logger.info("Executing search");
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setScroll(keepAlive)
            .setSize(5)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchAction.NAME);
        disableBlocks(plugins);
        SearchResponse response = ensureSearchWasCancelled(searchResponse);
        if (response != null) {
            // The response might not have failed on all shards - we need to clean scroll
            logger.info("Cleaning scroll with id {}", response.getScrollId());
            client().prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
    }

    public void testCancellationOfFirstScrollSearchRequestUsingRequestParameter() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setScroll(keepAlive)
            .setCancelAfterTimeInterval(requestCancellationTimeout)
            .setSize(5)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();

        awaitForBlock(plugins);
        sleepForAtLeast(requestCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        SearchResponse response = ensureSearchWasCancelled(searchResponse);
        if (response != null) {
            // The response might not have failed on all shards - we need to clean scroll
            logger.info("Cleaning scroll with id {}", response.getScrollId());
            client().prepareClearScroll().addScrollId(response.getScrollId()).get();
        }
    }

    public void testCancellationOfScrollSearchesOnFollowupRequests() throws Exception {

        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        // Disable block so the first request would pass
        disableBlocks(plugins);

        logger.info("Executing search");
        SearchResponse searchResponse = client().prepareSearch("test")
            .setScroll(keepAlive)
            .setSize(2)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .get();

        assertNotNull(searchResponse.getScrollId());

        // Enable block so the second request would block
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }

        String scrollId = searchResponse.getScrollId();
        logger.info("Executing scroll with id {}", scrollId);
        ActionFuture<SearchResponse> scrollResponse = client().prepareSearchScroll(searchResponse.getScrollId())
            .setScroll(keepAlive)
            .execute();

        awaitForBlock(plugins);
        cancelSearch(SearchScrollAction.NAME);
        disableBlocks(plugins);

        SearchResponse response = ensureSearchWasCancelled(scrollResponse);
        if (response != null) {
            // The response didn't fail completely - update scroll id
            scrollId = response.getScrollId();
        }
        logger.info("Cleaning scroll with id {}", scrollId);
        client().prepareClearScroll().addScrollId(scrollId).get();
    }

    public void testNoCancellationOfScrollSearchOnFollowUpRequest() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());

        // Disable block so the first request would pass
        disableBlocks(plugins);
        SearchResponse searchResponse = client().prepareSearch("test")
            .setScroll(keepAlive)
            .setCancelAfterTimeInterval(requestCancellationTimeout)
            .setSize(2)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .get();

        assertNotNull(searchResponse.getScrollId());
        // since the previous scroll response is received before cancellation timeout, the scheduled task will be cancelled. It will not
        // be used for the subsequent scroll request, as request is of SearchScrollRequest type instead of SearchRequest type
        // Enable block so the second request would block
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.reset();
            plugin.enableBlock();
        }

        String scrollId = searchResponse.getScrollId();
        ActionFuture<SearchResponse> scrollResponse = client().prepareSearchScroll(searchResponse.getScrollId())
            .setScroll(keepAlive)
            .execute();

        awaitForBlock(plugins);
        sleepForAtLeast(requestCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);

        // wait for response and ensure there is no failure
        SearchResponse response = scrollResponse.get();
        assertEquals(0, response.getFailedShards());
        scrollId = response.getScrollId();
        client().prepareClearScroll().addScrollId(scrollId).get();
    }

    public void testDisableCancellationAtRequestLevel() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, clusterCancellationTimeout).build()
            )
            .get();
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setAllowPartialSearchResults(randomBoolean())
            .setCancelAfterTimeInterval(NO_TIMEOUT)
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(clusterCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        // ensure search was successful since cancellation was disabled at request level
        assertEquals(0, searchResponse.get().getFailedShards());
    }

    public void testDisableCancellationAtClusterLevel() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, NO_TIMEOUT).build())
            .get();
        ActionFuture<SearchResponse> searchResponse = client().prepareSearch("test")
            .setAllowPartialSearchResults(randomBoolean())
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())))
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(clusterCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        // ensure search was successful since cancellation was disabled at request level
        assertEquals(0, searchResponse.get().getFailedShards());
    }

    public void testCancelMultiSearch() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        ActionFuture<MultiSearchResponse> msearchResponse = client().prepareMultiSearch()
            .add(
                client().prepareSearch("test")
                    .addScriptField(
                        "test_field",
                        new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())
                    )
            )
            .execute();
        awaitForBlock(plugins);
        cancelSearch(MultiSearchAction.NAME);
        disableBlocks(plugins);
        for (MultiSearchResponse.Item item : msearchResponse.actionGet()) {
            if (item.getFailure() != null) {
                assertThat(ExceptionsHelper.unwrap(item.getFailure(), TaskCancelledException.class), notNullValue());
            } else {
                assertFailures(item.getResponse());
                for (ShardSearchFailure shardFailure : item.getResponse().getShardFailures()) {
                    assertThat(ExceptionsHelper.unwrap(shardFailure.getCause(), TaskCancelledException.class), notNullValue());
                }
            }
        }
    }

    public void testMSearchChildRequestCancellationWithClusterLevelTimeout() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, clusterCancellationTimeout).build()
            )
            .get();
        ActionFuture<MultiSearchResponse> mSearchResponse = client().prepareMultiSearch()
            .setMaxConcurrentSearchRequests(2)
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(randomBoolean())
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(randomBoolean())
                    .setRequestCache(false)
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(clusterCancellationTimeout.getMillis());
        // unblock the search thread
        disableBlocks(plugins);
        // both child requests are expected to fail
        final Set<Integer> expectedFailedRequests = new HashSet<>();
        expectedFailedRequests.add(0);
        expectedFailedRequests.add(1);
        ensureMSearchWasCancelled(mSearchResponse, expectedFailedRequests);
    }

    /**
     * Verifies cancellation of sub search request with mix of request level and cluster level timeout parameter
     * @throws Exception in case of unexpected errors
     */
    public void testMSearchChildReqCancellationWithHybridTimeout() throws Exception {
        List<ScriptedBlockPlugin> plugins = initBlockFactory();
        indexTestData(client());
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING_KEY, clusterCancellationTimeout).build()
            )
            .get();
        ActionFuture<MultiSearchResponse> mSearchResponse = client().prepareMultiSearch()
            .setMaxConcurrentSearchRequests(3)
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(randomBoolean())
                    .setCancelAfterTimeInterval(requestCancellationTimeout)
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(randomBoolean())
                    .setCancelAfterTimeInterval(NO_TIMEOUT)
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .add(
                client().prepareSearch("test")
                    .setAllowPartialSearchResults(randomBoolean())
                    .setRequestCache(false)
                    .setQuery(
                        scriptQuery(new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap()))
                    )
            )
            .execute();
        awaitForBlock(plugins);
        sleepForAtLeast(Math.max(requestCancellationTimeout.getMillis(), clusterCancellationTimeout.getMillis()));
        // unblock the search thread
        disableBlocks(plugins);
        // only first and last child request are expected to fail
        final Set<Integer> expectedFailedRequests = new HashSet<>();
        expectedFailedRequests.add(0);
        expectedFailedRequests.add(2);
        ensureMSearchWasCancelled(mSearchResponse, expectedFailedRequests);
    }

    /**
     * Sleeps for the specified number of milliseconds plus a 100ms buffer to account for system timer/scheduler inaccuracies.
     *
     * @param milliseconds The minimum time to sleep
     * @throws InterruptedException if interrupted during sleep
     */
    private static void sleepForAtLeast(long milliseconds) throws InterruptedException {
        Thread.sleep(milliseconds + 100L);
    }
}
