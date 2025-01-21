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

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.core.tasks.resourcetracker.TaskResourceUsage;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static org.opensearch.tasks.TaskResourceTrackingService.TASK_RESOURCE_USAGE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractSearchAsyncActionTests extends OpenSearchTestCase {

    private final List<Tuple<String, String>> resolvedNodes = new ArrayList<>();
    private final Set<ShardSearchContextId> releasedContexts = new CopyOnWriteArraySet<>();
    private ExecutorService executor;
    private SearchRequestOperationsListenerAssertingListener assertingListener;
    ThreadPool threadPool;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newFixedThreadPool(1);
        threadPool = new TestThreadPool(getClass().getName());
        assertingListener = new SearchRequestOperationsListenerAssertingListener();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
        assertingListener.assertFinished();
    }

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
        SearchRequest request,
        ArraySearchPhaseResults<SearchPhaseResult> results,
        ActionListener<SearchResponse> listener,
        final boolean controlled,
        final AtomicLong expected,
        final TaskResourceUsage resourceUsage
    ) {
        return createAction(
            request,
            results,
            listener,
            controlled,
            false,
            false,
            expected,
            resourceUsage,
            new SearchShardIterator(null, null, Collections.emptyList(), null)
        );
    }

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
        SearchRequest request,
        ArraySearchPhaseResults<SearchPhaseResult> results,
        ActionListener<SearchResponse> listener,
        final boolean controlled,
        final boolean failExecutePhaseOnShard,
        final boolean catchExceptionWhenExecutePhaseOnShard,
        final AtomicLong expected,
        final TaskResourceUsage resourceUsage,
        final SearchShardIterator... shards
    ) {

        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);
        }

        BiFunction<String, String, Transport.Connection> nodeIdToConnection = (cluster, node) -> {
            resolvedNodes.add(Tuple.tuple(cluster, node));
            return null;
        };

        TaskResourceInfo taskResourceInfo = new TaskResourceInfo.Builder().setTaskResourceUsage(resourceUsage)
            .setTaskId(randomLong())
            .setParentTaskId(randomLong())
            .setAction(randomAlphaOfLengthBetween(1, 5))
            .setNodeId(randomAlphaOfLengthBetween(1, 5))
            .build();
        threadPool.getThreadContext().addResponseHeader(TASK_RESOURCE_USAGE, taskResourceInfo.toString());

        return new AbstractSearchAsyncAction<SearchPhaseResult>(
            "test",
            logger,
            null,
            nodeIdToConnection,
            Collections.singletonMap("foo", new AliasFilter(new MatchAllQueryBuilder())),
            Collections.singletonMap("foo", 2.0f),
            Collections.singletonMap("name", Sets.newHashSet("bar", "baz")),
            executor,
            request,
            listener,
            new GroupShardsIterator<>(Arrays.asList(shards)),
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            results,
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY,
            new SearchRequestContext(
                new SearchRequestOperationsListener.CompositeListener(List.of(assertingListener), LogManager.getLogger()),
                request,
                () -> null
            ),
            NoopTracer.INSTANCE
        ) {
            @Override
            protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                return null;
            }

            @Override
            protected void executePhaseOnShard(
                final SearchShardIterator shardIt,
                final SearchShardTarget shard,
                final SearchActionListener<SearchPhaseResult> listener
            ) {
                if (failExecutePhaseOnShard) {
                    listener.onFailure(new ShardNotFoundException(shardIt.shardId()));
                } else {
                    if (catchExceptionWhenExecutePhaseOnShard) {
                        try {
                            listener.onResponse(new QuerySearchResult());
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    } else {
                        listener.onResponse(new QuerySearchResult());
                    }
                }
            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }

            @Override
            public void sendReleaseSearchContext(
                ShardSearchContextId contextId,
                Transport.Connection connection,
                OriginalIndices originalIndices
            ) {
                releasedContexts.add(contextId);
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            new SearchRequest(),
            new ArraySearchPhaseResults<>(10),
            null,
            controlled,
            expected,
            new TaskResourceUsage(0, 0)
        );
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

    public void testBuildShardSearchTransportRequest() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean()).preference("_shards:1,3");
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            new ArraySearchPhaseResults<>(10),
            null,
            false,
            expected,
            new TaskResourceUsage(randomLong(), randomLong())
        );
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardIterator iterator = new SearchShardIterator(
            clusterAlias,
            new ShardId(new Index("name", "foo"), 1),
            Collections.emptyList(),
            new OriginalIndices(new String[] { "name", "name1" }, IndicesOptions.strictExpand())
        );
        ShardSearchRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
        assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
        assertArrayEquals(new String[] { "name", "name1" }, shardSearchTransportRequest.indices());
        assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
        assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
        assertArrayEquals(new String[] { "name", "name1" }, shardSearchTransportRequest.indices());
        assertArrayEquals(new String[] { "bar", "baz" }, shardSearchTransportRequest.indexRoutings());
        assertEquals("_shards:1,3", shardSearchTransportRequest.preference());
        assertEquals(clusterAlias, shardSearchTransportRequest.getClusterAlias());
    }

    public void testBuildSearchResponse() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(10);
        TaskResourceUsage taskResourceUsage = new TaskResourceUsage(randomLong(), randomLong());
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            phaseResults,
            null,
            false,
            new AtomicLong(),
            taskResourceUsage
        );
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        List<String> resourceUsages = threadPool.getThreadContext().getResponseHeaders().get(TASK_RESOURCE_USAGE);
        assertNotNull(resourceUsages);
        assertEquals(1, resourceUsages.size());
        assertTrue(resourceUsages.get(0).contains(Long.toString(taskResourceUsage.getCpuTimeInNanos())));
        assertTrue(resourceUsages.get(0).contains(Long.toString(taskResourceUsage.getMemoryInBytes())));
    }

    public void testBuildSearchResponseAllowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(10);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            null,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong())
        );
        action.onShardFailure(
            0,
            new SearchShardTarget("node", new ShardId("index", "index-uuid", 0), null, OriginalIndices.NONE),
            new IllegalArgumentException()
        );
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testSendSearchResponseDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        int numFailures = randomIntBetween(1, 5);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, numFailures);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            phaseResults,
            listener,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong())
        );
        for (int i = 0; i < numFailures; i++) {
            ShardId failureShardId = new ShardId("index", "index-uuid", i);
            String failureClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String failureNodeId = randomAlphaOfLengthBetween(5, 10);
            action.onShardFailure(
                i,
                new SearchShardTarget(failureNodeId, failureShardId, failureClusterAlias, OriginalIndices.NONE),
                new IllegalArgumentException()
            );
        }
        action.sendSearchResponse(InternalSearchResponse.empty(), phaseResults.results);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(numFailures, searchPhaseExecutionException.shardFailures().length);
        for (ShardSearchFailure shardSearchFailure : searchPhaseExecutionException.shardFailures()) {
            assertThat(shardSearchFailure.getCause(), instanceOf(IllegalArgumentException.class));
        }
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testOnPhaseFailureAndVerifyListeners() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testListener = new SearchRequestStats(clusterSettings);

        final List<SearchRequestOperationsListener> requestOperationListeners = List.of(testListener, assertingListener);
        SearchQueryThenFetchAsyncAction action = createSearchQueryThenFetchAsyncAction(requestOperationListeners);
        action.start();
        assertEquals(1, testListener.getPhaseCurrent(action.getSearchPhaseName()));
        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertEquals(0, testListener.getPhaseCurrent(action.getSearchPhaseName()));
        assertEquals(0, testListener.getPhaseTotal(action.getSearchPhaseName()));

        SearchDfsQueryThenFetchAsyncAction searchDfsQueryThenFetchAsyncAction = createSearchDfsQueryThenFetchAsyncAction(
            requestOperationListeners
        );
        searchDfsQueryThenFetchAsyncAction.start();
        assertEquals(1, testListener.getPhaseCurrent(searchDfsQueryThenFetchAsyncAction.getSearchPhaseName()));
        searchDfsQueryThenFetchAsyncAction.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertEquals(0, testListener.getPhaseCurrent(action.getSearchPhaseName()));
        assertEquals(0, testListener.getPhaseTotal(action.getSearchPhaseName()));

        FetchSearchPhase fetchPhase = createFetchSearchPhase();
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        searchShardIterator.resetAndSkip();
        action.skipShard(searchShardIterator);
        action.start();
        action.executeNextPhase(action, fetchPhase);
        assertEquals(1, testListener.getPhaseCurrent(fetchPhase.getSearchPhaseName()));
        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertEquals(0, testListener.getPhaseCurrent(fetchPhase.getSearchPhaseName()));
        assertEquals(0, testListener.getPhaseTotal(fetchPhase.getSearchPhaseName()));
    }

    public void testOnPhaseFailure() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, 0);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            phaseResults,
            listener,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong())
        );

        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals("message", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testShardNotAvailableWithDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        int numShards = randomIntBetween(2, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            phaseResults,
            listener,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong())
        );
        // skip one to avoid the "all shards failed" failure.
        SearchShardIterator skipIterator = new SearchShardIterator(null, null, Collections.emptyList(), null);
        skipIterator.resetAndSkip();
        action.skipShard(skipIterator);
        // expect at least 2 shards, so onPhaseDone should report failure.
        action.onPhaseDone();
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals("Partial shards failure (" + (numShards - 1) + " shards unavailable)", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
    }

    public void testShardNotAvailableWithIgnoreUnavailable() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false)
            .indicesOptions(new IndicesOptions(EnumSet.of(IndicesOptions.Option.IGNORE_UNAVAILABLE), IndicesOptions.WildcardStates.NONE));
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> {}, exception::set);
        int numShards = randomIntBetween(2, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            phaseResults,
            listener,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong())
        );
        // skip one to avoid the "all shards failed" failure.
        SearchShardIterator skipIterator = new SearchShardIterator(null, null, Collections.emptyList(), null);
        skipIterator.resetAndSkip();
        action.skipShard(skipIterator);

        // Validate no exception is thrown
        action.executeNextPhase(action, createFetchSearchPhase());
        action.sendSearchResponse(InternalSearchResponse.empty(), phaseResults.results);
    }

    private static ArraySearchPhaseResults<SearchPhaseResult> phaseResults(
        Set<ShardSearchContextId> contextIds,
        List<Tuple<String, String>> nodeLookups,
        int numFailures
    ) {
        int numResults = randomIntBetween(1, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numResults + numFailures);

        for (int i = 0; i < numResults; i++) {
            ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            contextIds.add(contextId);
            SearchPhaseResult phaseResult = new PhaseResult(contextId);
            String resultClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String resultNodeId = randomAlphaOfLengthBetween(5, 10);
            ShardId resultShardId = new ShardId("index", "index-uuid", i);
            nodeLookups.add(Tuple.tuple(resultClusterAlias, resultNodeId));
            phaseResult.setSearchShardTarget(new SearchShardTarget(resultNodeId, resultShardId, resultClusterAlias, OriginalIndices.NONE));
            phaseResult.setShardIndex(i);
            phaseResults.consumeResult(phaseResult, () -> {});
        }
        return phaseResults;
    }

    public void testOnShardFailurePhaseDoneFailure() throws InterruptedException {
        final Index index = new Index("test", UUID.randomUUID().toString());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean(true);

        final SearchShardIterator[] shards = IntStream.range(0, 5 + randomInt(10))
            .mapToObj(i -> new SearchShardIterator(null, new ShardId(index, i), List.of("n1", "n2", "n3"), null, null, null))
            .toArray(SearchShardIterator[]::new);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setMaxConcurrentShardRequests(1);

        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(shards.length);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {

                }

                @Override
                public void onFailure(Exception e) {
                    if (fail.compareAndExchange(true, false)) {
                        try {
                            throw new RuntimeException("Simulated exception");
                        } finally {
                            executor.submit(() -> latch.countDown());
                        }
                    }
                }
            },
            false,
            true,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong()),
            shards
        );
        action.run();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
    }

    public void testOnShardSuccessPhaseDoneFailure() throws InterruptedException {
        final Index index = new Index("test", UUID.randomUUID().toString());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean(true);

        final SearchShardIterator[] shards = IntStream.range(0, 5 + randomInt(10))
            .mapToObj(i -> new SearchShardIterator(null, new ShardId(index, i), List.of("n1", "n2", "n3"), null, null, null))
            .toArray(SearchShardIterator[]::new);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setMaxConcurrentShardRequests(1);

        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(shards.length);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    if (fail.compareAndExchange(true, false)) {
                        throw new RuntimeException("Simulated exception");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    executor.submit(() -> latch.countDown());
                }
            },
            false,
            false,
            false,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong()),
            shards
        );
        action.run();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        assertThat(searchResponse.getSuccessfulShards(), equalTo(shards.length));
    }

    private void innerTestExecutePhaseOnShardFailure(boolean catchExceptionWhenExecutePhaseOnShard) throws InterruptedException {
        final Index index = new Index("test", UUID.randomUUID().toString());

        final SearchShardIterator[] shards = IntStream.range(0, 2 + randomInt(3))
            .mapToObj(i -> new SearchShardIterator(null, new ShardId(index, i), List.of("n1", "n2", "n3"), null, null, null))
            .toArray(SearchShardIterator[]::new);

        final AtomicBoolean fail = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setMaxConcurrentShardRequests(5);

        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(shards.length);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {}

                @Override
                public void onFailure(Exception e) {
                    try {
                        // We end up here only when onPhaseDone() is called (causing NPE) and
                        // ending up in the onPhaseFailure() callback
                        if (fail.compareAndExchange(true, false)) {
                            assertThat(e, instanceOf(SearchPhaseExecutionException.class));
                            throw new RuntimeException("Simulated exception");
                        }
                    } finally {
                        executor.submit(() -> latch.countDown());
                    }
                }
            },
            false,
            false,
            catchExceptionWhenExecutePhaseOnShard,
            new AtomicLong(),
            new TaskResourceUsage(randomLong(), randomLong()),
            shards
        );
        action.run();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        assertThat(searchResponse.getSuccessfulShards(), equalTo(shards.length));
    }

    public void testExecutePhaseOnShardFailure() throws InterruptedException {
        innerTestExecutePhaseOnShardFailure(false);
    }

    public void testExecutePhaseOnShardFailureAndThrowException() throws InterruptedException {
        innerTestExecutePhaseOnShardFailure(true);
    }

    public void testOnPhaseListenersWithQueryAndThenFetchType() throws InterruptedException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testListener = new SearchRequestStats(clusterSettings);
        final List<SearchRequestOperationsListener> requestOperationListeners = new ArrayList<>(List.of(testListener, assertingListener));

        long delay = (randomIntBetween(1, 5));
        delay = delay * 10;

        SearchQueryThenFetchAsyncAction action = createSearchQueryThenFetchAsyncAction(requestOperationListeners);
        action.start();

        // Verify queryPhase current metric
        assertEquals(1, testListener.getPhaseCurrent(action.getSearchPhaseName()));
        TimeUnit.MILLISECONDS.sleep(delay);

        FetchSearchPhase fetchPhase = createFetchSearchPhase();
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        searchShardIterator.resetAndSkip();
        action.skipShard(searchShardIterator);
        action.executeNextPhase(action, fetchPhase);

        // Verify queryPhase total, current and latency metrics
        assertEquals(0, testListener.getPhaseCurrent(action.getSearchPhaseName()));
        assertThat(testListener.getPhaseMetric(action.getSearchPhaseName()), greaterThanOrEqualTo(delay));
        assertEquals(1, testListener.getPhaseTotal(action.getSearchPhaseName()));

        // Verify fetchPhase current metric
        assertEquals(1, testListener.getPhaseCurrent(fetchPhase.getSearchPhaseName()));
        TimeUnit.MILLISECONDS.sleep(delay);

        ExpandSearchPhase expandPhase = createExpandSearchPhase();
        action.executeNextPhase(fetchPhase, expandPhase);
        TimeUnit.MILLISECONDS.sleep(delay);

        // Verify fetchPhase total, current and latency metrics
        assertThat(testListener.getPhaseMetric(fetchPhase.getSearchPhaseName()), greaterThanOrEqualTo(delay));
        assertEquals(1, testListener.getPhaseTotal(fetchPhase.getSearchPhaseName()));
        assertEquals(0, testListener.getPhaseCurrent(fetchPhase.getSearchPhaseName()));

        assertEquals(1, testListener.getPhaseCurrent(expandPhase.getSearchPhaseName()));

        action.executeNextPhase(expandPhase, fetchPhase);
        action.onPhaseDone(); /* finish phase since we don't have reponse being sent */

        assertThat(testListener.getPhaseMetric(expandPhase.getSearchPhaseName()), greaterThanOrEqualTo(delay));
        assertEquals(1, testListener.getPhaseTotal(expandPhase.getSearchPhaseName()));
        assertEquals(0, testListener.getPhaseCurrent(expandPhase.getSearchPhaseName()));
    }

    public void testOnPhaseListenersWithDfsType() throws InterruptedException {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        SearchRequestStats testListener = new SearchRequestStats(clusterSettings);
        final List<SearchRequestOperationsListener> requestOperationListeners = new ArrayList<>(List.of(testListener, assertingListener));

        SearchDfsQueryThenFetchAsyncAction searchDfsQueryThenFetchAsyncAction = createSearchDfsQueryThenFetchAsyncAction(
            requestOperationListeners
        );
        long delay = (randomIntBetween(1, 5));

        FetchSearchPhase fetchPhase = createFetchSearchPhase();
        searchDfsQueryThenFetchAsyncAction.start();
        assertEquals(1, testListener.getPhaseCurrent(searchDfsQueryThenFetchAsyncAction.getSearchPhaseName()));
        TimeUnit.MILLISECONDS.sleep(delay);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        searchShardIterator.resetAndSkip();

        searchDfsQueryThenFetchAsyncAction.skipShard(searchShardIterator);
        searchDfsQueryThenFetchAsyncAction.executeNextPhase(searchDfsQueryThenFetchAsyncAction, fetchPhase);
        searchDfsQueryThenFetchAsyncAction.onPhaseFailure(
            fetchPhase,
            "Something went wrong",
            null
        ); /* finalizing the fetch phase since we do adhoc phase lifecycle calls */

        assertThat(testListener.getPhaseMetric(searchDfsQueryThenFetchAsyncAction.getSearchPhaseName()), greaterThanOrEqualTo(delay));
        assertEquals(1, testListener.getPhaseTotal(searchDfsQueryThenFetchAsyncAction.getSearchPhaseName()));
        assertEquals(0, testListener.getPhaseCurrent(searchDfsQueryThenFetchAsyncAction.getSearchPhaseName()));
    }

    private SearchDfsQueryThenFetchAsyncAction createSearchDfsQueryThenFetchAsyncAction(
        List<SearchRequestOperationsListener> searchRequestOperationsListeners
    ) {
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            r -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        Executor executor = OpenSearchExecutors.newDirectExecutorService();
        SearchShardIterator shards = new SearchShardIterator(null, null, Collections.emptyList(), null);
        GroupShardsIterator<SearchShardIterator> shardsIter = new GroupShardsIterator<>(List.of(shards));
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task.getProgressListener(),
            writableRegistry(),
            shardsIter.size(),
            exc -> {}
        );
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        return new SearchDfsQueryThenFetchAsyncAction(
            logger,
            null,
            null,
            null,
            null,
            null,
            controller,
            executor,
            resultConsumer,
            searchRequest,
            listener,
            shardsIter,
            timeProvider,
            null,
            task,
            SearchResponse.Clusters.EMPTY,
            new SearchRequestContext(
                new SearchRequestOperationsListener.CompositeListener(searchRequestOperationsListeners, logger),
                searchRequest,
                () -> null
            ),
            NoopTracer.INSTANCE
        );
    }

    private SearchQueryThenFetchAsyncAction createSearchQueryThenFetchAsyncAction(
        List<SearchRequestOperationsListener> searchRequestOperationsListeners
    ) {
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            r -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        Executor executor = OpenSearchExecutors.newDirectExecutorService();
        SearchShardIterator shards = new SearchShardIterator(null, null, Collections.emptyList(), null);
        GroupShardsIterator<SearchShardIterator> shardsIter = new GroupShardsIterator<>(List.of(shards));
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task.getProgressListener(),
            writableRegistry(),
            shardsIter.size(),
            exc -> {}
        );
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        return new SearchQueryThenFetchAsyncAction(
            logger,
            null,
            null,
            null,
            null,
            null,
            null,
            executor,
            resultConsumer,
            searchRequest,
            listener,
            shardsIter,
            timeProvider,
            null,
            task,
            SearchResponse.Clusters.EMPTY,
            new SearchRequestContext(
                new SearchRequestOperationsListener.CompositeListener(searchRequestOperationsListeners, logger),
                searchRequest,
                () -> null
            ),
            NoopTracer.INSTANCE
        ) {
            @Override
            ShardSearchFailure[] buildShardFailures() {
                return ShardSearchFailure.EMPTY_ARRAY;
            }

            @Override
            public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
                start();
            }
        };
    }

    private FetchSearchPhase createFetchSearchPhase() {
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            r -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        QueryPhaseResultConsumer results = controller.newSearchPhaseResults(
            OpenSearchExecutors.newDirectExecutorService(),
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            SearchProgressListener.NOOP,
            mockSearchPhaseContext.getRequest(),
            1,
            exc -> {}
        );
        return new FetchSearchPhase(
            results,
            controller,
            null,
            mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            }
        );
    }

    private ExpandSearchPhase createExpandSearchPhase() {
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(null, null, null, null, false, null, 1);
        return new ExpandSearchPhase(mockSearchPhaseContext, internalSearchResponse, null);
    }

    private static final class PhaseResult extends SearchPhaseResult {
        PhaseResult(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }
    }
}
