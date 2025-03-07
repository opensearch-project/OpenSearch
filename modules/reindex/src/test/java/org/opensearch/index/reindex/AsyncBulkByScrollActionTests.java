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

package org.opensearch.index.reindex;

import org.apache.lucene.search.TotalHits;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkItemResponse.Failure;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.ReduceSearchPhaseException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.reindex.ScrollableHitSource.Hit;
import org.opensearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.FilterClient;
import org.opensearch.transport.client.ParentTaskAssigningClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedSet;
import static org.opensearch.action.bulk.BackoffPolicy.constantBackoff;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;

public class AsyncBulkByScrollActionTests extends OpenSearchTestCase {
    private MyMockClient client;
    private DummyAbstractBulkByScrollRequest testRequest;
    private SearchRequest firstSearchRequest;
    private PlainActionFuture<BulkByScrollResponse> listener;
    private String scrollId;
    private ThreadPool threadPool;
    private TaskManager taskManager;
    private BulkByScrollTask testTask;
    private WorkerBulkByScrollTaskState worker;
    private Map<String, String> expectedHeaders = new HashMap<>();
    private DiscoveryNode localNode;
    private TaskId taskId;

    @Before
    public void setupForTest() {
        // Fill the context with something random so we can make sure we inherited it appropriately.
        expectedHeaders.clear();
        expectedHeaders.put(randomSimpleString(random()), randomSimpleString(random()));

        threadPool = new TestThreadPool(getTestName());
        setupClient(threadPool);
        firstSearchRequest = new SearchRequest();
        testRequest = new DummyAbstractBulkByScrollRequest(firstSearchRequest);
        listener = new PlainActionFuture<>();
        scrollId = null;
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        testTask = (BulkByScrollTask) taskManager.register("don'tcare", "hereeither", testRequest);
        testTask.setWorker(testRequest.getRequestsPerSecond(), null);
        worker = testTask.getWorkerState();

        localNode = new DiscoveryNode("thenode", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        taskId = new TaskId(localNode.getId(), testTask.getId());
    }

    private void setupClient(ThreadPool threadPool) {
        if (client != null) {
            client.close();
        }
        client = new MyMockClient(new NoOpClient(threadPool));
        client.threadPool().getThreadContext().putHeader(expectedHeaders);
    }

    @After
    public void tearDownAndVerifyCommonStuff() throws Exception {
        client.close();
        terminate(threadPool);
    }

    /**
     * Generates a random scrollId and registers it so that when the test
     * finishes we check that it was cleared. Subsequent calls reregister a new
     * random scroll id so it is checked instead.
     */
    private String scrollId() {
        scrollId = randomSimpleString(random(), 1, 10); // Empty strings get special behavior we don't want
        return scrollId;
    }

    public void testStartRetriesOnRejectionAndSucceeds() throws Exception {
        client.searchesToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        DummyAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(client.searchesToReject + 1, client.searchAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a search attempt pending that we didn't reject", client.lastSearch.get()));
        assertEquals(client.searchesToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        client.searchesToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        DummyAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        action.start();
        assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.searchAttempts.get()));
        assertBusy(() -> assertTrue(listener.isDone()));
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(ExceptionsHelper.stackTrace(e), containsString(OpenSearchRejectedExecutionException.class.getSimpleName()));
        assertNull("There shouldn't be a search attempt pending that we didn't reject", client.lastSearch.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionAndSucceeds() throws Exception {
        // this test primarily tests ClientScrollableHitSource but left it to test integration to status
        client.scrollsToReject = randomIntBetween(0, testRequest.getMaxRetries() - 1);
        // use fail() onResponse handler because mocked search never fires on listener.
        ClientScrollableHitSource hitSource = new ClientScrollableHitSource(
            logger,
            buildTestBackoffPolicy(),
            threadPool,
            testTask.getWorkerState()::countSearchRetry,
            r -> fail(),
            ExceptionsHelper::reThrowIfNotNull,
            new ParentTaskAssigningClient(client, localNode, testTask),
            testRequest.getSearchRequest()
        );
        hitSource.setScroll(scrollId());
        hitSource.startNextScroll(TimeValue.timeValueSeconds(0));
        assertBusy(() -> assertEquals(client.scrollsToReject + 1, client.scrollAttempts.get()));
        if (listener.isDone()) {
            Object result = listener.get();
            fail("Expected listener not to be done but it was and had " + result);
        }
        assertBusy(() -> assertNotNull("There should be a scroll attempt pending that we didn't reject", client.lastScroll.get()));
        assertEquals(client.scrollsToReject, testTask.getStatus().getSearchRetries());
    }

    public void testStartNextScrollRetriesOnRejectionButFailsOnTooManyRejections() throws Exception {
        // this test primarily tests ClientScrollableHitSource but left it to test integration to status
        client.scrollsToReject = testRequest.getMaxRetries() + randomIntBetween(1, 100);
        assertExactlyOnce(onFail -> {
            Consumer<Exception> validingOnFail = e -> {
                assertNotNull(ExceptionsHelper.unwrap(e, OpenSearchRejectedExecutionException.class));
                onFail.run();
            };
            ClientScrollableHitSource hitSource = new ClientScrollableHitSource(
                logger,
                buildTestBackoffPolicy(),
                threadPool,
                testTask.getWorkerState()::countSearchRetry,
                r -> fail(),
                validingOnFail,
                new ParentTaskAssigningClient(client, localNode, testTask),
                testRequest.getSearchRequest()
            );
            hitSource.setScroll(scrollId());
            hitSource.startNextScroll(TimeValue.timeValueSeconds(0));
            assertBusy(() -> assertEquals(testRequest.getMaxRetries() + 1, client.scrollAttempts.get()));
        });
        assertNull("There shouldn't be a scroll attempt pending that we didn't reject", client.lastScroll.get());
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getSearchRetries());
    }

    public void testScrollResponseSetsTotal() {
        // Default is 0, meaning unstarted
        assertEquals(0, testTask.getStatus().getTotal());

        long total = randomIntBetween(0, Integer.MAX_VALUE);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), total, emptyList(), null);
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), 0, 0, response);
        assertEquals(total, testTask.getStatus().getTotal());
    }

    /**
     * Tests that each scroll response is a batch and that the batch is launched properly.
     */
    public void testScrollResponseBatchingBehavior() throws Exception {
        int maxBatches = randomIntBetween(0, 100);
        for (int batches = 1; batches < maxBatches; batches++) {
            Hit hit = new ScrollableHitSource.BasicHit("index", "id", 0);
            ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 1, singletonList(hit), null);
            DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
            simulateScrollResponse(action, System.nanoTime(), 0, response);

            // Use assert busy because the update happens on another thread
            final int expectedBatches = batches;
            assertBusy(() -> assertEquals(expectedBatches, testTask.getStatus().getBatches()));
        }
    }

    public void testBulkResponseSetsLotsOfStatus() throws Exception {
        testRequest.setAbortOnVersionConflict(false);
        int maxBatches = randomIntBetween(0, 100);
        long versionConflicts = 0;
        long created = 0;
        long updated = 0;
        long deleted = 0;
        for (int batches = 0; batches < maxBatches; batches++) {
            BulkItemResponse[] responses = new BulkItemResponse[randomIntBetween(0, 100)];
            for (int i = 0; i < responses.length; i++) {
                ShardId shardId = new ShardId(new Index("name", "uid"), 0);
                if (rarely()) {
                    versionConflicts++;
                    responses[i] = new BulkItemResponse(
                        i,
                        randomFrom(DocWriteRequest.OpType.values()),
                        new Failure(shardId.getIndexName(), "id" + i, new VersionConflictEngineException(shardId, "id", "test"))
                    );
                    continue;
                }
                boolean createdResponse;
                DocWriteRequest.OpType opType;
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        createdResponse = true;
                        opType = DocWriteRequest.OpType.CREATE;
                        created++;
                        break;
                    case 1:
                        createdResponse = false;
                        opType = randomFrom(DocWriteRequest.OpType.INDEX, DocWriteRequest.OpType.UPDATE);
                        updated++;
                        break;
                    case 2:
                        createdResponse = false;
                        opType = DocWriteRequest.OpType.DELETE;
                        deleted++;
                        break;
                    default:
                        throw new RuntimeException("Bad scenario");
                }
                final int seqNo = randomInt(20);
                final int primaryTerm = randomIntBetween(1, 16);
                final IndexResponse response = new IndexResponse(shardId, "id" + i, seqNo, primaryTerm, randomInt(), createdResponse);
                responses[i] = new BulkItemResponse(i, opType, response);
            }
            assertExactlyOnce(onSuccess -> new DummyAsyncBulkByScrollAction().onBulkResponse(new BulkResponse(responses, 0), onSuccess));
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
            assertEquals(updated, testTask.getStatus().getUpdated());
            assertEquals(created, testTask.getStatus().getCreated());
            assertEquals(deleted, testTask.getStatus().getDeleted());
            assertEquals(versionConflicts, testTask.getStatus().getVersionConflicts());
        }
    }

    /**
     * Mimicks a ThreadPool rejecting execution of the task.
     */
    public void testThreadPoolRejectionsAbortRequest() throws Exception {
        worker.rethrottle(1);
        setupClient(new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String name) {
                // While we're here we can check that the sleep made it through
                assertThat(delay.nanos(), greaterThan(0L));
                assertThat(delay.seconds(), lessThanOrEqualTo(10L));
                final OpenSearchRejectedExecutionException exception = new OpenSearchRejectedExecutionException("test");
                if (command instanceof AbstractRunnable) {
                    ((AbstractRunnable) command).onRejection(exception);
                    return null;
                } else {
                    throw exception;
                }
            }
        });
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 0, emptyList(), null);
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), System.nanoTime(), 10, response);
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getCause(), instanceOf(OpenSearchRejectedExecutionException.class));
        assertThat(e.getCause(), hasToString(containsString("test")));
        assertThat(client.scrollsCleared, contains(scrollId));

        // When the task is rejected we don't increment the throttled timer
        assertEquals(timeValueMillis(0), testTask.getStatus().getThrottled());
    }

    /**
     * Mimicks shard search failures usually caused by the data node serving the
     * scroll request going down.
     */
    public void testShardFailuresAbortRequest() throws Exception {
        SearchFailure shardFailure = new SearchFailure(new RuntimeException("test"));
        ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(
            false,
            singletonList(shardFailure),
            0,
            emptyList(),
            null
        );
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), System.nanoTime(), 0, scrollResponse);
        BulkByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), contains(shardFailure));
        assertFalse(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    /**
     * Mimicks search timeouts.
     */
    public void testSearchTimeoutsAbortRequest() throws Exception {
        ScrollableHitSource.Response scrollResponse = new ScrollableHitSource.Response(true, emptyList(), 0, emptyList(), null);
        simulateScrollResponse(new DummyAsyncBulkByScrollAction(), System.nanoTime(), 0, scrollResponse);
        BulkByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), empty());
        assertThat(response.getSearchFailures(), empty());
        assertTrue(response.isTimedOut());
        assertNull(response.getReasonCancelled());
        assertThat(client.scrollsCleared, contains(scrollId));
    }

    /**
     * Mimicks bulk indexing failures.
     */
    public void testBulkFailuresAbortRequest() throws Exception {
        Failure failure = new Failure("index", "id", new RuntimeException("test"));
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] { new BulkItemResponse(0, DocWriteRequest.OpType.CREATE, failure) },
            randomLong()
        );
        action.onBulkResponse(bulkResponse, Assert::fail);
        BulkByScrollResponse response = listener.get();
        assertThat(response.getBulkFailures(), contains(failure));
        assertThat(response.getSearchFailures(), empty());
        assertNull(response.getReasonCancelled());
    }

    /**
     * Mimicks script failures or general wrongness by implementers.
     */
    public void testBuildRequestThrowsException() throws Exception {
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction() {
            @Override
            protected AbstractAsyncBulkByScrollAction.RequestWrapper<?> buildRequest(Hit doc) {
                throw new RuntimeException("surprise");
            }
        };
        ScrollableHitSource.BasicHit hit = new ScrollableHitSource.BasicHit("index", "id", 0);
        hit.setSource(new BytesArray("{}"), MediaTypeRegistry.JSON);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), 1, singletonList(hit), null);
        simulateScrollResponse(action, System.nanoTime(), 0, response);
        ExecutionException e = expectThrows(ExecutionException.class, () -> listener.get());
        assertThat(e.getCause(), instanceOf(RuntimeException.class));
        assertThat(e.getCause().getMessage(), equalTo("surprise"));
    }

    /**
     * Mimicks bulk rejections. These should be retried and eventually succeed.
     */
    public void testBulkRejectionsRetryWithEnoughRetries() throws Exception {
        int bulksToTry = randomIntBetween(1, 10);
        long retryAttempts = 0;
        for (int i = 0; i < bulksToTry; i++) {
            bulkRetryTestCase(false);
            retryAttempts += testRequest.getMaxRetries();
            assertEquals(retryAttempts, testTask.getStatus().getBulkRetries());
        }
    }

    /**
     * Mimicks bulk rejections. These should be retried but we fail anyway because we run out of retries.
     */
    public void testBulkRejectionsRetryAndFailAnyway() throws Exception {
        bulkRetryTestCase(true);
        assertEquals(testRequest.getMaxRetries(), testTask.getStatus().getBulkRetries());
    }

    public void testScrollDelay() throws Exception {
        /*
         * Replace the thread pool with one that will save the delay sent for the command. We'll use that to check that we used a proper
         * delay for throttling.
         */
        AtomicReference<TimeValue> capturedDelay = new AtomicReference<>();
        AtomicReference<Runnable> capturedCommand = new AtomicReference<>();
        setupClient(new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String name) {
                capturedDelay.set(delay);
                capturedCommand.set(command);
                return new ScheduledCancellable() {
                    private boolean cancelled = false;

                    @Override
                    public long getDelay(TimeUnit unit) {
                        return unit.convert(delay.millis(), TimeUnit.MILLISECONDS);
                    }

                    @Override
                    public int compareTo(Delayed o) {
                        return 0;
                    }

                    @Override
                    public boolean cancel() {
                        cancelled = true;
                        return true;
                    }

                    @Override
                    public boolean isCancelled() {
                        return cancelled;
                    }
                };
            }
        });

        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction() {
            @Override
            protected RequestWrapper<?> buildRequest(Hit doc) {
                return wrap(new IndexRequest().index("test"));
            }
        };
        action.setScroll(scrollId());

        // Set the base for the scroll to wait - this is added to the figure we calculate below
        firstSearchRequest.scroll(timeValueSeconds(10));

        // Set throttle to 1 request per second to make the math simpler
        worker.rethrottle(1f);
        action.start();

        // create a simulated response.
        SearchHit hit = new SearchHit(0, "id", emptyMap(), emptyMap()).sourceRef(new BytesArray("{}"));
        SearchHits hits = new SearchHits(
            IntStream.range(0, 100).mapToObj(i -> hit).toArray(SearchHit[]::new),
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            0
        );
        InternalSearchResponse internalResponse = new InternalSearchResponse(hits, null, null, null, false, false, 1);
        SearchResponse searchResponse = new SearchResponse(
            internalResponse,
            scrollId(),
            5,
            4,
            0,
            randomLong(),
            null,
            SearchResponse.Clusters.EMPTY
        );

        client.lastSearch.get().listener.onResponse(searchResponse);

        assertEquals(0, capturedDelay.get().seconds());
        capturedCommand.get().run();

        // So the next request is going to have to wait an extra 100 seconds or so (base was 10 seconds, so 110ish)
        assertThat(client.lastScroll.get().request.scroll().keepAlive().seconds(), either(equalTo(110L)).or(equalTo(109L)));

        // Now we can simulate a response and check the delay that we used for the task
        if (randomBoolean()) {
            client.lastScroll.get().listener.onResponse(searchResponse);
            assertEquals(99, capturedDelay.get().seconds());
        } else {
            // Let's rethrottle between the starting the scroll and getting the response
            worker.rethrottle(10f);
            client.lastScroll.get().listener.onResponse(searchResponse);
            // The delay uses the new throttle
            assertEquals(9, capturedDelay.get().seconds());
        }

        // Running the command ought to increment the delay counter on the task.
        capturedCommand.get().run();
        assertEquals(capturedDelay.get(), testTask.getStatus().getThrottled());
    }

    /**
     * Execute a bulk retry test case. The total number of failures is random and the number of retries attempted is set to
     * testRequest.getMaxRetries and controlled by the failWithRejection parameter.
     */
    private void bulkRetryTestCase(boolean failWithRejection) throws Exception {
        int totalFailures = randomIntBetween(1, testRequest.getMaxRetries());
        int size = randomIntBetween(1, 100);
        testRequest.setMaxRetries(totalFailures - (failWithRejection ? 1 : 0));

        client.bulksToReject = client.bulksAttempts.get() + totalFailures;
        DummyAsyncBulkByScrollAction action = new DummyActionWithoutBackoff();
        BulkRequest request = new BulkRequest();
        for (int i = 0; i < size + 1; i++) {
            request.add(new IndexRequest("index").id("id" + i));
        }
        if (failWithRejection) {
            action.sendBulkRequest(request, Assert::fail);
            BulkByScrollResponse response = listener.get();
            assertThat(response.getBulkFailures(), hasSize(1));
            assertEquals(response.getBulkFailures().get(0).getStatus(), RestStatus.TOO_MANY_REQUESTS);
            assertThat(response.getSearchFailures(), empty());
            assertNull(response.getReasonCancelled());
        } else {
            assertExactlyOnce(onSuccess -> action.sendBulkRequest(request, onSuccess));
        }
    }

    /**
     * The default retry time matches what we say it is in the javadoc for the request.
     */
    public void testDefaultRetryTimes() {
        Iterator<TimeValue> policy = new DummyAsyncBulkByScrollAction().buildBackoffPolicy().iterator();
        long millis = 0;
        while (policy.hasNext()) {
            millis += policy.next().millis();
        }
        /*
         * This is the total number of milliseconds that a reindex made with the default settings will backoff before attempting one final
         * time. If that request is rejected then the whole process fails with a rejected exception.
         */
        int defaultBackoffBeforeFailing = 59460;
        assertEquals(defaultBackoffBeforeFailing, millis);
    }

    public void testRefreshIsFalseByDefault() throws Exception {
        refreshTestCase(null, true, false);
    }

    public void testRefreshFalseDoesntExecuteRefresh() throws Exception {
        refreshTestCase(false, true, false);
    }

    public void testRefreshTrueExecutesRefresh() throws Exception {
        refreshTestCase(true, true, true);
    }

    public void testRefreshTrueSkipsRefreshIfNoDestinationIndexes() throws Exception {
        refreshTestCase(true, false, false);
    }

    private void refreshTestCase(Boolean refresh, boolean addDestinationIndexes, boolean shouldRefresh) {
        if (refresh != null) {
            testRequest.setRefresh(refresh);
        }
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        if (addDestinationIndexes) {
            action.addDestinationIndices(singleton("foo"));
        }
        action.refreshAndFinish(emptyList(), emptyList(), false);
        if (shouldRefresh) {
            assertArrayEquals(new String[] { "foo" }, client.lastRefreshRequest.get().indices());
        } else {
            assertNull("No refresh was attempted", client.lastRefreshRequest.get());
        }
    }

    public void testCancelBeforeInitialSearch() throws Exception {
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.start());
    }

    public void testCancelBeforeScrollResponse() throws Exception {
        cancelTaskCase(
            (DummyAsyncBulkByScrollAction action) -> simulateScrollResponse(
                action,
                System.nanoTime(),
                1,
                new ScrollableHitSource.Response(false, emptyList(), between(1, 100000), emptyList(), null)
            )
        );
    }

    public void testCancelBeforeSendBulkRequest() throws Exception {
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.sendBulkRequest(new BulkRequest(), Assert::fail));
    }

    public void testCancelBeforeOnBulkResponse() throws Exception {
        cancelTaskCase(
            (DummyAsyncBulkByScrollAction action) -> action.onBulkResponse(new BulkResponse(new BulkItemResponse[0], 0), Assert::fail)
        );
    }

    public void testCancelBeforeStartNextScroll() throws Exception {
        long now = System.nanoTime();
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.notifyDone(now, null, 0));
    }

    public void testCancelBeforeRefreshAndFinish() throws Exception {
        // Refresh or not doesn't matter - we don't try to refresh.
        testRequest.setRefresh(usually());
        cancelTaskCase((DummyAsyncBulkByScrollAction action) -> action.refreshAndFinish(emptyList(), emptyList(), false));
        assertNull("No refresh was attempted", client.lastRefreshRequest.get());
    }

    /**
     * Tests that we can cancel the request during its throttling delay. This can't use {@link #cancelTaskCase(Consumer)} because it needs
     * to send the request un-canceled and cancel it at a specific time.
     */
    public void testCancelWhileDelayedAfterScrollResponse() throws Exception {
        String reason = randomSimpleString(random());

        /*
         * Replace the thread pool with one that will cancel the task as soon as anything is scheduled, which reindex tries to do when there
         * is a delay.
         */
        setupClient(new TestThreadPool(getTestName()) {
            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, String name) {
                /*
                 * This is called twice:
                 * 1. To schedule the throttling. When that happens we immediately cancel the task.
                 * 2. After the task is canceled.
                 * Both times we use delegate to the standard behavior so the task is scheduled as expected so it can be cancelled and all
                 * that good stuff.
                 */
                if (delay.nanos() > 0) {
                    generic().execute(() -> taskManager.cancel(testTask, reason, () -> {}));
                }
                return super.schedule(command, delay, name);
            }
        });

        // Send the scroll response which will trigger the custom thread pool above, canceling the request before running the response
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        long total = randomIntBetween(0, Integer.MAX_VALUE);
        ScrollableHitSource.Response response = new ScrollableHitSource.Response(false, emptyList(), total, emptyList(), null);
        // Use a long delay here so the test will time out if the cancellation doesn't reschedule the throttled task
        worker.rethrottle(1);
        simulateScrollResponse(action, System.nanoTime(), 1000, response);

        // Now that we've got our cancel we'll just verify that it all came through all right
        assertEquals(reason, listener.get(10, TimeUnit.SECONDS).getReasonCancelled());
        if (previousScrollSet) {
            // Canceled tasks always start to clear the scroll before they die.
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    private void cancelTaskCase(Consumer<DummyAsyncBulkByScrollAction> testMe) throws Exception {
        DummyAsyncBulkByScrollAction action = new DummyAsyncBulkByScrollAction();
        boolean previousScrollSet = usually();
        if (previousScrollSet) {
            action.setScroll(scrollId());
        }
        String reason = randomSimpleString(random());
        taskManager.cancel(testTask, reason, () -> {});
        testMe.accept(action);
        assertEquals(reason, listener.get().getReasonCancelled());
        if (previousScrollSet) {
            // Canceled tasks always start to clear the scroll before they die.
            assertThat(client.scrollsCleared, contains(scrollId));
        }
    }

    /**
     * Simulate a scroll response by setting the scroll id and firing the onScrollResponse method.
     */
    private void simulateScrollResponse(
        DummyAsyncBulkByScrollAction action,
        long lastBatchTime,
        int lastBatchSize,
        ScrollableHitSource.Response response
    ) {
        action.setScroll(scrollId());
        action.onScrollResponse(lastBatchTime, lastBatchSize, new ScrollableHitSource.AsyncResponse() {
            @Override
            public ScrollableHitSource.Response response() {
                return response;
            }

            @Override
            public void done(TimeValue extraKeepAlive) {
                fail();
            }
        });
    }

    private class DummyAsyncBulkByScrollAction extends AbstractAsyncBulkByScrollAction<
        DummyAbstractBulkByScrollRequest,
        DummyTransportAsyncBulkByScrollAction> {
        DummyAsyncBulkByScrollAction() {
            super(
                testTask,
                randomBoolean(),
                randomBoolean(),
                AsyncBulkByScrollActionTests.this.logger,
                new ParentTaskAssigningClient(client, localNode, testTask),
                client.threadPool(),
                testRequest,
                listener,
                null,
                null
            );
        }

        @Override
        protected AbstractAsyncBulkByScrollAction.RequestWrapper<?> buildRequest(Hit doc) {
            throw new UnsupportedOperationException("Use another override to test this.");
        }
    }

    /**
     * An extension to {@linkplain DummyAsyncBulkByScrollAction} that uses a 0 delaying backoff policy.
     */
    private class DummyActionWithoutBackoff extends DummyAsyncBulkByScrollAction {
        @Override
        BackoffPolicy buildBackoffPolicy() {
            return buildTestBackoffPolicy();
        }
    }

    private BackoffPolicy buildTestBackoffPolicy() {
        // Force a backoff time of 0 to prevent sleeping
        return constantBackoff(timeValueMillis(0), testRequest.getMaxRetries());
    }

    private static class DummyTransportAsyncBulkByScrollAction extends TransportAction<
        DummyAbstractBulkByScrollRequest,
        BulkByScrollResponse> {

        protected DummyTransportAsyncBulkByScrollAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
            super(actionName, actionFilters, taskManager);
        }

        @Override
        protected void doExecute(Task task, DummyAbstractBulkByScrollRequest request, ActionListener<BulkByScrollResponse> listener) {
            // no-op
        }
    }

    private static class DummyAbstractBulkByScrollRequest extends AbstractBulkByScrollRequest<DummyAbstractBulkByScrollRequest> {
        DummyAbstractBulkByScrollRequest(SearchRequest searchRequest) {
            super(searchRequest, true);
        }

        @Override
        public DummyAbstractBulkByScrollRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected DummyAbstractBulkByScrollRequest self() {
            return this;
        }
    }

    private class MyMockClient extends FilterClient {
        private final List<String> scrollsCleared = new ArrayList<>();
        private final AtomicInteger bulksAttempts = new AtomicInteger();
        private final AtomicInteger searchAttempts = new AtomicInteger();
        private final AtomicInteger scrollAttempts = new AtomicInteger();
        private final AtomicReference<RefreshRequest> lastRefreshRequest = new AtomicReference<>();
        /**
         * Last search attempt that wasn't rejected outright.
         */
        private final AtomicReference<RequestAndListener<SearchRequest, SearchResponse>> lastSearch = new AtomicReference<>();
        /**
         * Last scroll attempt that wasn't rejected outright.
         */
        private final AtomicReference<RequestAndListener<SearchScrollRequest, SearchResponse>> lastScroll = new AtomicReference<>();
        /**
         * Set of all scrolls we've already used. Used to check that we don't reuse the same request twice.
         */
        private final Set<SearchScrollRequest> usedScolls = synchronizedSet(newSetFromMap(new IdentityHashMap<>()));

        private int bulksToReject = 0;
        private int searchesToReject = 0;
        private int scrollsToReject = 0;

        MyMockClient(Client in) {
            super(in);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (false == expectedHeaders.equals(threadPool().getThreadContext().getHeaders())) {
                listener.onFailure(
                    new RuntimeException("Expected " + expectedHeaders + " but got " + threadPool().getThreadContext().getHeaders())
                );
                return;
            }

            if (request instanceof ClearScrollRequest) {
                assertEquals(TaskId.EMPTY_TASK_ID, request.getParentTask());
            } else {
                assertEquals(taskId, request.getParentTask());
            }
            if (request instanceof RefreshRequest) {
                lastRefreshRequest.set((RefreshRequest) request);
                listener.onResponse(null);
                return;
            }
            if (request instanceof SearchRequest) {
                if (searchAttempts.incrementAndGet() <= searchesToReject) {
                    listener.onFailure(wrappedRejectedException());
                    return;
                }
                lastSearch.set(new RequestAndListener<>((SearchRequest) request, (ActionListener<SearchResponse>) listener));
                return;
            }
            if (request instanceof SearchScrollRequest) {
                SearchScrollRequest scroll = (SearchScrollRequest) request;
                boolean newRequest = usedScolls.add(scroll);
                assertTrue("We can't reuse scroll requests", newRequest);
                if (scrollAttempts.incrementAndGet() <= scrollsToReject) {
                    listener.onFailure(wrappedRejectedException());
                    return;
                }
                lastScroll.set(new RequestAndListener<>(scroll, (ActionListener<SearchResponse>) listener));
                return;
            }
            if (request instanceof ClearScrollRequest) {
                ClearScrollRequest clearScroll = (ClearScrollRequest) request;
                scrollsCleared.addAll(clearScroll.getScrollIds());
                listener.onResponse((Response) new ClearScrollResponse(true, clearScroll.getScrollIds().size()));
                return;
            }
            if (request instanceof BulkRequest) {
                BulkRequest bulk = (BulkRequest) request;
                int toReject;
                if (bulksAttempts.incrementAndGet() > bulksToReject) {
                    toReject = -1;
                } else {
                    toReject = randomIntBetween(0, bulk.requests().size() - 1);
                }
                BulkItemResponse[] responses = new BulkItemResponse[bulk.requests().size()];
                for (int i = 0; i < bulk.requests().size(); i++) {
                    DocWriteRequest<?> item = bulk.requests().get(i);
                    DocWriteResponse response;
                    ShardId shardId = new ShardId(new Index(item.index(), "uuid"), 0);
                    if (item instanceof IndexRequest) {
                        IndexRequest index = (IndexRequest) item;
                        response = new IndexResponse(
                            shardId,
                            index.id() == null ? "dummy_id" : index.id(),
                            randomInt(20),
                            randomIntBetween(1, 16),
                            randomIntBetween(0, Integer.MAX_VALUE),
                            true
                        );
                    } else if (item instanceof UpdateRequest) {
                        UpdateRequest update = (UpdateRequest) item;
                        response = new UpdateResponse(
                            shardId,
                            update.id(),
                            randomNonNegativeLong(),
                            randomIntBetween(1, Integer.MAX_VALUE),
                            randomIntBetween(0, Integer.MAX_VALUE),
                            Result.CREATED
                        );
                    } else if (item instanceof DeleteRequest) {
                        DeleteRequest delete = (DeleteRequest) item;
                        response = new DeleteResponse(
                            shardId,
                            delete.id(),
                            randomInt(20),
                            randomIntBetween(1, 16),
                            randomIntBetween(0, Integer.MAX_VALUE),
                            true
                        );
                    } else {
                        throw new RuntimeException("Unknown request:  " + item);
                    }
                    if (i == toReject) {
                        responses[i] = new BulkItemResponse(
                            i,
                            item.opType(),
                            new Failure(response.getIndex(), response.getId(), new OpenSearchRejectedExecutionException())
                        );
                    } else {
                        responses[i] = new BulkItemResponse(i, item.opType(), response);
                    }
                }
                listener.onResponse((Response) new BulkResponse(responses, 1));
                return;
            }
            super.doExecute(action, request, listener);
        }

        private Exception wrappedRejectedException() {
            Exception e = new OpenSearchRejectedExecutionException();
            int wraps = randomIntBetween(0, 4);
            for (int i = 0; i < wraps; i++) {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        e = new SearchPhaseExecutionException("test", "test failure", e, new ShardSearchFailure[0]);
                        continue;
                    case 1:
                        e = new ReduceSearchPhaseException("test", "test failure", e, new ShardSearchFailure[0]);
                        continue;
                    case 2:
                        e = new OpenSearchException(e);
                        continue;
                }
            }
            return e;
        }
    }

    private static class RequestAndListener<Request extends ActionRequest, Response> {
        private final Request request;
        private final ActionListener<Response> listener;

        RequestAndListener(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }
    }

    /**
     * Assert that calling the consumer invokes the runnable exactly once.
     */
    private void assertExactlyOnce(CheckedConsumer<Runnable, Exception> consumer) throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        consumer.accept(() -> assertTrue(called.compareAndSet(false, true)));
        assertBusy(() -> assertTrue(called.get()));
    }
}
