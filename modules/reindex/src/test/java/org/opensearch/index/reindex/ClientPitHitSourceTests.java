/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ShardDocSortBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.ParentTaskAssigningClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.lucene.search.TotalHits;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for {@link ClientPitHitSource}.
 */
public class ClientPitHitSourceTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    /**
     * Test that PIT is created on start and first search is executed.
     */
    public void testStartCreatesPitAndSearches() throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(10);
        AtomicReference<Exception> failure = new AtomicReference<>();

        MockPitClient client = new MockPitClient(threadPool);
        client.setPitId("test-pit-id-1");
        client.setSearchResponse(createEmptySearchResponse("test-pit-id-1"));

        ClientPitHitSource hitSource = createHitSource(client, responses::add, failure::set);
        hitSource.start();

        ScrollableHitSource.AsyncResponse response = responses.poll(10, TimeUnit.SECONDS);
        assertNotNull("Should have received a response", response);
        assertThat(response.response().getHits().size(), equalTo(0));
        assertNull(failure.get());

        // Verify PIT was created
        assertThat(client.pitCreated.get(), equalTo(true));
        assertThat(client.searchCount.get(), equalTo(1));
    }

    /**
     * Test that search_after is used for subsequent pages.
     */
    public void testSearchAfterUsedForPagination() throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(10);
        AtomicReference<Exception> failure = new AtomicReference<>();

        MockPitClient client = new MockPitClient(threadPool);
        client.setPitId("test-pit-id-1");

        // First search returns hits with sort values
        SearchHit hit1 = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit1.sortValues(new Object[] { 1L }, new DocValueFormat[] { DocValueFormat.RAW });
        hit1.sourceRef(new BytesArray("{\"field\":\"value1\"}"));
        SearchHit hit2 = new SearchHit(2, "doc2", Collections.emptyMap(), Collections.emptyMap());
        hit2.sortValues(new Object[] { 2L }, new DocValueFormat[] { DocValueFormat.RAW });
        hit2.sourceRef(new BytesArray("{\"field\":\"value2\"}"));

        client.setSearchResponse(createSearchResponse("test-pit-id-1", hit1, hit2));

        ClientPitHitSource hitSource = createHitSource(client, responses::add, failure::set);
        hitSource.start();

        ScrollableHitSource.AsyncResponse firstResponse = responses.poll(10, TimeUnit.SECONDS);
        assertNotNull(firstResponse);
        assertThat(firstResponse.response().getHits().size(), equalTo(2));

        // Now request next page — should use search_after
        client.setSearchResponse(createEmptySearchResponse("test-pit-id-1"));
        firstResponse.done(TimeValue.ZERO);

        ScrollableHitSource.AsyncResponse secondResponse = responses.poll(10, TimeUnit.SECONDS);
        assertNotNull(secondResponse);

        // Verify search_after was set on the second request
        assertThat(client.searchCount.get(), equalTo(2));
        assertThat(client.lastSearchAfterValues, notNullValue());
        assertThat(client.lastSearchAfterValues[0], equalTo(2L));
    }

    /**
     * Test that PIT is deleted on close.
     */
    public void testCloseDeletesPit() throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(10);
        AtomicReference<Exception> failure = new AtomicReference<>();

        MockPitClient client = new MockPitClient(threadPool);
        client.setPitId("test-pit-id-1");
        client.setSearchResponse(createEmptySearchResponse("test-pit-id-1"));

        ClientPitHitSource hitSource = createHitSource(client, responses::add, failure::set);
        hitSource.start();

        // Wait for first response
        responses.poll(10, TimeUnit.SECONDS);

        // Close should delete PIT
        AtomicBoolean closed = new AtomicBoolean(false);
        hitSource.setScroll("test-pit-id-1"); // Set the scroll/pit ID so close knows to clean up
        hitSource.close(() -> closed.set(true));

        assertTrue("Close callback should have been called", closed.get());
        assertTrue("PIT should have been deleted", client.pitDeleted.get());
    }

    /**
     * Test that _shard_doc sort is added when no sort is specified.
     */
    public void testShardDocSortAddedAutomatically() throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(10);
        AtomicReference<Exception> failure = new AtomicReference<>();

        MockPitClient client = new MockPitClient(threadPool);
        client.setPitId("test-pit-id-1");
        client.setSearchResponse(createEmptySearchResponse("test-pit-id-1"));

        ClientPitHitSource hitSource = createHitSource(client, responses::add, failure::set);
        hitSource.start();

        responses.poll(10, TimeUnit.SECONDS);

        // Verify _shard_doc sort was added
        assertNotNull(client.lastSearchRequest);
        assertNotNull(client.lastSearchRequest.source().sorts());
        boolean hasShardDoc = client.lastSearchRequest.source().sorts().stream()
            .anyMatch(s -> s instanceof ShardDocSortBuilder);
        assertTrue("_shard_doc sort should be present", hasShardDoc);
    }

    /**
     * Test that PIT ID is updated when response contains a new one.
     */
    public void testPitIdUpdatedFromResponse() throws InterruptedException {
        BlockingQueue<ScrollableHitSource.AsyncResponse> responses = new ArrayBlockingQueue<>(10);
        AtomicReference<Exception> failure = new AtomicReference<>();

        MockPitClient client = new MockPitClient(threadPool);
        client.setPitId("initial-pit-id");

        // Response returns a different PIT ID (server may rotate them)
        SearchHit hit = new SearchHit(1, "doc1", Collections.emptyMap(), Collections.emptyMap());
        hit.sortValues(new Object[] { 1L }, new DocValueFormat[] { DocValueFormat.RAW });
        hit.sourceRef(new BytesArray("{\"field\":\"value\"}"));
        client.setSearchResponse(createSearchResponse("updated-pit-id", hit));

        ClientPitHitSource hitSource = createHitSource(client, responses::add, failure::set);
        hitSource.start();

        ScrollableHitSource.AsyncResponse response = responses.poll(10, TimeUnit.SECONDS);
        assertNotNull(response);

        // The PIT ID in the response should be the updated one
        assertThat(response.response().getScrollId(), equalTo("updated-pit-id"));
        assertThat(hitSource.getPitId(), equalTo("updated-pit-id"));
    }

    // ---- Helper methods ----

    private ClientPitHitSource createHitSource(
        MockPitClient mockClient,
        Consumer<ScrollableHitSource.AsyncResponse> onResponse,
        Consumer<Exception> onFailure
    ) {
        Logger logger = LogManager.getLogger(ClientPitHitSourceTests.class);
        TaskId parentTask = new TaskId("thenode", randomInt());
        ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(mockClient, parentTask);

        SearchRequest searchRequest = new SearchRequest("test-index");
        searchRequest.source(new SearchSourceBuilder().size(10));
        searchRequest.scroll(TimeValue.timeValueMinutes(5));

        return new ClientPitHitSource(
            logger,
            BackoffPolicy.noBackoff(),
            threadPool,
            () -> {},
            onResponse,
            onFailure,
            assigningClient,
            searchRequest
        );
    }

    private SearchResponse createEmptySearchResponse(String pitId) {
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY, pitId);
    }

    private SearchResponse createSearchResponse(String pitId, SearchHit... searchHits) {
        SearchHits hits = new SearchHits(searchHits, new TotalHits(searchHits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 100, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY, pitId);
    }

    /**
     * Mock client that intercepts PIT create/delete and search calls.
     */
    private static class MockPitClient extends NoOpClient {
        private String pitId;
        private SearchResponse searchResponse;
        final AtomicBoolean pitCreated = new AtomicBoolean(false);
        final AtomicBoolean pitDeleted = new AtomicBoolean(false);
        final AtomicInteger searchCount = new AtomicInteger(0);
        volatile Object[] lastSearchAfterValues;
        volatile SearchRequest lastSearchRequest;

        MockPitClient(ThreadPool threadPool) {
            super(threadPool);
        }

        void setPitId(String pitId) {
            this.pitId = pitId;
        }

        void setSearchResponse(SearchResponse response) {
            this.searchResponse = response;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (action == CreatePitAction.INSTANCE) {
                pitCreated.set(true);
                CreatePitResponse response = new CreatePitResponse(
                    pitId,
                    System.currentTimeMillis(),
                    1,
                    1,
                    0,
                    0,
                    ShardSearchFailure.EMPTY_ARRAY
                );
                listener.onResponse((Response) response);
            } else if (action == DeletePitAction.INSTANCE) {
                pitDeleted.set(true);
                List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                deletePitInfos.add(new DeletePitInfo(true, pitId));
                DeletePitResponse response = new DeletePitResponse(deletePitInfos);
                listener.onResponse((Response) response);
            } else if (action == SearchAction.INSTANCE) {
                searchCount.incrementAndGet();
                SearchRequest searchReq = (SearchRequest) request;
                lastSearchRequest = searchReq;
                if (searchReq.source() != null) {
                    lastSearchAfterValues = searchReq.source().searchAfter();
                }
                listener.onResponse((Response) searchResponse);
            } else {
                super.doExecute(action, request, listener);
            }
        }
    }
}
