/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitController;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.PitTestsUtil;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.opensearch.action.search.PitTestsUtil.assertSegments;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

/**
 * Single node integration tests for various PIT use cases such as create pit, search etc
 */
public class CreatePitSingleNodeTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        // very frequent checks
        return Settings.builder()
            .put(super.nodeSettings())
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .put(CreatePitController.PIT_INIT_KEEP_ALIVE.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    public void testCreatePITSuccess() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime(), TimeValue.timeValueDays(1));
        assertSegments(false, client(), pitResponse.getId());
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertHitCount(searchResponse, 1);

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        validatePitStats("index", 1, 0, 0);
        validatePitStats("index", 1, 0, 1);
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertSegments(true, client());
        validatePitStats("index", 0, 1, 0);
        validatePitStats("index", 0, 1, 1);
    }

    public void testCreatePITWithMultipleIndicesSuccess() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        createIndex("index1", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index1").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        SearchService service = getInstanceFromNode(SearchService.class);

        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse response = execute.get();
        PitTestsUtil.assertUsingGetAllPits(client(), response.getId(), response.getCreationTime(), TimeValue.timeValueDays(1));
        assertSegments(false, client(), response.getId());
        assertEquals(4, response.getSuccessfulShards());
        assertEquals(4, service.getActiveContexts());

        validatePitStats("index", 1, 0, 0);
        validatePitStats("index1", 1, 0, 0);
        service.doClose();
        assertSegments(true, client());
        validatePitStats("index", 0, 1, 0);
        validatePitStats("index1", 0, 1, 0);
    }

    public void testCreatePITWithShardReplicasSuccess() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime(), TimeValue.timeValueDays(1));
        assertSegments(false, client(), pitResponse.getId());
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertHitCount(searchResponse, 1);

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        validatePitStats("index", 1, 0, 0);
        validatePitStats("index", 1, 0, 1);
        service.doClose();
        assertSegments(true, client());
        validatePitStats("index", 0, 1, 0);
        validatePitStats("index", 0, 1, 1);
    }

    public void testCreatePITWithNonExistentIndex() {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        SearchService service = getInstanceFromNode(SearchService.class);

        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);

        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(ex.getMessage().contains("no such index [index1]"));
        assertEquals(0, service.getActiveContexts());
        assertSegments(true, client());
        service.doClose();
    }

    public void testCreatePITOnCloseIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareClose("index").get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);

        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(ex.getMessage().contains("IndexClosedException"));

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(0, service.getActiveContexts());
        PitTestsUtil.assertGetAllPitsEmpty(client());
        assertSegments(true, client());
        service.doClose();
    }

    public void testPitSearchOnDeletedIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        client().admin().indices().prepareDelete("index").get();

        IndexNotFoundException ex = expectThrows(IndexNotFoundException.class, () -> {
            client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.getMessage().contains("no such index [index]"));
        SearchService service = getInstanceFromNode(SearchService.class);
        PitTestsUtil.assertGetAllPitsEmpty(client());
        assertEquals(0, service.getActiveContexts());
        assertSegments(true, client());
        service.doClose();
    }

    public void testInvalidPitId() {
        createIndex("idx");
        String id = "c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(id).setKeepAlive(TimeValue.timeValueDays(1)))
                .get()
        );
        assertEquals("invalid id: [" + id + "]", e.getMessage());
    }

    public void testPitSearchOnCloseIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime(), TimeValue.timeValueDays(1));
        assertSegments(false, client(), pitResponse.getId());
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        validatePitStats("index", 1, 0, 0);
        validatePitStats("index", 1, 0, 1);

        client().admin().indices().prepareClose("index").get();
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            SearchResponse searchResponse = client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        assertEquals(0, service.getActiveContexts());
        PitTestsUtil.assertGetAllPitsEmpty(client());
        assertSegments(true, client());
        // PIT reader contexts are lost after close, verifying it with open index api
        client().admin().indices().prepareOpen("index").get();
        ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        assertEquals(0, service.getActiveContexts());
        service.doClose();
    }

    public void testMaxOpenPitContexts() throws Exception {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        SearchService service = getInstanceFromNode(SearchService.class);

        for (int i = 0; i < SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY); i++) {
            client().execute(CreatePitAction.INSTANCE, request).get();
        }
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(
            ex.getMessage()
                .contains(
                    "Trying to create too many Point In Time contexts. "
                        + "Must be less than or equal to: ["
                        + SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY)
                        + "]. "
                        + "This limit can be set by changing the [search.max_open_pit_context] setting."
                )
        );
        final int maxPitContexts = SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY);
        validatePitStats("index", maxPitContexts, 0, 0);
        service.doClose();
        validatePitStats("index", 0, maxPitContexts, 0);
    }

    public void testCreatePitMoreThanMaxOpenPitContexts() throws Exception {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        SearchService service = getInstanceFromNode(SearchService.class);
        List<String> pitIds = new ArrayList<>();

        try {
            for (int i = 0; i < 1000; i++) {
                CreatePitResponse cpr = client().execute(CreatePitAction.INSTANCE, request).actionGet();
                if (cpr.getId() != null) pitIds.add(cpr.getId());
            }
        } catch (Exception ex) {
            assertTrue(
                ((SearchPhaseExecutionException) ex).getDetailedMessage()
                    .contains(
                        "Trying to create too many Point In Time contexts. "
                            + "Must be less than or equal to: ["
                            + SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY)
                            + "]. "
                            + "This limit can be set by changing the [search.max_open_pit_context] setting."
                    )
            );
        }
        final int maxPitContexts = SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY);
        validatePitStats("index", maxPitContexts, 0, 0);
        // deleteall
        DeletePitRequest deletePITRequest = new DeletePitRequest(pitIds.toArray(new String[pitIds.size()]));

        /*
          When we invoke delete again, returns success after clearing the remaining readers. Asserting reader context
          not found exceptions don't result in failures ( as deletion in one node is successful )
         */
        ActionFuture<DeletePitResponse> execute = client().execute(DeletePitAction.INSTANCE, deletePITRequest);
        DeletePitResponse deletePITResponse = execute.get();
        for (DeletePitInfo deletePitInfo : deletePITResponse.getDeletePitResults()) {
            assertThat(deletePitInfo.getPitId(), not(blankOrNullString()));
            assertTrue(deletePitInfo.isSuccessful());
        }
        validatePitStats("index", 0, maxPitContexts, 0);
        client().execute(CreatePitAction.INSTANCE, request).get();
        validatePitStats("index", 1, maxPitContexts, 0);
        service.doClose();
        validatePitStats("index", 0, maxPitContexts + 1, 0);
    }

    public void testOpenPitContextsConcurrently() throws Exception {
        createIndex("index");
        final int maxPitContexts = SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY);
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        SearchService service = getInstanceFromNode(SearchService.class);
        Thread[] threads = new Thread[randomIntBetween(2, 8)];
        CountDownLatch latch = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (;;) {
                        try {
                            client().execute(CreatePitAction.INSTANCE, request).get();
                        } catch (ExecutionException e) {
                            assertTrue(
                                e.getMessage()
                                    .contains(
                                        "Trying to create too many Point In Time contexts. "
                                            + "Must be less than or equal to: ["
                                            + SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY)
                                            + "]. "
                                            + "This limit can be set by changing the ["
                                            + SearchService.MAX_OPEN_PIT_CONTEXT.getKey()
                                            + "] setting."
                                    )
                            );
                            return;
                        }
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].setName("opensearch[node_s_0][search]");
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(service.getActiveContexts(), equalTo(maxPitContexts));
        validatePitStats("index", maxPitContexts, 0, 0);
        service.doClose();
        validatePitStats("index", 0, maxPitContexts, 0);
    }

    /**
     * Point in time search should return the same results as creation time and index updates should not affect the PIT search results
     */
    public void testPitAfterUpdateIndex() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 5)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        for (int i = 0; i < 50; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(
                    jsonBuilder().startObject()
                        .field("user", "foobar")
                        .field("postDate", System.currentTimeMillis())
                        .field("message", "test")
                        .endObject()
                )
                .get();
        }
        client().admin().indices().prepareRefresh().get();

        // create pit
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueMinutes(2), true);
        request.setIndices(new String[] { "test" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime(), TimeValue.timeValueMinutes(2));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertThat(
            client().prepareSearch()
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(0)
                .setQuery(matchAllQuery())
                .get()
                .getHits()
                .getTotalHits().value,
            Matchers.equalTo(50L)
        );

        assertThat(
            client().prepareSearch()
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(0)
                .setQuery(termQuery("message", "test"))
                .get()
                .getHits()
                .getTotalHits().value,
            Matchers.equalTo(50L)
        );
        assertThat(
            client().prepareSearch()
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(0)
                .setQuery(termQuery("message", "test"))
                .get()
                .getHits()
                .getTotalHits().value,
            Matchers.equalTo(50L)
        );
        assertThat(
            client().prepareSearch()
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(0)
                .setQuery(termQuery("message", "update"))
                .get()
                .getHits()
                .getTotalHits().value,
            Matchers.equalTo(0L)
        );
        assertThat(
            client().prepareSearch()
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                .setSize(0)
                .setQuery(termQuery("message", "update"))
                .get()
                .getHits()
                .getTotalHits().value,
            Matchers.equalTo(0L)
        );

        // update index
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(queryStringQuery("user:foobar"))
            .setSize(50)
            .addSort("postDate", SortOrder.ASC)
            .get();
        try {
            do {
                for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                    Map<String, Object> map = searchHit.getSourceAsMap();
                    map.put("message", "update");
                    client().prepareIndex("test").setId(searchHit.getId()).setSource(map).get();
                }
                searchResponse = client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get();

            } while (searchResponse.getHits().getHits().length > 0);

            client().admin().indices().prepareRefresh().get();
            assertThat(
                client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get().getHits().getTotalHits().value,
                Matchers.equalTo(50L)
            );
            /*
              assert without point in time
             */

            assertThat(
                client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get().getHits().getTotalHits().value,
                Matchers.equalTo(0L)
            );
            assertThat(
                client().prepareSearch().setSize(0).setQuery(termQuery("message", "test")).get().getHits().getTotalHits().value,
                Matchers.equalTo(0L)
            );
            assertThat(
                client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).get().getHits().getTotalHits().value,
                Matchers.equalTo(50L)
            );
            assertThat(
                client().prepareSearch().setSize(0).setQuery(termQuery("message", "update")).get().getHits().getTotalHits().value,
                Matchers.equalTo(50L)
            );
            /*
              using point in time id will have the same search results as ones before update
             */
            assertThat(
                client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                    .setSize(0)
                    .setQuery(termQuery("message", "test"))
                    .get()
                    .getHits()
                    .getTotalHits().value,
                Matchers.equalTo(50L)
            );
            assertThat(
                client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                    .setSize(0)
                    .setQuery(termQuery("message", "test"))
                    .get()
                    .getHits()
                    .getTotalHits().value,
                Matchers.equalTo(50L)
            );
            assertThat(
                client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                    .setSize(0)
                    .setQuery(termQuery("message", "update"))
                    .get()
                    .getHits()
                    .getTotalHits().value,
                Matchers.equalTo(0L)
            );
            assertThat(
                client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
                    .setSize(0)
                    .setQuery(termQuery("message", "update"))
                    .get()
                    .getHits()
                    .getTotalHits().value,
                Matchers.equalTo(0L)
            );
            validatePitStats("test", 1, 0, 0);
        } finally {
            service.doClose();
            assertEquals(0, service.getActiveContexts());
            validatePitStats("test", 0, 1, 0);
            PitTestsUtil.assertGetAllPitsEmpty(client());
            assertSegments(true, client());
        }
    }

    public void testConcurrentSearches() throws Exception {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        PitTestsUtil.assertUsingGetAllPits(client(), pitResponse.getId(), pitResponse.getCreationTime(), TimeValue.timeValueDays(1));
        assertSegments(false, client(), pitResponse.getId());
        Thread[] threads = new Thread[5];
        CountDownLatch latch = new CountDownLatch(threads.length);

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                    for (int j = 0; j < 50; j++) {
                        client().prepareSearch()
                            .setSize(2)
                            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                            .execute()
                            .get();
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            threads[i].setName("opensearch[node_s_0][search]");
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        validatePitStats("index", 1, 0, 0);
        validatePitStats("index", 1, 0, 1);
        service.doClose();
        assertEquals(0, service.getActiveContexts());
        validatePitStats("index", 0, 1, 0);
        validatePitStats("index", 0, 1, 1);
        PitTestsUtil.assertGetAllPitsEmpty(client());
    }

    public void validatePitStats(String index, long expectedPitCurrent, long expectedPitCount, int shardId) throws ExecutionException,
        InterruptedException {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(index));
        IndexShard indexShard = indexService.getShard(shardId);
        assertEquals(expectedPitCurrent, indexShard.searchStats().getTotal().getPitCurrent());
        assertEquals(expectedPitCount, indexShard.searchStats().getTotal().getPitCount());
    }
}
