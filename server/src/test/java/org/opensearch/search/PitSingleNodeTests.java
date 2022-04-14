/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.hamcrest.Matchers;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.search.CreatePITAction;
import org.opensearch.action.search.CreatePITRequest;
import org.opensearch.action.search.CreatePITResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class PitSingleNodeTests extends OpenSearchSingleNodeTestCase {
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
            .put(SearchService.CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    public void testCreatePITSuccess() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertHitCount(searchResponse, 1);

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
    }

    public void testCreatePITWithMultipleIndicesSuccess() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        createIndex("index1", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index1").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        SearchService service = getInstanceFromNode(SearchService.class);

        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse response = execute.get();
        assertEquals(4, response.getSuccessfulShards());
        assertEquals(4, service.getActiveContexts());
        service.doClose();
    }

    public void testCreatePITWithShardReplicasSuccess() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();

        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertHitCount(searchResponse, 1);

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        service.doClose();
    }

    public void testCreatePITWithNonExistentIndex() {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        SearchService service = getInstanceFromNode(SearchService.class);

        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);

        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(ex.getMessage().contains("no such index [index1]"));
        assertEquals(0, service.getActiveContexts());
        service.doClose();
    }

    public void testCreatePITOnCloseIndex() {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        client().admin().indices().prepareClose("index").get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);

        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(ex.getMessage().contains("IndexClosedException"));

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(0, service.getActiveContexts());
        service.doClose();
    }

    public void testPitSearchOnDeletedIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        client().admin().indices().prepareDelete("index").get();

        IndexNotFoundException ex = expectThrows(IndexNotFoundException.class, () -> {
            client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.getMessage().contains("no such index [index]"));
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(0, service.getActiveContexts());
        service.doClose();
    }

    public void testIllegalPitId() {
        createIndex("idx");
        String id = "c2Nhbjs2OzM0NDg1ODpzRlBLc0FXNlNyNm5JWUc1";
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(id).setKeepAlive(TimeValue.timeValueDays(1)))
                .get()
        );
        assertEquals("Cannot parse pit id", e.getMessage());
    }

    public void testPitSearchOnCloseIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        client().admin().indices().prepareClose("index").get();
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            SearchResponse searchResponse = client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        assertEquals(0, service.getActiveContexts());

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

    public void testSearchWithFirstPhaseKeepAliveExpiry() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueMillis(100), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        // since first phase temporary keep alive is set at 1 second in this test file
        // and create pit request keep alive is less than that, keep alive is set to 1 second, (max of 2 keep alives)
        // so reader context will clear up after 1 second
        Thread.sleep(1000);
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        assertEquals(0, service.getActiveContexts());
        service.doClose();
    }

    public void testSearchWithPitSecondPhaseKeepAliveExpiry() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueSeconds(2), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        Thread.sleep(1000);
        assertEquals(2, service.getActiveContexts());
        Thread.sleep(1500);
        assertEquals(0, service.getActiveContexts());
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch()
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        service.doClose();
    }

    public void testSearchWithPitKeepAliveExtension() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueSeconds(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        client().prepareSearch()
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueSeconds(3)))
            .get();
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        Thread.sleep(2500);
        assertEquals(2, service.getActiveContexts());
        Thread.sleep(1000);
        assertEquals(0, service.getActiveContexts());
        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch("index")
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueMinutes(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        service.doClose();
    }

    public void testMaxOpenPitContexts() throws Exception {
        createIndex("index");
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        SearchService service = getInstanceFromNode(SearchService.class);

        for (int i = 0; i < SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY); i++) {
            client().execute(CreatePITAction.INSTANCE, request).get();
        }
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
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
        service.doClose();
    }

    public void testOpenPitContextsConcurrently() throws Exception {
        createIndex("index");
        final int maxPitContexts = SearchService.MAX_OPEN_PIT_CONTEXT.get(Settings.EMPTY);
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
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
                            client().execute(CreatePITAction.INSTANCE, request).get();
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
        service.doClose();
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
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueMinutes(2), true);
        request.setIndices(new String[] { "test" });
        ActionFuture<CreatePITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        CreatePITResponse pitResponse = execute.get();
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
            /**
             * assert without point in time
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
            /**
             * using point in time id will have the same search results as ones before update
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
        } finally {
            service.doClose();
            assertEquals(0, service.getActiveContexts());
        }
    }

}
