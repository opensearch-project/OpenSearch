/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.ActionFuture;
import org.opensearch.action.search.CreatePITAction;
import org.opensearch.action.search.CreatePITRequest;
import org.opensearch.action.search.CreatePITResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
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
            client().prepareSearch("index")
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.getMessage().contains("no such index [index]"));
        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(0, service.getActiveContexts());
        service.doClose();
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
            SearchResponse searchResponse = client().prepareSearch("index")
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.shardFailures()[0].reason().contains("SearchContextMissingException"));
        assertEquals(0, service.getActiveContexts());

        // PIT reader contexts are lost after close, verifying it with open index api
        client().admin().indices().prepareOpen("index").get();
        ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch("index")
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
        Thread.sleep(1000);
        // since first phase keep alive is set at 1 second in this test file and create pit request keep alive is
        // less than that, so reader context will clear up after a second
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        SearchPhaseExecutionException ex = expectThrows(SearchPhaseExecutionException.class, () -> {
            client().prepareSearch("index")
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
            client().prepareSearch("index")
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
        client().prepareSearch("index")
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

}
