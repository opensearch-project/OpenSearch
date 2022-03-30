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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.ExecutionException;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class PitSingleNodeTests extends OpenSearchSingleNodeTestCase {
    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testCreatePIT() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        SearchResponse pitResponse = execute.get();
        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.pointInTimeId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertHitCount(searchResponse, 1);

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
    }

    public void testCreatePITOnValidIndexAndThenDeleteIndex() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        SearchResponse pitResponse = execute.get();
        client().admin().indices().prepareDelete("index").get();

        IndexNotFoundException ex = expectThrows(IndexNotFoundException.class, () -> {
            SearchResponse searchResponse = client().prepareSearch("index")
                .setSize(2)
                .setPointInTime(new PointInTimeBuilder(pitResponse.pointInTimeId()).setKeepAlive(TimeValue.timeValueDays(1)))
                .get();
        });
        assertTrue(ex.getMessage().contains("no such index [index]"));
        SearchService service = getInstanceFromNode(SearchService.class);
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
        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);

        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(ex.getMessage().contains("IndexClosedException"));

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(0, service.getActiveContexts());
        service.doClose();
    }

    public void testCreatePITWithMultipleIndices() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        createIndex("index1", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index1").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        SearchService service = getInstanceFromNode(SearchService.class);

        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        SearchResponse response = execute.get();
        assertEquals(4, response.getSuccessfulShards());
        assertEquals(4, service.getActiveContexts());
        service.doClose();
    }

    public void testCreatePITWithNonExistentIndex() {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index", "index1" });
        SearchService service = getInstanceFromNode(SearchService.class);

        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);

        ExecutionException ex = expectThrows(ExecutionException.class, execute::get);

        assertTrue(ex.getMessage().contains("no such index [index1]"));
        assertEquals(0, service.getActiveContexts());
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
        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
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

    public void testCreatePITWithShardReplicas() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 1).build());
        client().prepareIndex("index").setId("1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        CreatePITRequest request = new CreatePITRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "index" });
        ActionFuture<SearchResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        SearchResponse pitResponse = execute.get();

        client().prepareIndex("index").setId("2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2)
            .setPointInTime(new PointInTimeBuilder(pitResponse.pointInTimeId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertHitCount(searchResponse, 1);

        SearchService service = getInstanceFromNode(SearchService.class);
        assertEquals(2, service.getActiveContexts());
        service.doClose();
    }
}
