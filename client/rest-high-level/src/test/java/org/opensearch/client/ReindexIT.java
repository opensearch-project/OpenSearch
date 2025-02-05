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

package org.opensearch.client;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.tasks.RawTaskStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class ReindexIT extends OpenSearchRestHighLevelClientTestCase {

    public void testReindex() throws IOException {
        final String sourceIndex = "source1";
        final String destinationIndex = "dest";
        {
            // Prepare
            Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
            createIndex(sourceIndex, settings);
            createIndex(destinationIndex, settings);
            BulkRequest bulkRequest = new BulkRequest().add(
                new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), MediaTypeRegistry.JSON)
            )
                .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo2", "bar2"), MediaTypeRegistry.JSON))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            assertEquals(RestStatus.OK, highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT).status());
        }
        {
            // reindex one document with id 1 from source to destination
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destinationIndex);
            reindexRequest.setSourceQuery(new IdsQueryBuilder().addIds("1"));
            reindexRequest.setRefresh(true);

            BulkByScrollResponse bulkResponse = execute(reindexRequest, highLevelClient()::reindex, highLevelClient()::reindexAsync);

            assertEquals(1, bulkResponse.getCreated());
            assertEquals(1, bulkResponse.getTotal());
            assertEquals(0, bulkResponse.getDeleted());
            assertEquals(0, bulkResponse.getNoops());
            assertEquals(0, bulkResponse.getVersionConflicts());
            assertEquals(1, bulkResponse.getBatches());
            assertTrue(bulkResponse.getTook().getMillis() > 0);
            assertEquals(1, bulkResponse.getBatches());
            assertEquals(0, bulkResponse.getBulkFailures().size());
            assertEquals(0, bulkResponse.getSearchFailures().size());
        }
        {
            // set require_alias=true, but there exists no alias
            ReindexRequest reindexRequest = new ReindexRequest();
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destinationIndex);
            reindexRequest.setSourceQuery(new IdsQueryBuilder().addIds("1"));
            reindexRequest.setRefresh(true);
            reindexRequest.setRequireAlias(true);

            OpenSearchStatusException exception = expectThrows(OpenSearchStatusException.class, () -> {
                execute(reindexRequest, highLevelClient()::reindex, highLevelClient()::reindexAsync);
            });
            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertEquals(
                "OpenSearch exception [type=index_not_found_exception, reason=no such index [dest] and [require_alias] request flag is [true] and [dest] is not an alias]",
                exception.getMessage()
            );
        }
    }

    public void testReindexTask() throws Exception {
        final String sourceIndex = "source123";
        final String destinationIndex = "dest2";
        {
            // Prepare
            Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
            createIndex(sourceIndex, settings);
            createIndex(destinationIndex, settings);
            BulkRequest bulkRequest = new BulkRequest().add(
                new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), MediaTypeRegistry.JSON)
            )
                .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo2", "bar2"), MediaTypeRegistry.JSON))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            assertEquals(RestStatus.OK, highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT).status());
        }
        {
            // tag::submit-reindex-task
            ReindexRequest reindexRequest = new ReindexRequest(); // <1>
            reindexRequest.setSourceIndices(sourceIndex);
            reindexRequest.setDestIndex(destinationIndex);
            reindexRequest.setRefresh(true);

            TaskSubmissionResponse reindexSubmission = highLevelClient()
                .submitReindexTask(reindexRequest, RequestOptions.DEFAULT); // <2>

            String taskId = reindexSubmission.getTask(); // <3>
            // end::submit-reindex-task

            assertBusy(checkTaskCompletionStatus(client(), taskId));
        }
    }

    public void testReindexConflict() throws IOException {
        final String sourceIndex = "testreindexconflict_source";
        final String destIndex = "testreindexconflict_dest";

        final Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
        createIndex(sourceIndex, settings);
        createIndex(destIndex, settings);
        final BulkRequest bulkRequest = new BulkRequest().add(
            new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", "bar"), MediaTypeRegistry.JSON)
        )
            .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo", "bar"), MediaTypeRegistry.JSON))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        assertThat(highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT).status(), equalTo(RestStatus.OK));

        putConflictPipeline();

        final ReindexRequest reindexRequest = new ReindexRequest();
        reindexRequest.setSourceIndices(sourceIndex);
        reindexRequest.setDestIndex(destIndex);
        reindexRequest.setRefresh(true);
        reindexRequest.setDestPipeline(CONFLICT_PIPELINE_ID);
        final BulkByScrollResponse response = highLevelClient().reindex(reindexRequest, RequestOptions.DEFAULT);

        assertThat(response.getVersionConflicts(), equalTo(2L));
        assertThat(response.getSearchFailures(), empty());
        assertThat(response.getBulkFailures(), hasSize(2));
        assertThat(
            response.getBulkFailures().stream().map(BulkItemResponse.Failure::getMessage).collect(Collectors.toSet()),
            everyItem(containsString("version conflict"))
        );

        assertThat(response.getTotal(), equalTo(2L));
        assertThat(response.getCreated(), equalTo(0L));
        assertThat(response.getUpdated(), equalTo(0L));
        assertThat(response.getDeleted(), equalTo(0L));
        assertThat(response.getNoops(), equalTo(0L));
        assertThat(response.getBatches(), equalTo(1));
        assertTrue(response.getTook().getMillis() > 0);
    }

    public void testDeleteByQuery() throws Exception {
        final String sourceIndex = "source1";
        {
            // Prepare
            Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
            createIndex(sourceIndex, settings);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    new BulkRequest().add(
                        new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", 1), MediaTypeRegistry.JSON)
                    )
                        .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo", 2), MediaTypeRegistry.JSON))
                        .add(new IndexRequest(sourceIndex).id("3").source(Collections.singletonMap("foo", 3), MediaTypeRegistry.JSON))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // test1: delete one doc
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.indices(sourceIndex);
            deleteByQueryRequest.setQuery(new IdsQueryBuilder().addIds("1"));
            deleteByQueryRequest.setRefresh(true);
            BulkByScrollResponse bulkResponse = execute(
                deleteByQueryRequest,
                highLevelClient()::deleteByQuery,
                highLevelClient()::deleteByQueryAsync
            );
            assertEquals(1, bulkResponse.getTotal());
            assertEquals(1, bulkResponse.getDeleted());
            assertEquals(0, bulkResponse.getNoops());
            assertEquals(0, bulkResponse.getVersionConflicts());
            assertEquals(1, bulkResponse.getBatches());
            assertTrue(bulkResponse.getTook().getMillis() > 0);
            assertEquals(1, bulkResponse.getBatches());
            assertEquals(0, bulkResponse.getBulkFailures().size());
            assertEquals(0, bulkResponse.getSearchFailures().size());
            assertEquals(
                2,
                highLevelClient().search(new SearchRequest(sourceIndex), RequestOptions.DEFAULT).getHits().getTotalHits().value()
            );
        }
        {
            // test delete-by-query rethrottling
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.indices(sourceIndex);
            deleteByQueryRequest.setQuery(new IdsQueryBuilder().addIds("2", "3"));
            deleteByQueryRequest.setRefresh(true);

            // this following settings are supposed to halt reindexing after first document
            deleteByQueryRequest.setBatchSize(1);
            deleteByQueryRequest.setRequestsPerSecond(0.00001f);
            final CountDownLatch taskFinished = new CountDownLatch(1);
            highLevelClient().deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {

                @Override
                public void onResponse(BulkByScrollResponse response) {
                    taskFinished.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e.toString());
                }
            });

            TaskId taskIdToRethrottle = findTaskToRethrottle(DeleteByQueryAction.NAME, deleteByQueryRequest.getDescription());
            float requestsPerSecond = 1000f;
            ListTasksResponse response = execute(
                new RethrottleRequest(taskIdToRethrottle, requestsPerSecond),
                highLevelClient()::deleteByQueryRethrottle,
                highLevelClient()::deleteByQueryRethrottleAsync
            );
            assertThat(response.getTasks(), hasSize(1));
            assertEquals(taskIdToRethrottle, response.getTasks().get(0).getTaskId());
            assertThat(response.getTasks().get(0).getStatus(), instanceOf(RawTaskStatus.class));
            assertEquals(
                Float.toString(requestsPerSecond),
                ((RawTaskStatus) response.getTasks().get(0).getStatus()).toMap().get("requests_per_second").toString()
            );
            assertTrue(taskFinished.await(10, TimeUnit.SECONDS));

            // any rethrottling after the delete-by-query is done performed with the same taskId should result in a failure
            response = execute(
                new RethrottleRequest(taskIdToRethrottle, requestsPerSecond),
                highLevelClient()::deleteByQueryRethrottle,
                highLevelClient()::deleteByQueryRethrottleAsync
            );
            assertTrue(response.getTasks().isEmpty());
            assertFalse(response.getNodeFailures().isEmpty());
            assertEquals(1, response.getNodeFailures().size());
            assertEquals(
                "OpenSearch exception [type=resource_not_found_exception, reason=task [" + taskIdToRethrottle + "] is missing]",
                response.getNodeFailures().get(0).getCause().getMessage()
            );
        }
    }

    public void testDeleteByQueryTask() throws Exception {
        final String sourceIndex = "source456";
        {
            // Prepare
            Settings settings = Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0).build();
            createIndex(sourceIndex, settings);
            assertEquals(
                RestStatus.OK,
                highLevelClient().bulk(
                    new BulkRequest().add(
                        new IndexRequest(sourceIndex).id("1").source(Collections.singletonMap("foo", 1), MediaTypeRegistry.JSON)
                    )
                        .add(new IndexRequest(sourceIndex).id("2").source(Collections.singletonMap("foo", 2), MediaTypeRegistry.JSON))
                        .add(new IndexRequest(sourceIndex).id("3").source(Collections.singletonMap("foo", 3), MediaTypeRegistry.JSON))
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE),
                    RequestOptions.DEFAULT
                ).status()
            );
        }
        {
            // tag::submit-delete_by_query-task
            DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
            deleteByQueryRequest.indices(sourceIndex);
            deleteByQueryRequest.setQuery(new IdsQueryBuilder().addIds("1"));
            deleteByQueryRequest.setRefresh(true);

            TaskSubmissionResponse deleteByQuerySubmission = highLevelClient()
                .submitDeleteByQueryTask(deleteByQueryRequest, RequestOptions.DEFAULT);

            String taskId = deleteByQuerySubmission.getTask();
            // end::submit-delete_by_query-task

            assertBusy(checkTaskCompletionStatus(client(), taskId));
        }
    }
}
