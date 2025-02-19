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

package org.opensearch.client.documentation;

import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.AbstractBulkByScrollRequestBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.CancelTests;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexModulePlugin;
import org.opensearch.index.reindex.ReindexRequestBuilder;
import org.opensearch.index.reindex.RethrottleAction;
import org.opensearch.index.reindex.RethrottleRequestBuilder;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequestBuilder;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDocumentationIT extends OpenSearchIntegTestCase {

    // Semaphore used to allow & block indexing operations during the test
    private static final Semaphore ALLOWED_OPERATIONS = new Semaphore(0);
    private static final String INDEX_NAME = "source_index";

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexModulePlugin.class, ReindexCancellationPlugin.class);
    }

    @Before
    public void setup() {
        client().admin().indices().prepareCreate(INDEX_NAME).get();
    }

    @SuppressWarnings("unused")
    public void testReindex() {
        Client client = client();
        // tag::reindex1
        BulkByScrollResponse response =
          new ReindexRequestBuilder(client, ReindexAction.INSTANCE)
            .source("source_index")
            .destination("target_index")
            .filter(QueryBuilders.matchQuery("category", "xzy")) // <1>
            .get();
        // end::reindex1
    }

    @SuppressWarnings("unused")
    public void testUpdateByQuery() {
        Client client = client();
        client.admin().indices().prepareCreate("foo").get();
        client.admin().indices().prepareCreate("bar").get();
        client.admin().indices().preparePutMapping(INDEX_NAME).setSource("cat", "type=keyword").get();
        {
            // tag::update-by-query
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index").abortOnVersionConflict(false);
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query
        }
        {
            // tag::update-by-query-filter
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .filter(QueryBuilders.termQuery("level", "awesome"))
                .maxDocs(1000)
                .script(new Script(ScriptType.INLINE,
                    "painless",
                    "ctx._source.awesome = 'absolutely'",
                    Collections.emptyMap()));
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-filter

            // validate order of string params to Script constructor
            assertEquals(updateByQuery.request().getScript().getLang(), "painless");
        }
        {
            // tag::update-by-query-size
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .source()
                .setSize(500);
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-size
        }
        {
            // tag::update-by-query-sort
            UpdateByQueryRequestBuilder updateByQuery =
               new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .maxDocs(100)
                .source()
                .addSort("cat", SortOrder.DESC);
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-sort
        }
        {
            // tag::update-by-query-script
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("source_index")
                .script(new Script(
                    ScriptType.INLINE,
                    "painless",
                    "if (ctx._source.awesome == 'absolutely') {"
                        + "  ctx.op='noop'"
                        + "} else if (ctx._source.awesome == 'lame') {"
                        + "  ctx.op='delete'"
                        + "} else {"
                        + "ctx._source.awesome = 'absolutely'}",
                    Collections.emptyMap()));
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-script

            // validate order of string params to Script constructor
            assertEquals(updateByQuery.request().getScript().getLang(), "painless");
        }
        {
            // tag::update-by-query-multi-index
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source("foo", "bar");
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-multi-index
        }
        {
            // tag::update-by-query-routing
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.source().setRouting("cat");
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-routing
        }
        {
            // tag::update-by-query-pipeline
            UpdateByQueryRequestBuilder updateByQuery =
              new UpdateByQueryRequestBuilder(client, UpdateByQueryAction.INSTANCE);
            updateByQuery.setPipeline("hurray");
            BulkByScrollResponse response = updateByQuery.get();
            // end::update-by-query-pipeline
        }
    }

    public void testTasks() throws Exception {
        final Client client = client();
        final ReindexRequestBuilder builder = reindexAndPartiallyBlock();

        {
            // tag::update-by-query-list-tasks
            ListTasksResponse tasksList = client.admin().cluster().prepareListTasks()
                .setActions(UpdateByQueryAction.NAME).setDetailed(true).get();
            for (TaskInfo info: tasksList.getTasks()) {
                TaskId taskId = info.getTaskId();
                BulkByScrollTask.Status status =
                    (BulkByScrollTask.Status) info.getStatus();
                // do stuff
            }
            // end::update-by-query-list-tasks
        }

        TaskInfo mainTask = CancelTests.findTaskToCancel(ReindexAction.NAME, builder.request().getSlices());
        BulkByScrollTask.Status status = (BulkByScrollTask.Status) mainTask.getStatus();
        assertNull(status.getReasonCancelled());
        TaskId taskId = mainTask.getTaskId();
        {
            // tag::update-by-query-get-task
            GetTaskResponse get = client.admin().cluster().prepareGetTask(taskId).get();
            // end::update-by-query-get-task
        }
        {
            // tag::update-by-query-cancel-task
            // Cancel all update-by-query requests
            client.admin().cluster().prepareCancelTasks()
                .setActions(UpdateByQueryAction.NAME).get().getTasks();
            // Cancel a specific update-by-query request
            client.admin().cluster().prepareCancelTasks()
                .setTaskId(taskId).get().getTasks();
            // end::update-by-query-cancel-task
        }
        {
            // tag::update-by-query-rethrottle
            new RethrottleRequestBuilder(client, RethrottleAction.INSTANCE)
                .setTaskId(taskId)
                .setRequestsPerSecond(2.0f)
                .get();
            // end::update-by-query-rethrottle
        }

        // unblocking the blocked update
        ALLOWED_OPERATIONS.release(builder.request().getSlices());
    }

    @SuppressWarnings("unused")
    public void testDeleteByQuery() {
        Client client = client();
        client.admin().indices().prepareCreate("persons").get();

        // tag::delete-by-query-sync
        BulkByScrollResponse response =
          new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .filter(QueryBuilders.matchQuery("gender", "male")) // <1>
            .source("persons")                                  // <2>
            .get();                                             // <3>
        long deleted = response.getDeleted();                   // <4>
        // end::delete-by-query-sync

        // tag::delete-by-query-async
        new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
            .filter(QueryBuilders.matchQuery("gender", "male"))     // <1>
            .source("persons")                                      // <2>
            .execute(new ActionListener<BulkByScrollResponse>() {   // <3>
                @Override
                public void onResponse(BulkByScrollResponse response) {
                    long deleted = response.getDeleted();           // <4>
                }
                @Override
                public void onFailure(Exception e) {
                    // Handle the exception
                }
            });
        // end::delete-by-query-async
    }

    /**
     * Similar to what CancelTests does: blocks some operations to be able to catch some tasks in running state
     * @see CancelTests#testCancel(String, AbstractBulkByScrollRequestBuilder, CancelTests.CancelAssertion, Matcher)
     */
    private ReindexRequestBuilder reindexAndPartiallyBlock() throws Exception {
        final Client client = client();
        final int numDocs = randomIntBetween(10, 100);
        ALLOWED_OPERATIONS.release(numDocs);

        indexRandom(
            true,
            false,
            true,
            IntStream.range(0, numDocs)
                .mapToObj(i -> client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("n", Integer.toString(i)))
                .collect(Collectors.toList())
        );

        // Checks that the all documents have been indexed and correctly counted
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), numDocs);
        assertThat(ALLOWED_OPERATIONS.drainPermits(), equalTo(0));

        ReindexRequestBuilder builder = new ReindexRequestBuilder(client, ReindexAction.INSTANCE).source(INDEX_NAME)
            .destination("target_index");
        // Scroll by 1 so that cancellation is easier to control
        builder.source().setSize(1);

        int numModifiedDocs = randomIntBetween(builder.request().getSlices() * 2, numDocs);
        // chose to modify some of docs - rest is still blocked
        ALLOWED_OPERATIONS.release(numModifiedDocs - builder.request().getSlices());

        // Now execute the reindex action...
        builder.execute();

        // 10 seconds is usually fine but on heavily loaded machines this can take a while
        assertBusy(() -> {
            assertTrue("Expected some queued threads", ALLOWED_OPERATIONS.hasQueuedThreads());
            assertEquals("Expected that no permits are available", 0, ALLOWED_OPERATIONS.availablePermits());
        }, 1, TimeUnit.MINUTES);
        return builder;
    }

    public static class ReindexCancellationPlugin extends Plugin {

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexOperationListener(new BlockingOperationListener());
        }
    }

    public static class BlockingOperationListener implements IndexingOperationListener {

        @Override
        public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
            return preCheck(index);
        }

        @Override
        public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
            return preCheck(delete);
        }

        private <T extends Engine.Operation> T preCheck(T operation) {
            if ((operation.origin() != Engine.Operation.Origin.PRIMARY)) {
                return operation;
            }

            try {
                if (ALLOWED_OPERATIONS.tryAcquire(30, TimeUnit.SECONDS)) {
                    return operation;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("Something went wrong");
        }
    }

}
