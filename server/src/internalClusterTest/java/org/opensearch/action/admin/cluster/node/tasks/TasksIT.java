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

package org.opensearch.action.admin.cluster.node.tasks;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.admin.cluster.health.ClusterHealthAction;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.indices.refresh.RefreshAction;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeAction;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.support.replication.TransportReplicationActionTests;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.tasks.TaskResult;
import org.opensearch.tasks.TaskResultsService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.tasks.MockTaskManager;
import org.opensearch.test.tasks.MockTaskManagerListener;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.common.unit.TimeValue.timeValueSeconds;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFutureThrows;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

/**
 * Integration tests for task management API
 * <p>
 * We need at least 2 nodes so we have a cluster-manager node a non-cluster-manager node
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2)
public class TasksIT extends AbstractTasksIT {

    public void testTaskCounts() {
        // Run only on data nodes
        ListTasksResponse response = client().admin()
            .cluster()
            .prepareListTasks("data:true")
            .setActions(ListTasksAction.NAME + "[n]")
            .get();
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(cluster().numDataNodes()));
    }

    public void testClusterManagerNodeOperationTasks() {
        registerTaskManagerListeners(ClusterHealthAction.NAME);

        // First run the health on the cluster-manager node - should produce only one task on the cluster-manager node
        internalCluster().clusterManagerClient().admin().cluster().prepareHealth().get();
        assertEquals(1, numberOfEvents(ClusterHealthAction.NAME, Tuple::v1)); // counting only registration events
        assertEquals(1, numberOfEvents(ClusterHealthAction.NAME, event -> event.v1() == false)); // counting only unregistration events

        resetTaskManagerListeners(ClusterHealthAction.NAME);

        // Now run the health on a non-cluster-manager node - should produce one task on cluster-manager and one task on another node
        internalCluster().nonClusterManagerClient().admin().cluster().prepareHealth().get();
        assertEquals(2, numberOfEvents(ClusterHealthAction.NAME, Tuple::v1)); // counting only registration events
        assertEquals(2, numberOfEvents(ClusterHealthAction.NAME, event -> event.v1() == false)); // counting only unregistration events
        List<TaskInfo> tasks = findEvents(ClusterHealthAction.NAME, Tuple::v1);

        // Verify that one of these tasks is a parent of another task
        if (tasks.get(0).getParentTaskId().isSet()) {
            assertParentTask(Collections.singletonList(tasks.get(0)), tasks.get(1));
        } else {
            assertParentTask(Collections.singletonList(tasks.get(1)), tasks.get(0));
        }
    }

    public void testTransportReplicationAllShardsTasks() {
        registerTaskManagerListeners(ValidateQueryAction.NAME); // main task
        registerTaskManagerListeners(ValidateQueryAction.NAME + "[s]"); // shard
                                                                        // level
        // tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareValidateQuery("test").setAllShards(true).get();

        // the field stats operation should produce one main task
        NumShards numberOfShards = getNumShards("test");
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME, Tuple::v1));
        // and then one operation per shard
        assertEquals(numberOfShards.numPrimaries, numberOfEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1));

        // the shard level tasks should have the main task as a parent
        assertParentTask(findEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1), findEvents(ValidateQueryAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportBroadcastByNodeTasks() {
        registerTaskManagerListeners(UpgradeAction.NAME);  // main task
        registerTaskManagerListeners(UpgradeAction.NAME + "[n]"); // node level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareUpgrade("test").get();

        // the percolate operation should produce one main task
        assertEquals(1, numberOfEvents(UpgradeAction.NAME, Tuple::v1));
        // and then one operation per each node where shards are located
        assertEquals(internalCluster().nodesInclude("test").size(), numberOfEvents(UpgradeAction.NAME + "[n]", Tuple::v1));

        // all node level tasks should have the main task as a parent
        assertParentTask(findEvents(UpgradeAction.NAME + "[n]", Tuple::v1), findEvents(UpgradeAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportReplicationSingleShardTasks() {
        registerTaskManagerListeners(ValidateQueryAction.NAME);  // main task
        registerTaskManagerListeners(ValidateQueryAction.NAME + "[s]"); // shard level tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareValidateQuery("test").get();

        // the validate operation should produce one main task
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME, Tuple::v1));
        // and then one operation
        assertEquals(1, numberOfEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1));
        // the shard level operation should have the main task as its parent
        assertParentTask(findEvents(ValidateQueryAction.NAME + "[s]", Tuple::v1), findEvents(ValidateQueryAction.NAME, Tuple::v1).get(0));
    }

    public void testTransportBroadcastReplicationTasks() {
        registerTaskManagerListeners(RefreshAction.NAME);  // main task
        registerTaskManagerListeners(RefreshAction.NAME + "[s]"); // shard level tasks
        registerTaskManagerListeners(RefreshAction.NAME + "[s][*]"); // primary and replica shard tasks
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated
        client().admin().indices().prepareRefresh("test").get();

        // the refresh operation should produce one main task
        NumShards numberOfShards = getNumShards("test");

        logger.debug("number of shards, total: [{}], primaries: [{}] ", numberOfShards.totalNumShards, numberOfShards.numPrimaries);
        logger.debug("main events {}", numberOfEvents(RefreshAction.NAME, Tuple::v1));
        logger.debug("main event node {}", findEvents(RefreshAction.NAME, Tuple::v1).get(0).getTaskId().getNodeId());
        logger.debug("[s] events {}", numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1));
        logger.debug("[s][*] events {}", numberOfEvents(RefreshAction.NAME + "[s][*]", Tuple::v1));
        logger.debug("nodes with the index {}", internalCluster().nodesInclude("test"));

        assertEquals(1, numberOfEvents(RefreshAction.NAME, Tuple::v1));
        // Because it's broadcast replication action we will have as many [s] level requests
        // as we have primary shards on the coordinating node plus we will have one task per primary outside of the
        // coordinating node due to replication.
        // If all primaries are on the coordinating node, the number of tasks should be equal to the number of primaries
        // If all primaries are not on the coordinating node, the number of tasks should be equal to the number of primaries times 2
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1), greaterThanOrEqualTo(numberOfShards.numPrimaries));
        assertThat(numberOfEvents(RefreshAction.NAME + "[s]", Tuple::v1), lessThanOrEqualTo(numberOfShards.numPrimaries * 2));

        // Verify that all [s] events have the proper parent
        // This is complicated because if the shard task runs on the same node it has main task as a parent
        // but if it runs on non-coordinating node it would have another intermediate [s] task on the coordinating node as a parent
        TaskInfo mainTask = findEvents(RefreshAction.NAME, Tuple::v1).get(0);
        List<TaskInfo> sTasks = findEvents(RefreshAction.NAME + "[s]", Tuple::v1);
        for (TaskInfo taskInfo : sTasks) {
            if (mainTask.getTaskId().getNodeId().equals(taskInfo.getTaskId().getNodeId())) {
                // This shard level task runs on the same node as a parent task - it should have the main task as a direct parent
                assertParentTask(Collections.singletonList(taskInfo), mainTask);
            } else {
                String description = taskInfo.getDescription();
                // This shard level task runs on another node - it should have a corresponding shard level task on the node where main task
                // is running
                List<TaskInfo> sTasksOnRequestingNode = findEvents(
                    RefreshAction.NAME + "[s]",
                    event -> event.v1()
                        && mainTask.getTaskId().getNodeId().equals(event.v2().getTaskId().getNodeId())
                        && description.equals(event.v2().getDescription())
                );
                // There should be only one parent task
                assertEquals(1, sTasksOnRequestingNode.size());
                assertParentTask(Collections.singletonList(taskInfo), sTasksOnRequestingNode.get(0));
            }
        }

        // we will have as many [s][p] and [s][r] tasks as we have primary and replica shards
        assertEquals(numberOfShards.totalNumShards, numberOfEvents(RefreshAction.NAME + "[s][*]", Tuple::v1));

        // we the [s][p] and [s][r] tasks should have a corresponding [s] task on the same node as a parent
        List<TaskInfo> spEvents = findEvents(RefreshAction.NAME + "[s][*]", Tuple::v1);
        for (TaskInfo taskInfo : spEvents) {
            List<TaskInfo> sTask;
            if (taskInfo.getAction().endsWith("[s][p]")) {
                // A [s][p] level task should have a corresponding [s] level task on the same node
                sTask = findEvents(
                    RefreshAction.NAME + "[s]",
                    event -> event.v1()
                        && taskInfo.getTaskId().getNodeId().equals(event.v2().getTaskId().getNodeId())
                        && taskInfo.getDescription().equals(event.v2().getDescription())
                );
            } else {
                // A [s][r] level task should have a corresponding [s] level task on the a different node (where primary is located)
                sTask = findEvents(
                    RefreshAction.NAME + "[s]",
                    event -> event.v1()
                        && taskInfo.getParentTaskId().getNodeId().equals(event.v2().getTaskId().getNodeId())
                        && taskInfo.getDescription().equals(event.v2().getDescription())
                );
            }
            // There should be only one parent task
            assertEquals(1, sTask.size());
            assertParentTask(Collections.singletonList(taskInfo), sTask.get(0));
        }
    }

    public void testTransportBulkTasks() {
        registerTaskManagerListeners(BulkAction.NAME);  // main task
        registerTaskManagerListeners(BulkAction.NAME + "[s]");  // shard task
        registerTaskManagerListeners(BulkAction.NAME + "[s][p]");  // shard task on primary
        registerTaskManagerListeners(BulkAction.NAME + "[s][r]");  // shard task on replica
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated to catch replication tasks
        // ensures the mapping is available on all nodes so we won't retry the request (in case replicas don't have the right mapping).
        client().admin().indices().preparePutMapping("test").setSource("foo", "type=keyword").get();
        client().prepareBulk().add(client().prepareIndex("test").setId("test_id").setSource("{\"foo\": \"bar\"}", XContentType.JSON)).get();

        // the bulk operation should produce one main task
        List<TaskInfo> topTask = findEvents(BulkAction.NAME, Tuple::v1);
        assertEquals(1, topTask.size());
        assertEquals("requests[1], indices[test]", topTask.get(0).getDescription());

        // we should also get 1 or 2 [s] operation with main operation as a parent
        // in case the primary is located on the coordinating node we will have 1 operation, otherwise - 2
        List<TaskInfo> shardTasks = findEvents(BulkAction.NAME + "[s]", Tuple::v1);
        assertThat(shardTasks.size(), allOf(lessThanOrEqualTo(2), greaterThanOrEqualTo(1)));

        // Select the effective shard task
        TaskInfo shardTask;
        if (shardTasks.size() == 1) {
            // we have only one task - it's going to be the parent task for all [s][p] and [s][r] tasks
            shardTask = shardTasks.get(0);
            // and it should have the main task as a parent
            assertParentTask(shardTask, findEvents(BulkAction.NAME, Tuple::v1).get(0));
        } else {
            if (shardTasks.get(0).getParentTaskId().equals(shardTasks.get(1).getTaskId())) {
                // task 1 is the parent of task 0, that means that task 0 will control [s][p] and [s][r] tasks
                shardTask = shardTasks.get(0);
                // in turn the parent of the task 1 should be the main task
                assertParentTask(shardTasks.get(1), findEvents(BulkAction.NAME, Tuple::v1).get(0));
            } else {
                // otherwise task 1 will control [s][p] and [s][r] tasks
                shardTask = shardTasks.get(1);
                // in turn the parent of the task 0 should be the main task
                assertParentTask(shardTasks.get(0), findEvents(BulkAction.NAME, Tuple::v1).get(0));
            }
        }
        assertThat(shardTask.getDescription(), startsWith("requests[1], index[test]["));

        // we should also get one [s][p] operation with shard operation as a parent
        assertEquals(1, numberOfEvents(BulkAction.NAME + "[s][p]", Tuple::v1));
        assertParentTask(findEvents(BulkAction.NAME + "[s][p]", Tuple::v1), shardTask);

        // we should get as many [s][r] operations as we have replica shards
        // they all should have the same shard task as a parent
        assertEquals(getNumShards("test").numReplicas, numberOfEvents(BulkAction.NAME + "[s][r]", Tuple::v1));
        assertParentTask(findEvents(BulkAction.NAME + "[s][r]", Tuple::v1), shardTask);
    }

    public void testSearchTaskDescriptions() {
        registerTaskManagerListeners(SearchAction.NAME);  // main task
        registerTaskManagerListeners(SearchAction.NAME + "[*]");  // shard task
        createIndex("test");
        ensureGreen("test"); // Make sure all shards are allocated to catch replication tasks
        client().prepareIndex("test")
            .setId("test_id")
            .setSource("{\"foo\": \"bar\"}", XContentType.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Map<String, String> headers = new HashMap<>();
        headers.put(Task.X_OPAQUE_ID, "my_id");
        headers.put("Foo-Header", "bar");
        headers.put("Custom-Task-Header", "my_value");
        assertSearchResponse(client().filterWithHeader(headers).prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).get());

        // the search operation should produce one main task
        List<TaskInfo> mainTask = findEvents(SearchAction.NAME, Tuple::v1);
        assertEquals(1, mainTask.size());
        assertThat(mainTask.get(0).getDescription(), startsWith("indices[test], search_type["));
        assertThat(mainTask.get(0).getDescription(), containsString("\"query\":{\"match_all\""));
        assertTaskHeaders(mainTask.get(0));

        // check that if we have any shard-level requests they all have non-zero length description
        List<TaskInfo> shardTasks = findEvents(SearchAction.NAME + "[*]", Tuple::v1);
        for (TaskInfo taskInfo : shardTasks) {
            assertThat(taskInfo.getParentTaskId(), notNullValue());
            assertEquals(mainTask.get(0).getTaskId(), taskInfo.getParentTaskId());
            assertTaskHeaders(taskInfo);
            switch (taskInfo.getAction()) {
                case SearchTransportService.QUERY_ACTION_NAME:
                case SearchTransportService.DFS_ACTION_NAME:
                    assertTrue(taskInfo.getDescription(), Regex.simpleMatch("shardId[[test][*]]", taskInfo.getDescription()));
                    break;
                case SearchTransportService.QUERY_ID_ACTION_NAME:
                    assertTrue(taskInfo.getDescription(), Regex.simpleMatch("id[*], indices[test]", taskInfo.getDescription()));
                    break;
                case SearchTransportService.FETCH_ID_ACTION_NAME:
                    assertTrue(
                        taskInfo.getDescription(),
                        Regex.simpleMatch("id[*], size[1], lastEmittedDoc[null]", taskInfo.getDescription())
                    );
                    break;
                case SearchTransportService.QUERY_CAN_MATCH_NAME:
                    assertTrue(taskInfo.getDescription(), Regex.simpleMatch("shardId[[test][*]]", taskInfo.getDescription()));
                    break;
                default:
                    fail("Unexpected action [" + taskInfo.getAction() + "] with description [" + taskInfo.getDescription() + "]");
            }
            // assert that all task descriptions have non-zero length
            assertThat(taskInfo.getDescription().length(), greaterThan(0));
        }

    }

    public void testSearchTaskHeaderLimit() {
        int maxSize = Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.getDefault(Settings.EMPTY).getBytes() / 2 + 1);

        Map<String, String> headers = new HashMap<>();
        headers.put(Task.X_OPAQUE_ID, "my_id");
        headers.put("Custom-Task-Header", randomAlphaOfLengthBetween(maxSize, maxSize + 100));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().filterWithHeader(headers).admin().cluster().prepareListTasks().get()
        );
        assertThat(ex.getMessage(), startsWith("Request exceeded the maximum size of task headers "));
    }

    private void assertTaskHeaders(TaskInfo taskInfo) {
        assertThat(taskInfo.getHeaders().keySet(), hasSize(2));
        assertEquals("my_id", taskInfo.getHeaders().get(Task.X_OPAQUE_ID));
        assertEquals("my_value", taskInfo.getHeaders().get("Custom-Task-Header"));
    }

    /**
     * Very basic "is it plugged in" style test that indexes a document and makes sure that you can fetch the status of the process. The
     * goal here is to verify that the large moving parts that make fetching task status work fit together rather than to verify any
     * particular status results from indexing. For that, look at {@link TransportReplicationActionTests}. We intentionally don't use the
     * task recording mechanism used in other places in this test so we can make sure that the status fetching works properly over the wire.
     */
    public void testCanFetchIndexStatus() throws Exception {
        // First latch waits for the task to start, second on blocks it from finishing.
        CountDownLatch taskRegistered = new CountDownLatch(1);
        CountDownLatch letTaskFinish = new CountDownLatch(1);
        Thread index = null;
        try {
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                ((MockTaskManager) transportService.getTaskManager()).addListener(new MockTaskManagerListener() {
                    @Override
                    public void onTaskRegistered(Task task) {
                        if (task.getAction().startsWith(IndexAction.NAME)) {
                            taskRegistered.countDown();
                            logger.debug("Blocking [{}] starting", task);
                            try {
                                assertTrue(letTaskFinish.await(10, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    @Override
                    public void onTaskUnregistered(Task task) {}

                    @Override
                    public void waitForTaskCompletion(Task task) {}

                    @Override
                    public void taskExecutionStarted(Task task, Boolean closeableInvoked) {}
                });
            }
            // Need to run the task in a separate thread because node client's .execute() is blocked by our task listener
            index = new Thread(() -> {
                IndexResponse indexResponse = client().prepareIndex("test").setSource("test", "test").get();
                assertArrayEquals(ReplicationResponse.EMPTY, indexResponse.getShardInfo().getFailures());
            });
            index.start();
            assertTrue(taskRegistered.await(10, TimeUnit.SECONDS)); // waiting for at least one task to be registered

            ListTasksResponse listResponse = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions("indices:data/write/index*")
                .setDetailed(true)
                .get();
            assertThat(listResponse.getTasks(), not(empty()));
            for (TaskInfo task : listResponse.getTasks()) {
                assertNotNull(task.getStatus());
                GetTaskResponse getResponse = client().admin().cluster().prepareGetTask(task.getTaskId()).get();
                assertFalse("task should still be running", getResponse.getTask().isCompleted());
                TaskInfo fetchedWithGet = getResponse.getTask().getTask();
                assertEquals(task.getId(), fetchedWithGet.getId());
                assertEquals(task.getType(), fetchedWithGet.getType());
                assertEquals(task.getAction(), fetchedWithGet.getAction());
                assertEquals(task.getDescription(), fetchedWithGet.getDescription());
                assertEquals(task.getStatus(), fetchedWithGet.getStatus());
                assertEquals(task.getStartTime(), fetchedWithGet.getStartTime());
                assertThat(fetchedWithGet.getRunningTimeNanos(), greaterThanOrEqualTo(task.getRunningTimeNanos()));
                assertEquals(task.isCancellable(), fetchedWithGet.isCancellable());
                assertEquals(task.getParentTaskId(), fetchedWithGet.getParentTaskId());
            }
        } finally {
            letTaskFinish.countDown();
            if (index != null) {
                index.join();
            }
            assertBusy(() -> {
                assertEquals(
                    emptyList(),
                    client().admin().cluster().prepareListTasks().setActions("indices:data/write/index*").get().getTasks()
                );
            });
        }
    }

    public void testTasksCancellation() throws Exception {
        // Start blocking test task
        // Get real client (the plugin is not registered on transport nodes)
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        ActionFuture<TestTaskPlugin.NodesResponse> future = client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request);

        logger.info("--> started test tasks");

        // Wait for the task to start on all nodes
        assertBusy(
            () -> assertEquals(
                internalCluster().size(),
                client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").get().getTasks().size()
            )
        );

        logger.info("--> cancelling the main test task");
        CancelTasksResponse cancelTasksResponse = client().admin()
            .cluster()
            .prepareCancelTasks()
            .setActions(TestTaskPlugin.TestTaskAction.NAME)
            .get();
        assertEquals(1, cancelTasksResponse.getTasks().size());

        // Tasks are marked as cancelled at this point but not yet completed.
        List<TaskInfo> taskInfoList = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(TestTaskPlugin.TestTaskAction.NAME + "*")
            .get()
            .getTasks();
        for (TaskInfo taskInfo : taskInfoList) {
            assertTrue(taskInfo.isCancelled());
            assertNotNull(taskInfo.getCancellationStartTime());
        }
        future.get();

        logger.info("--> checking that test tasks are not running");
        assertEquals(
            0,
            client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "*").get().getTasks().size()
        );
    }

    public void testTasksUnblocking() throws Exception {
        // Start blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        ActionFuture<TestTaskPlugin.NodesResponse> future = client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request);
        // Wait for the task to start on all nodes
        assertBusy(
            () -> assertEquals(
                internalCluster().size(),
                client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").get().getTasks().size()
            )
        );

        new TestTaskPlugin.UnblockTestTasksRequestBuilder(client(), TestTaskPlugin.UnblockTestTasksAction.INSTANCE).get();

        future.get();
        assertEquals(
            0,
            client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]").get().getTasks().size()
        );
    }

    public void testListTasksWaitForCompletion() throws Exception {
        waitForCompletionTestCase(
            randomBoolean(),
            id -> client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(TestTaskPlugin.TestTaskAction.NAME)
                .setWaitForCompletion(true)
                .execute(),
            response -> {
                assertThat(response.getNodeFailures(), empty());
                assertThat(response.getTaskFailures(), empty());
                assertThat(response.getTasks(), hasSize(1));
                TaskInfo task = response.getTasks().get(0);
                assertEquals(TestTaskPlugin.TestTaskAction.NAME, task.getAction());
            }
        );
    }

    public void testGetTaskWaitForCompletionWithoutStoringResult() throws Exception {
        waitForCompletionTestCase(
            false,
            id -> client().admin().cluster().prepareGetTask(id).setWaitForCompletion(true).execute(),
            response -> {
                assertTrue(response.getTask().isCompleted());
                // We didn't store the result so it won't come back when we wait
                assertNull(response.getTask().getResponse());
                // But the task's details should still be there because we grabbed a reference to the task before waiting for it to complete
                assertNotNull(response.getTask().getTask());
                assertEquals(TestTaskPlugin.TestTaskAction.NAME, response.getTask().getTask().getAction());
            }
        );
    }

    public void testGetTaskWaitForCompletionWithStoringResult() throws Exception {
        waitForCompletionTestCase(
            true,
            id -> client().admin().cluster().prepareGetTask(id).setWaitForCompletion(true).execute(),
            response -> {
                assertTrue(response.getTask().isCompleted());
                // We stored the task so we should get its results
                assertEquals(0, response.getTask().getResponseAsMap().get("failure_count"));
                // The task's details should also be there
                assertNotNull(response.getTask().getTask());
                assertEquals(TestTaskPlugin.TestTaskAction.NAME, response.getTask().getTask().getAction());
            }
        );
    }

    /**
     * Test wait for completion.
     * @param storeResult should the task store its results
     * @param wait start waiting for a task. Accepts that id of the task to wait for and returns a future waiting for it.
     * @param validator validate the response and return the task ids that were found
     */
    private <T> void waitForCompletionTestCase(boolean storeResult, Function<TaskId, ActionFuture<T>> wait, Consumer<T> validator)
        throws Exception {
        // Start blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        request.setShouldStoreResult(storeResult);
        ActionFuture<TestTaskPlugin.NodesResponse> future = client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request);

        ActionFuture<T> waitResponseFuture;
        TaskId taskId;
        try {
            taskId = waitForTestTaskStartOnAllNodes();

            // Wait for the task to start
            assertBusy(() -> client().admin().cluster().prepareGetTask(taskId).get());

            // Register listeners so we can be sure the waiting started
            CountDownLatch waitForWaitingToStart = new CountDownLatch(1);
            for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
                ((MockTaskManager) transportService.getTaskManager()).addListener(new MockTaskManagerListener() {
                    @Override
                    public void waitForTaskCompletion(Task task) {
                        waitForWaitingToStart.countDown();
                    }

                    @Override
                    public void taskExecutionStarted(Task task, Boolean closeableInvoked) {}

                    @Override
                    public void onTaskRegistered(Task task) {}

                    @Override
                    public void onTaskUnregistered(Task task) {}
                });
            }

            // Spin up a request to wait for the test task to finish
            waitResponseFuture = wait.apply(taskId);

            /* Wait for the wait to start. This should count down just *before* we wait for completion but after the list/get has got a
             * reference to the running task. Because we unblock immediately after this the task may no longer be running for us to wait
             * on which is fine. */
            waitForWaitingToStart.await();
        } finally {
            // Unblock the request so the wait for completion request can finish
            new TestTaskPlugin.UnblockTestTasksRequestBuilder(client(), TestTaskPlugin.UnblockTestTasksAction.INSTANCE).get();
        }

        // Now that the task is unblocked the list response will come back
        T waitResponse = waitResponseFuture.get();
        validator.accept(waitResponse);

        TestTaskPlugin.NodesResponse response = future.get();
        assertEquals(emptyList(), response.failures());
    }

    public void testListTasksWaitForTimeout() throws Exception {
        waitForTimeoutTestCase(id -> {
            ListTasksResponse response = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(TestTaskPlugin.TestTaskAction.NAME)
                .setWaitForCompletion(true)
                .setTimeout(timeValueMillis(100))
                .get();
            assertThat(response.getNodeFailures(), not(empty()));
            return response.getNodeFailures();
        });
    }

    public void testGetTaskWaitForTimeout() throws Exception {
        waitForTimeoutTestCase(id -> {
            Exception e = expectThrows(
                Exception.class,
                () -> client().admin().cluster().prepareGetTask(id).setWaitForCompletion(true).setTimeout(timeValueMillis(100)).get()
            );
            return singleton(e);
        });
    }

    /**
     * Test waiting for a task that times out.
     * @param wait wait for the running task and return all the failures you accumulated waiting for it
     */
    private void waitForTimeoutTestCase(Function<TaskId, ? extends Iterable<? extends Throwable>> wait) throws Exception {
        // Start blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        ActionFuture<TestTaskPlugin.NodesResponse> future = client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request);
        try {
            TaskId taskId = waitForTestTaskStartOnAllNodes();

            // Wait for the task to start
            assertBusy(() -> client().admin().cluster().prepareGetTask(taskId).get());

            // Spin up a request that should wait for those tasks to finish
            // It will timeout because we haven't unblocked the tasks
            Iterable<? extends Throwable> failures = wait.apply(taskId);

            for (Throwable failure : failures) {
                assertNotNull(ExceptionsHelper.unwrap(failure, OpenSearchTimeoutException.class, ReceiveTimeoutTransportException.class));
            }
        } finally {
            // Now we can unblock those requests
            new TestTaskPlugin.UnblockTestTasksRequestBuilder(client(), TestTaskPlugin.UnblockTestTasksAction.INSTANCE).get();
        }
        future.get();
    }

    /**
     * Wait for the test task to be running on all nodes and return the TaskId of the primary task.
     */
    private TaskId waitForTestTaskStartOnAllNodes() throws Exception {
        assertBusy(() -> {
            List<TaskInfo> tasks = client().admin()
                .cluster()
                .prepareListTasks()
                .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]")
                .get()
                .getTasks();
            assertEquals(internalCluster().size(), tasks.size());
        });
        List<TaskInfo> task = client().admin().cluster().prepareListTasks().setActions(TestTaskPlugin.TestTaskAction.NAME).get().getTasks();
        assertThat(task, hasSize(1));
        return task.get(0).getTaskId();
    }

    public void testTasksListWaitForNoTask() throws Exception {
        // Spin up a request to wait for no matching tasks
        ActionFuture<ListTasksResponse> waitResponseFuture = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(TestTaskPlugin.TestTaskAction.NAME + "[n]")
            .setWaitForCompletion(true)
            .setTimeout(timeValueMillis(10))
            .execute();

        // It should finish quickly and without complaint
        assertThat(waitResponseFuture.get().getTasks(), empty());
    }

    public void testTasksGetWaitForNoTask() throws Exception {
        // Spin up a request to wait for no matching tasks
        ActionFuture<GetTaskResponse> waitResponseFuture = client().admin()
            .cluster()
            .prepareGetTask("notfound:1")
            .setWaitForCompletion(true)
            .setTimeout(timeValueMillis(10))
            .execute();

        // It should finish quickly and without complaint
        expectNotFound(waitResponseFuture::get);
    }

    public void testTasksWaitForAllTask() throws Exception {
        // Spin up a request to wait for all tasks in the cluster to make sure it doesn't cause an infinite loop
        ListTasksResponse response = client().admin()
            .cluster()
            .prepareListTasks()
            .setWaitForCompletion(true)
            .setTimeout(timeValueSeconds(10))
            .get();

        // It should finish quickly and without complaint and list the list tasks themselves
        assertThat(response.getNodeFailures(), emptyCollectionOf(OpenSearchException.class));
        assertThat(response.getTaskFailures(), emptyCollectionOf(TaskOperationFailure.class));
        assertThat(response.getTasks().size(), greaterThanOrEqualTo(1));
    }

    public void testTaskStoringSuccessfulResult() throws Exception {
        registerTaskManagerListeners(TestTaskPlugin.TestTaskAction.NAME);  // we need this to get task id of the process

        // Start non-blocking test task
        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        request.setShouldStoreResult(true);
        request.setShouldBlock(false);
        TaskId parentTaskId = new TaskId("parent_node", randomLong());
        request.setParentTask(parentTaskId);

        client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request).get();

        List<TaskInfo> events = findEvents(TestTaskPlugin.TestTaskAction.NAME, Tuple::v1);

        assertEquals(1, events.size());
        TaskInfo taskInfo = events.get(0);
        TaskId taskId = taskInfo.getTaskId();

        TaskResult taskResult = client().admin().cluster().getTask(new GetTaskRequest().setTaskId(taskId)).get().getTask();
        assertTrue(taskResult.isCompleted());
        assertNull(taskResult.getError());

        assertEquals(taskInfo.getTaskId(), taskResult.getTask().getTaskId());
        assertEquals(taskInfo.getParentTaskId(), taskResult.getTask().getParentTaskId());
        assertEquals(taskInfo.getType(), taskResult.getTask().getType());
        assertEquals(taskInfo.getAction(), taskResult.getTask().getAction());
        assertEquals(taskInfo.getDescription(), taskResult.getTask().getDescription());
        assertEquals(taskInfo.getStartTime(), taskResult.getTask().getStartTime());
        assertEquals(taskInfo.getHeaders(), taskResult.getTask().getHeaders());
        Map<?, ?> result = taskResult.getResponseAsMap();
        assertEquals("0", result.get("failure_count").toString());

        assertNoFailures(client().admin().indices().prepareRefresh(TaskResultsService.TASK_INDEX).get());

        SearchResponse searchResponse = client().prepareSearch(TaskResultsService.TASK_INDEX)
            .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("task.action", taskInfo.getAction())))
            .get();

        assertEquals(1L, searchResponse.getHits().getTotalHits().value);

        searchResponse = client().prepareSearch(TaskResultsService.TASK_INDEX)
            .setSource(SearchSourceBuilder.searchSource().query(QueryBuilders.termQuery("task.node", taskInfo.getTaskId().getNodeId())))
            .get();

        assertEquals(1L, searchResponse.getHits().getTotalHits().value);

        GetTaskResponse getResponse = expectFinishedTask(taskId);
        assertEquals(result, getResponse.getTask().getResponseAsMap());
        assertNull(getResponse.getTask().getError());

        // run it again to check that the tasks index has been successfully created and can be re-used
        client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request).get();
        events = findEvents(TestTaskPlugin.TestTaskAction.NAME, Tuple::v1);
        assertEquals(2, events.size());
    }

    public void testTaskStoringFailureResult() throws Exception {
        registerTaskManagerListeners(TestTaskPlugin.TestTaskAction.NAME);  // we need this to get task id of the process

        TestTaskPlugin.NodesRequest request = new TestTaskPlugin.NodesRequest("test");
        request.setShouldFail(true);
        request.setShouldStoreResult(true);
        request.setShouldBlock(false);

        // Start non-blocking test task that should fail
        assertFutureThrows(client().execute(TestTaskPlugin.TestTaskAction.INSTANCE, request), IllegalStateException.class);

        List<TaskInfo> events = findEvents(TestTaskPlugin.TestTaskAction.NAME, Tuple::v1);
        assertEquals(1, events.size());
        TaskInfo failedTaskInfo = events.get(0);
        TaskId failedTaskId = failedTaskInfo.getTaskId();

        TaskResult taskResult = client().admin().cluster().getTask(new GetTaskRequest().setTaskId(failedTaskId)).get().getTask();
        assertTrue(taskResult.isCompleted());
        assertNull(taskResult.getResponse());

        assertEquals(failedTaskInfo.getTaskId(), taskResult.getTask().getTaskId());
        assertEquals(failedTaskInfo.getType(), taskResult.getTask().getType());
        assertEquals(failedTaskInfo.getAction(), taskResult.getTask().getAction());
        assertEquals(failedTaskInfo.getDescription(), taskResult.getTask().getDescription());
        assertEquals(failedTaskInfo.getStartTime(), taskResult.getTask().getStartTime());
        assertEquals(failedTaskInfo.getHeaders(), taskResult.getTask().getHeaders());
        Map<?, ?> error = (Map<?, ?>) taskResult.getErrorAsMap();
        assertEquals("Simulating operation failure", error.get("reason"));
        assertEquals("illegal_state_exception", error.get("type"));

        GetTaskResponse getResponse = expectFinishedTask(failedTaskId);
        assertNull(getResponse.getTask().getResponse());
        assertEquals(error, getResponse.getTask().getErrorAsMap());
    }

    public void testGetTaskNotFound() throws Exception {
        // Node isn't found, tasks index doesn't even exist
        expectNotFound(() -> client().admin().cluster().prepareGetTask("not_a_node:1").get());

        // Node exists but the task still isn't found
        expectNotFound(() -> client().admin().cluster().prepareGetTask(new TaskId(internalCluster().getNodeNames()[0], 1)).get());
    }

    public void testNodeNotFoundButTaskFound() throws Exception {
        // Save a fake task that looks like it is from a node that isn't part of the cluster
        CyclicBarrier b = new CyclicBarrier(2);
        TaskResultsService resultsService = internalCluster().getInstance(TaskResultsService.class);
        resultsService.storeResult(
            new TaskResult(
                new TaskInfo(
                    new TaskId("fake", 1),
                    "test",
                    "test",
                    "",
                    null,
                    0,
                    0,
                    false,
                    false,
                    TaskId.EMPTY_TASK_ID,
                    Collections.emptyMap(),
                    null
                ),
                new RuntimeException("test")
            ),
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void response) {
                    try {
                        b.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        b.await();

        // Now we can find it!
        GetTaskResponse response = expectFinishedTask(new TaskId("fake:1"));
        assertEquals("test", response.getTask().getTask().getAction());
        assertNotNull(response.getTask().getError());
        assertNull(response.getTask().getResponse());
    }
}
