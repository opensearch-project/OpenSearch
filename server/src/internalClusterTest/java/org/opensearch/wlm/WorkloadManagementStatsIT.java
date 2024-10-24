/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.wlm.WlmStatsAction;
import org.opensearch.action.admin.cluster.wlm.WlmStatsRequest;
import org.opensearch.action.admin.cluster.wlm.WlmStatsResponse;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.backpressure.SearchBackpressureIT.ExceptionCatchingListener;
import org.opensearch.search.backpressure.SearchBackpressureIT.TaskFactory;
import org.opensearch.search.backpressure.SearchBackpressureIT.TestResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;
import static org.opensearch.wlm.QueryGroupTask.QUERY_GROUP_ID_HEADER;

public class WorkloadManagementStatsIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    final static String PUT = "PUT";
    final static String DELETE = "DELETE";
    final static String DEFAULT_QUERY_GROUP = "DEFAULT_QUERY_GROUP";
    final static String _ALL = "_all";
    final static String CPU = "CPU";
    final static String MEMORY = "MEMORY";
    final static String NAME1 = "name1";
    final static String NAME2 = "name2";
    final static String INVALID_ID = "invalid_id";
    private static final TimeValue TIMEOUT = new TimeValue(3, TimeUnit.SECONDS);

    public WorkloadManagementStatsIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestClusterUpdatePlugin.class);
        return plugins;
    }

    public void testDefaultQueryGroup() throws ExecutionException, InterruptedException {
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(response, new String[] { DEFAULT_QUERY_GROUP }, null);
    }

    public void testBasicWlmStats() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(response, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        updateQueryGroupInClusterState(DELETE, queryGroup);
        WlmStatsResponse response2 = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(response2, new String[] { DEFAULT_QUERY_GROUP }, new String[] { id });
    }

    public void testWlmStatsWithQueryGroupId() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { id }, null);
        validateResponse(response, new String[] { id }, new String[] { DEFAULT_QUERY_GROUP });

        WlmStatsResponse response2 = getWlmStatsResponse(null, new String[] { DEFAULT_QUERY_GROUP }, null);
        validateResponse(response2, new String[] { DEFAULT_QUERY_GROUP }, new String[] { id });

        QueryGroup queryGroup2 = new QueryGroup(
            NAME2,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.2))
        );
        String id2 = queryGroup2.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup2);
        WlmStatsResponse response3 = getWlmStatsResponse(null, new String[] { DEFAULT_QUERY_GROUP, id2 }, null);
        validateResponse(response3, new String[] { DEFAULT_QUERY_GROUP, id2 }, new String[] { id });

        WlmStatsResponse response4 = getWlmStatsResponse(null, new String[] { INVALID_ID }, null);
        validateResponse(response4, null, new String[] { DEFAULT_QUERY_GROUP, id, id2, INVALID_ID });

        updateQueryGroupInClusterState(DELETE, queryGroup);
        updateQueryGroupInClusterState(DELETE, queryGroup2);
    }

    public void testWlmStatsWithBreach() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.0000001))
        );
        String id = queryGroup.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(null, new String[] { _ALL }, true);
        validateResponse(response, null, new String[] { DEFAULT_QUERY_GROUP, id });

        WlmStatsResponse response2 = getWlmStatsResponse(null, new String[] { _ALL }, false);
        validateResponse(response2, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        WlmStatsResponse response3 = getWlmStatsResponse(null, new String[] { _ALL }, null);
        validateResponse(response3, new String[] { DEFAULT_QUERY_GROUP, id }, null);

        executeQueryGroupTask(MEMORY, id);
        Thread.sleep(1000);
        WlmStatsResponse response4 = getWlmStatsResponse(null, new String[] { _ALL }, true);
        validateResponse(response4, new String[] { id }, new String[] { DEFAULT_QUERY_GROUP });

        updateQueryGroupInClusterState(DELETE, queryGroup);
    }

    public void testWlmStatsWithNodesId() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String queryGroupId = queryGroup.get_id();
        String nodeId = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes().getLocalNodeId();
        updateQueryGroupInClusterState(PUT, queryGroup);
        WlmStatsResponse response = getWlmStatsResponse(new String[] { nodeId }, new String[] { _ALL }, true);
        validateResponse(response, new String[] { nodeId }, new String[] { DEFAULT_QUERY_GROUP, queryGroupId });

        WlmStatsResponse response2 = getWlmStatsResponse(new String[] { nodeId, INVALID_ID }, new String[] { _ALL }, false);
        validateResponse(response2, new String[] { nodeId, DEFAULT_QUERY_GROUP, queryGroupId }, new String[] { INVALID_ID });

        WlmStatsResponse response3 = getWlmStatsResponse(new String[] { INVALID_ID }, new String[] { _ALL }, false);
        validateResponse(response3, null, new String[] { nodeId, DEFAULT_QUERY_GROUP, queryGroupId });

        updateQueryGroupInClusterState(DELETE, queryGroup);
    }

    public void testWlmStatsWithIdAndBreach() throws Exception {
        QueryGroup queryGroup = new QueryGroup(
            NAME1,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5))
        );
        String queryGroupId = queryGroup.get_id();
        String nodeId = client().admin().cluster().prepareState().clear().setNodes(true).get().getState().nodes().getLocalNodeId();
        updateQueryGroupInClusterState(PUT, queryGroup);

        QueryGroup queryGroup2 = new QueryGroup(
            NAME2,
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.SOFT, Map.of(ResourceType.CPU, 0.000000001))
        );
        String queryGroupId2 = queryGroup2.get_id();
        updateQueryGroupInClusterState(PUT, queryGroup2);

        WlmStatsResponse response = getWlmStatsResponse(new String[] { nodeId }, new String[] { DEFAULT_QUERY_GROUP }, true);
        validateResponse(response, new String[] { nodeId }, new String[] { DEFAULT_QUERY_GROUP, queryGroupId });

        WlmStatsResponse response2 = getWlmStatsResponse(null, new String[] { DEFAULT_QUERY_GROUP, queryGroupId }, true);
        validateResponse(response2, null, new String[] { DEFAULT_QUERY_GROUP, queryGroupId });

        WlmStatsResponse response3 = getWlmStatsResponse(null, new String[] { DEFAULT_QUERY_GROUP, queryGroupId }, false);
        validateResponse(response3, new String[] { DEFAULT_QUERY_GROUP, queryGroupId }, null);

        WlmStatsResponse response4 = getWlmStatsResponse(null, new String[] { queryGroupId }, false);
        validateResponse(response4, new String[] { queryGroupId }, new String[] { DEFAULT_QUERY_GROUP });

        executeQueryGroupTask(CPU, queryGroupId2);
        Thread.sleep(1000);
        WlmStatsResponse response5 = getWlmStatsResponse(null, new String[] { queryGroupId, queryGroupId2 }, true);
        validateResponse(response5, new String[] { queryGroupId2 }, new String[] { DEFAULT_QUERY_GROUP, queryGroupId });

        updateQueryGroupInClusterState(DELETE, queryGroup);
        updateQueryGroupInClusterState(DELETE, queryGroup2);
    }

    public void updateQueryGroupInClusterState(String method, QueryGroup queryGroup) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(queryGroup, method), listener);
        assertTrue(listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertEquals(0, listener.getLatch().getCount());
    }

    public WlmStatsResponse getWlmStatsResponse(String[] nodesId, String[] queryGroupIds, Boolean breach) throws ExecutionException,
        InterruptedException {
        WlmStatsRequest wlmStatsRequest = new WlmStatsRequest(nodesId, new HashSet<>(Arrays.asList(queryGroupIds)), breach);
        return client().execute(WlmStatsAction.INSTANCE, wlmStatsRequest).get();
    }

    public void validateResponse(WlmStatsResponse response, String[] validIds, String[] invalidIds) {
        assertNotNull(response.toString());
        String res = response.toString();
        if (validIds != null) {
            for (String validId : validIds) {
                assertTrue(res.contains(validId));
            }
        }
        if (invalidIds != null) {
            for (String invalidId : invalidIds) {
                assertFalse(res.contains(invalidId));
            }
        }
    }

    public static class TestClusterUpdateRequest extends ClusterManagerNodeRequest<TestClusterUpdateRequest> {
        final private String method;
        final private QueryGroup queryGroup;

        public TestClusterUpdateRequest(QueryGroup queryGroup, String method) {
            this.method = method;
            this.queryGroup = queryGroup;
        }

        public TestClusterUpdateRequest(StreamInput in) throws IOException {
            super(in);
            this.method = in.readString();
            this.queryGroup = new QueryGroup(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(method);
            queryGroup.writeTo(out);
        }

        public QueryGroup getQueryGroup() {
            return queryGroup;
        }

        public String getMethod() {
            return method;
        }
    }

    public static class TestClusterUpdateTransportAction extends TransportClusterManagerNodeAction<TestClusterUpdateRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_cluster_update_action", TestResponse::new);

        @Inject
        public TestClusterUpdateTransportAction(
            ThreadPool threadPool,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            ClusterService clusterService
        ) {
            super(
                ACTION.name(),
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                TestClusterUpdateRequest::new,
                indexNameExpressionResolver
            );
        }

        @Override
        protected String executor() {
            return SAME;
        }

        @Override
        protected TestResponse read(StreamInput in) throws IOException {
            return new TestResponse(in);
        }

        @Override
        protected ClusterBlockException checkBlock(TestClusterUpdateRequest request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected void clusterManagerOperation(
            TestClusterUpdateRequest request,
            ClusterState clusterState,
            ActionListener<TestResponse> listener
        ) {
            clusterService.submitStateUpdateTask("query-group-persistence-service", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Map<String, QueryGroup> currentGroups = currentState.metadata().queryGroups();
                    QueryGroup queryGroup = request.getQueryGroup();
                    String id = queryGroup.get_id();
                    String method = request.getMethod();
                    Metadata metadata;
                    if (method.equals(PUT)) { // create
                        metadata = Metadata.builder(currentState.metadata()).put(queryGroup).build();
                    } else { // delete
                        metadata = Metadata.builder(currentState.metadata()).remove(currentGroups.get(id)).build();
                    }
                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new TestResponse());
                }
            });
        }
    }

    public static class TestQueryGroupTaskRequest<T extends Task> extends ActionRequest {
        private final String type;
        private final String queryGroupId;
        private TaskFactory<T> taskFactory;

        public TestQueryGroupTaskRequest(String type, String queryGroupId, TaskFactory<T> taskFactory) {
            this.type = type;
            this.queryGroupId = queryGroupId;
            this.taskFactory = taskFactory;
        }

        public TestQueryGroupTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.type = in.readString();
            this.queryGroupId = in.readString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return taskFactory.createTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(type);
            out.writeString(queryGroupId);
        }

        public String getType() {
            return type;
        }

        public String getQueryGroupId() {
            return queryGroupId;
        }
    }

    public static class TestQueryGroupTaskTransportAction extends HandledTransportAction<TestQueryGroupTaskRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>("internal::test_query_group_task_action", TestResponse::new);
        private final ThreadPool threadPool;

        @Inject
        public TestQueryGroupTaskTransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestQueryGroupTaskRequest::new);
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, TestQueryGroupTaskRequest request, ActionListener<TestResponse> listener) {
            threadPool.getThreadContext().putHeader(QUERY_GROUP_ID_HEADER, request.getQueryGroupId());
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                try {
                    CancellableTask cancellableTask = (CancellableTask) task;
                    ((QueryGroupTask) task).setQueryGroupId(threadPool.getThreadContext());
                    assertEquals(request.getQueryGroupId(), ((QueryGroupTask) task).getQueryGroupId());
                    long startTime = System.nanoTime();
                    while (System.nanoTime() - startTime < TIMEOUT.getNanos()) {
                        doWork(request);
                        if (cancellableTask.isCancelled()) {
                            break;
                        }
                    }
                    if (cancellableTask.isCancelled()) {
                        throw new TaskCancelledException(cancellableTask.getReasonCancelled());
                    } else {
                        listener.onResponse(new TestResponse());
                    }
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void doWork(TestQueryGroupTaskRequest request) throws InterruptedException {
            switch (request.getType()) {
                case CPU:
                    long i = 0, j = 1, k = 1, iterations = 1000;
                    do {
                        j += i;
                        k *= j;
                        i++;
                    } while (i < iterations);
                    break;
                case MEMORY:
                    int bytesToAllocate = (int) (Runtime.getRuntime().totalMemory() * 0.01);
                    Byte[] bytes = new Byte[bytesToAllocate];
                    int[] ints = new int[bytesToAllocate];
                    break;
            }
        }
    }

    public Exception executeQueryGroupTask(String resourceType, String queryGroupId) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(
            TestQueryGroupTaskTransportAction.ACTION,
            new TestQueryGroupTaskRequest(
                resourceType,
                queryGroupId,
                (TaskFactory<Task>) (id, type, action, description, parentTaskId, headers) -> new SearchTask(
                    id,
                    type,
                    action,
                    () -> description,
                    parentTaskId,
                    headers
                )
            ),
            listener
        );
        return listener.getException();
    }

    public static class TestClusterUpdatePlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(
                new ActionHandler<>(TestClusterUpdateTransportAction.ACTION, TestClusterUpdateTransportAction.class),
                new ActionHandler<>(TestQueryGroupTaskTransportAction.ACTION, TestQueryGroupTaskTransportAction.class)
            );
        }

        @Override
        public List<ActionType<? extends ActionResponse>> getClientActions() {
            return Arrays.asList(TestClusterUpdateTransportAction.ACTION, TestQueryGroupTaskTransportAction.ACTION);
        }
    }
}
