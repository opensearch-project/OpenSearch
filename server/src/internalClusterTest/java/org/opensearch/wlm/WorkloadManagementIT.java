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
import org.opensearch.cluster.metadata.WorkloadGroup;
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
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.threadpool.ThreadPool.Names.SAME;
import static org.opensearch.wlm.WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER;
import static org.hamcrest.Matchers.instanceOf;

public class WorkloadManagementIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    final static String PUT = "PUT";
    final static String MEMORY = "MEMORY";
    final static String CPU = "CPU";
    final static String ENABLED = "enabled";
    final static String DELETE = "DELETE";
    private static final TimeValue TIMEOUT = new TimeValue(1, TimeUnit.SECONDS);

    public WorkloadManagementIT(Settings nodeSettings) {
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

    @Before
    public final void setupNodeSettings() {
        Settings request = Settings.builder()
            .put(WorkloadManagementSettings.NODE_LEVEL_MEMORY_REJECTION_THRESHOLD.getKey(), 0.8)
            .put(WorkloadManagementSettings.NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD.getKey(), 0.9)
            .put(WorkloadManagementSettings.NODE_LEVEL_CPU_REJECTION_THRESHOLD.getKey(), 0.8)
            .put(WorkloadManagementSettings.NODE_LEVEL_CPU_CANCELLATION_THRESHOLD.getKey(), 0.9)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
    }

    @After
    public final void cleanupNodeSettings() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testHighCPUInEnforcedMode() throws InterruptedException {
        Settings request = Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), ENABLED).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "name",
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.01, ResourceType.MEMORY, 0.01)
            )
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        Exception caughtException = executeWorkloadGroupTask(CPU, workloadGroup.get_id());
        assertNotNull("SearchTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        updateWorkloadGroupInClusterState(DELETE, workloadGroup);
    }

    public void testHighCPUInMonitorMode() throws InterruptedException {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "name",
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.01, ResourceType.MEMORY, 0.01)
            )
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        Exception caughtException = executeWorkloadGroupTask(CPU, workloadGroup.get_id());
        assertNull(caughtException);
        updateWorkloadGroupInClusterState(DELETE, workloadGroup);
    }

    public void testHighMemoryInEnforcedMode() throws InterruptedException {
        Settings request = Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), ENABLED).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "name",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.01))
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        Exception caughtException = executeWorkloadGroupTask(MEMORY, workloadGroup.get_id());
        assertNotNull("SearchTask should have been cancelled with TaskCancelledException", caughtException);
        MatcherAssert.assertThat(caughtException, instanceOf(TaskCancelledException.class));
        updateWorkloadGroupInClusterState(DELETE, workloadGroup);
    }

    public void testHighMemoryInMonitorMode() throws InterruptedException {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "name",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.01))
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        Exception caughtException = executeWorkloadGroupTask(MEMORY, workloadGroup.get_id());
        assertNull("SearchTask should have been cancelled with TaskCancelledException", caughtException);
        updateWorkloadGroupInClusterState(DELETE, workloadGroup);
    }

    public void testNoCancellation() throws InterruptedException {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "name",
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.8, ResourceType.MEMORY, 0.8)
            )
        );
        updateWorkloadGroupInClusterState(PUT, workloadGroup);
        Exception caughtException = executeWorkloadGroupTask(CPU, workloadGroup.get_id());
        assertNull(caughtException);
        updateWorkloadGroupInClusterState(DELETE, workloadGroup);
    }

    public Exception executeWorkloadGroupTask(String resourceType, String workloadGroupId) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(
            TestWorkloadGroupTaskTransportAction.ACTION,
            new TestWorkloadGroupTaskRequest(
                resourceType,
                workloadGroupId,
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
        assertTrue(listener.getLatch().await(TIMEOUT.getSeconds() + 1, TimeUnit.SECONDS));
        return listener.getException();
    }

    public void updateWorkloadGroupInClusterState(String method, WorkloadGroup workloadGroup) throws InterruptedException {
        ExceptionCatchingListener listener = new ExceptionCatchingListener();
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(workloadGroup, method), listener);
        assertTrue(listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        assertEquals(0, listener.getLatch().getCount());
    }

    public static class TestClusterUpdateRequest extends ClusterManagerNodeRequest<TestClusterUpdateRequest> {
        final private String method;
        final private WorkloadGroup workloadGroup;

        public TestClusterUpdateRequest(WorkloadGroup workloadGroup, String method) {
            this.method = method;
            this.workloadGroup = workloadGroup;
        }

        public TestClusterUpdateRequest(StreamInput in) throws IOException {
            super(in);
            this.method = in.readString();
            this.workloadGroup = new WorkloadGroup(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(method);
            workloadGroup.writeTo(out);
        }

        public WorkloadGroup getWorkloadGroup() {
            return workloadGroup;
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
                    Map<String, WorkloadGroup> currentGroups = currentState.metadata().workloadGroups();
                    WorkloadGroup workloadGroup = request.getWorkloadGroup();
                    String id = workloadGroup.get_id();
                    String method = request.getMethod();
                    Metadata metadata;
                    if (method.equals(PUT)) { // create
                        metadata = Metadata.builder(currentState.metadata()).put(workloadGroup).build();
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

    public static class TestWorkloadGroupTaskRequest<T extends Task> extends ActionRequest {
        private final String type;
        private final String workloadGroupId;
        private TaskFactory<T> taskFactory;

        public TestWorkloadGroupTaskRequest(String type, String workloadGroupId, TaskFactory<T> taskFactory) {
            this.type = type;
            this.workloadGroupId = workloadGroupId;
            this.taskFactory = taskFactory;
        }

        public TestWorkloadGroupTaskRequest(StreamInput in) throws IOException {
            super(in);
            this.type = in.readString();
            this.workloadGroupId = in.readString();
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
            out.writeString(workloadGroupId);
        }

        public String getType() {
            return type;
        }

        public String getWorkloadGroupId() {
            return workloadGroupId;
        }
    }

    public static class TestWorkloadGroupTaskTransportAction extends HandledTransportAction<TestWorkloadGroupTaskRequest, TestResponse> {
        public static final ActionType<TestResponse> ACTION = new ActionType<>(
            "internal::test_workload_group_task_action",
            TestResponse::new
        );
        private final ThreadPool threadPool;

        @Inject
        public TestWorkloadGroupTaskTransportAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters) {
            super(ACTION.name(), transportService, actionFilters, TestWorkloadGroupTaskRequest::new);
            this.threadPool = threadPool;
        }

        @Override
        protected void doExecute(Task task, TestWorkloadGroupTaskRequest request, ActionListener<TestResponse> listener) {
            threadPool.getThreadContext().putHeader(WORKLOAD_GROUP_ID_HEADER, request.getWorkloadGroupId());
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                try {
                    CancellableTask cancellableTask = (CancellableTask) task;
                    ((WorkloadGroupTask) task).setWorkloadGroupId(threadPool.getThreadContext());
                    assertEquals(request.getWorkloadGroupId(), ((WorkloadGroupTask) task).getWorkloadGroupId());
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

        private void doWork(TestWorkloadGroupTaskRequest request) throws InterruptedException {
            switch (request.getType()) {
                case "CPU":
                    long i = 0, j = 1, k = 1, iterations = 1000;
                    do {
                        j += i;
                        k *= j;
                        i++;
                    } while (i < iterations);
                    break;
                case "MEMORY":
                    int bytesToAllocate = (int) (Runtime.getRuntime().totalMemory() * 0.01);
                    Byte[] bytes = new Byte[bytesToAllocate];
                    int[] ints = new int[bytesToAllocate];
                    break;
            }
        }
    }

    public static class TestClusterUpdatePlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return Arrays.asList(
                new ActionHandler<>(TestClusterUpdateTransportAction.ACTION, TestClusterUpdateTransportAction.class),
                new ActionHandler<>(TestWorkloadGroupTaskTransportAction.ACTION, TestWorkloadGroupTaskTransportAction.class)
            );
        }

        @Override
        public List<ActionType<? extends ActionResponse>> getClientActions() {
            return Arrays.asList(TestClusterUpdateTransportAction.ACTION, TestWorkloadGroupTaskTransportAction.ACTION);
        }
    }
}
