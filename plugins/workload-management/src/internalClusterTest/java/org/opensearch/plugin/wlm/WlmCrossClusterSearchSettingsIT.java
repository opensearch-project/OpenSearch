/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTask;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.plugin.wlm.WlmNodeThrottlingIT.ScriptedBlockPlugin;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.AbstractMultiClustersTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupSearchSettings;
import org.opensearch.wlm.WorkloadGroupTask;
import org.joda.time.Instant;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * End-to-end tests of a workload group's {@code search.cancel_after_time_interval} on cross-cluster searches. Only the local
 * cluster has the group, and a search reaches it through the workloadGroupId header, which travels with its remote legs too.
 */
public class WlmCrossClusterSearchSettingsIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final String REMOTE_INDEX = "remote_index";
    private static final String WORKLOAD_GROUP_ID = "wlm_ccs_cancel_after_group";
    private static final TimeValue GROUP_CANCEL_AFTER = TimeValue.timeValueSeconds(1);
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(ScriptedBlockPlugin.class);
        if (LOCAL_CLUSTER.equals(clusterAlias)) {
            plugins.add(WorkloadGroupUpdatePlugin.class);
        }
        return plugins;
    }

    @Before
    public void createRemoteIndexAndLocalWorkloadGroup() throws Exception {
        assertAcked(
            client(REMOTE_CLUSTER).admin()
                .indices()
                .prepareCreate(REMOTE_INDEX)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        client(REMOTE_CLUSTER).prepareIndex(REMOTE_INDEX)
            .setId("1")
            .setSource(Map.of("field", "value"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        WorkloadGroup workloadGroup = new WorkloadGroup(
            "ccs_cancel_after_group",
            WORKLOAD_GROUP_ID,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.CPU, 0.4, ResourceType.MEMORY, 0.4),
                Settings.builder().put(WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey(), GROUP_CANCEL_AFTER).build()
            ),
            Instant.now().getMillis()
        );
        WlmAutoTaggingIT.ExceptionCatchingListener listener = new WlmAutoTaggingIT.ExceptionCatchingListener();
        client().execute(
            WlmAutoTaggingIT.TestClusterUpdateTransportAction.ACTION,
            new WlmAutoTaggingIT.TestClusterUpdateRequest(workloadGroup, "PUT"),
            listener
        );
        assertTrue("cluster-state update did not complete in time", listener.getLatch().await(TIMEOUT.getSeconds(), TimeUnit.SECONDS));
        if (listener.getException() != null) {
            throw new AssertionError("cluster-state update failed", listener.getException());
        }
        assertBusy(() -> {
            for (ClusterService clusterService : cluster(LOCAL_CLUSTER).getInstances(ClusterService.class)) {
                assertTrue(clusterService.state().metadata().workloadGroups().containsKey(WORKLOAD_GROUP_ID));
            }
        });
    }

    public void testMinimizeRoundtripsCancelsTheCoordinatorAndEachRemoteLegAtTheGroupInterval() throws Exception {
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        SearchRequest request = blockingRemoteSearch();
        request.setCcsMinimizeRoundtrips(true);
        ActionFuture<SearchResponse> response = groupClient().search(request);
        try {
            SearchShardTask remoteShardTask = awaitBlockedShardTask(REMOTE_CLUSTER, REMOTE_INDEX);
            SearchTask remoteLeg = (SearchTask) task(REMOTE_CLUSTER, remoteShardTask.getParentTaskId());
            assertFalse("a remote leg must not be a child of the coordinator's task", remoteLeg.getParentTaskId().isSet());
            // The leg copies the interval that the local listener wrote onto the request, so the remote cluster, which has no such
            // group, creates its task with that interval and arms its own timer with it.
            assertEquals(GROUP_CANCEL_AFTER, remoteLeg.getCancellationTimeout());
            SearchTask coordinator = awaitLocalSearchTask().task();
            assertBusy(() -> {
                assertTrue("the coordinator's timer must cancel its task at the group's interval", coordinator.isCancelled());
                assertTrue("the remote leg's own timer must cancel it at the group's interval", remoteLeg.isCancelled());
                assertTrue("the remote cancellation has not reached the shard yet", remoteShardTask.isCancelled());
            }, 10, TimeUnit.SECONDS);
        } finally {
            disableBlocks(plugins);
        }
        assertFailedAsCancelled(expectThrows(Exception.class, () -> response.actionGet(TIMEOUT)));
    }

    public void testWithoutMinimizeRoundtripsTheCoordinatorTimerCancelsTheSearch() throws Exception {
        List<ScriptedBlockPlugin> plugins = enableBlocks();
        SearchRequest request = blockingRemoteSearch();
        request.setCcsMinimizeRoundtrips(false);
        ActionFuture<SearchResponse> response = groupClient().search(request);
        try {
            SearchShardTask remoteShardTask = awaitBlockedShardTask(REMOTE_CLUSTER, REMOTE_INDEX);
            LocalSearch coordinator = awaitLocalSearchTask();
            // The remote shard runs a child request of the coordinator's task, and a shard request doesn't carry the interval, so the
            // coordinator's timer is the only one. Its cancellation can't ban children on a remote cluster's nodes, so the remote shard
            // runs on until it is released, and the search fails once the cancelled coordinator consumes the shard's result.
            assertEquals(new TaskId(coordinator.nodeId(), coordinator.task().getId()), remoteShardTask.getParentTaskId());
            assertBusy(
                () -> assertTrue("the coordinator's timer must cancel its task at the group's interval", coordinator.task().isCancelled()),
                10,
                TimeUnit.SECONDS
            );
        } finally {
            disableBlocks(plugins);
        }
        assertFailedAsCancelled(expectThrows(Exception.class, () -> response.actionGet(TIMEOUT)));
    }

    // Helpers

    private static void assertFailedAsCancelled(Exception e) {
        assertNotNull("expected the search to fail as cancelled but got: " + e, ExceptionsHelper.unwrap(e, TaskCancelledException.class));
    }

    private Client groupClient() {
        return client().filterWithHeader(Map.of(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, WORKLOAD_GROUP_ID));
    }

    /** A search on the remote index with a query that blocks on its shard. */
    private static SearchRequest blockingRemoteSearch() {
        QueryBuilder blockingQuery = scriptQuery(
            new Script(ScriptType.INLINE, "mockscript", ScriptedBlockPlugin.SCRIPT_NAME, Collections.emptyMap())
        );
        return new SearchRequest(REMOTE_CLUSTER + ":" + REMOTE_INDEX).source(new SearchSourceBuilder().query(blockingQuery));
    }

    /** The coordinator's search task and the id of the local node that runs it. */
    private record LocalSearch(String nodeId, SearchTask task) {
    }

    /** The local cluster's task for the search on the remote index. */
    private LocalSearch awaitLocalSearchTask() throws Exception {
        AtomicReference<LocalSearch> found = new AtomicReference<>();
        assertBusy(() -> {
            for (TransportService transportService : cluster(LOCAL_CLUSTER).getInstances(TransportService.class)) {
                for (Task task : transportService.getTaskManager().getTasks().values()) {
                    if (task instanceof SearchTask searchTask
                        && SearchAction.NAME.equals(task.getAction())
                        && task.getDescription().contains(REMOTE_CLUSTER + ":" + REMOTE_INDEX)) {
                        found.set(new LocalSearch(transportService.getLocalNode().getId(), searchTask));
                    }
                }
            }
            assertNotNull("the coordinator's search task is not registered yet", found.get());
        });
        return found.get();
    }

    /** The shard task of the search on {@code indexName} in {@code clusterAlias}, once it has reached the shard, where it blocks. */
    private SearchShardTask awaitBlockedShardTask(String clusterAlias, String indexName) throws Exception {
        AtomicReference<SearchShardTask> found = new AtomicReference<>();
        assertBusy(() -> {
            for (TransportService transportService : cluster(clusterAlias).getInstances(TransportService.class)) {
                for (Task task : transportService.getTaskManager().getTasks().values()) {
                    if (task instanceof SearchShardTask shardTask && task.getDescription().contains("[" + indexName + "]")) {
                        found.set(shardTask);
                    }
                }
            }
            assertNotNull("the search has not reached its shard in [" + clusterAlias + "] yet", found.get());
        });
        return found.get();
    }

    /** The task {@code taskId} of a node in {@code clusterAlias}. */
    private Task task(String clusterAlias, TaskId taskId) {
        for (TransportService transportService : cluster(clusterAlias).getInstances(TransportService.class)) {
            if (transportService.getLocalNode().getId().equals(taskId.getNodeId())) {
                Task task = transportService.getTaskManager().getTask(taskId.getId());
                assertNotNull("task [" + taskId + "] is not running", task);
                return task;
            }
        }
        throw new AssertionError("no node of cluster [" + clusterAlias + "] has the id [" + taskId.getNodeId() + "]");
    }

    private List<ScriptedBlockPlugin> enableBlocks() {
        List<ScriptedBlockPlugin> plugins = new ArrayList<>();
        for (InternalTestCluster cluster : clusters().values()) {
            for (PluginsService pluginsService : cluster.getDataNodeInstances(PluginsService.class)) {
                plugins.addAll(pluginsService.filterPlugins(ScriptedBlockPlugin.class));
            }
        }
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.enableBlock();
        }
        return plugins;
    }

    private void disableBlocks(List<ScriptedBlockPlugin> plugins) {
        for (ScriptedBlockPlugin plugin : plugins) {
            plugin.disableBlock();
        }
    }

    /** Registers the test action that writes a workload group straight into the cluster state. */
    public static class WorkloadGroupUpdatePlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(
                new ActionHandler<>(
                    WlmAutoTaggingIT.TestClusterUpdateTransportAction.ACTION,
                    WlmAutoTaggingIT.TestClusterUpdateTransportAction.class
                )
            );
        }
    }
}
