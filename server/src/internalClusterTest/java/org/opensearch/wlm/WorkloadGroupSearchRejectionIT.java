/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.wlm.WorkloadManagementIT.TestClusterUpdatePlugin;
import org.opensearch.wlm.WorkloadManagementIT.TestClusterUpdateRequest;
import org.opensearch.wlm.WorkloadManagementIT.TestClusterUpdateTransportAction;
import org.opensearch.wlm.stats.WorkloadGroupState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Searches in a workload group over its resource limit are rejected by the coordinator before they run, and counted once
 * as a rejection and never as a completion. The group's usage is set directly rather than driven by load, and the
 * enforcement interval is long enough that the periodic run never overwrites it.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class WorkloadGroupSearchRejectionIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "docs";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestClusterUpdatePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(WorkloadManagementSettings.QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING.getKey(), TimeUnit.HOURS.toMillis(1))
            .build();
    }

    public void testSearchInAGroupOverItsLimitIsRejectedBeforeItRuns() throws Exception {
        WorkloadGroup group = createGroupAndIndex(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        setCpuUsage(group, 0.9); // above the normalised rejection threshold of 0.5 * 0.8 = 0.4

        // Rejected by the coordinator, rather than run and failed by the shard as a SearchPhaseExecutionException.
        expectThrows(OpenSearchRejectedExecutionException.class, () -> searchIn(group).actionGet());
        assertBusy(() -> {
            assertEquals(1, sum(group, WorkloadGroupState::getTotalRejections));
            assertEquals("a rejected search is not a completion", 0, sum(group, WorkloadGroupState::getTotalCompletions));
            assertEquals("a rejected search never ran, so it is not a failure", 0, sum(group, WorkloadGroupState::getFailures));
        });
    }

    public void testShardRejectionIsNotCountedAsACompletion() throws Exception {
        // The coordinator's node is under the limit and the data node is over it, so the shard rejects the search.
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        WorkloadGroup group = createGroupAndIndex(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        for (WorkloadGroupService service : internalCluster().getDataNodeInstances(WorkloadGroupService.class)) {
            groupState(service, group).getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.9);
        }

        Client client = client(coordinator).filterWithHeader(Map.of(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, group.get_id()));
        SearchPhaseExecutionException e = expectThrows(
            SearchPhaseExecutionException.class,
            () -> client.prepareSearch(INDEX).execute().actionGet()
        );
        assertNotNull(ExceptionsHelper.unwrap(e, OpenSearchRejectedExecutionException.class));
        assertBusy(() -> {
            assertEquals(1, sum(group, WorkloadGroupState::getTotalRejections));
            // Only the coordinator's task ran to completion; the rejected shard task is not counted.
            WorkloadGroupService coordinatorService = internalCluster().getInstance(WorkloadGroupService.class, coordinator);
            assertEquals(1, groupState(coordinatorService, group).getTotalCompletions());
            assertEquals(1, sum(group, WorkloadGroupState::getTotalCompletions));
        });
    }

    public void testMonitorGroupOverItsLimitIsNotRejected() throws Exception {
        WorkloadGroup group = createGroupAndIndex(MutableWorkloadGroupFragment.ResiliencyMode.MONITOR);
        setCpuUsage(group, 0.9);

        assertHitCount(searchIn(group).actionGet(), 1);
        assertEquals(0, sum(group, WorkloadGroupState::getTotalRejections));
        assertBusy(() -> assertTrue("the search counts as a completion", sum(group, WorkloadGroupState::getTotalCompletions) > 0));
    }

    public void testEachMultiSearchItemIsRejectedOnItsOwn() throws Exception {
        WorkloadGroup group = createGroupAndIndex(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
        setCpuUsage(group, 0.9);

        MultiSearchResponse response = client().filterWithHeader(Map.of(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, group.get_id()))
            .prepareMultiSearch()
            .add(client().prepareSearch(INDEX))
            .add(client().prepareSearch(INDEX))
            .get();
        for (MultiSearchResponse.Item item : response.getResponses()) {
            assertThat(item.getFailure(), instanceOf(OpenSearchRejectedExecutionException.class));
        }
        assertBusy(() -> {
            assertEquals(2, sum(group, WorkloadGroupState::getTotalRejections));
            assertEquals(0, sum(group, WorkloadGroupState::getTotalCompletions));
        });
    }

    // A control for the tests above: admission must not reject or fail a search in a group under its limit.
    public void testSearchInAGroupUnderItsLimitRuns() throws Exception {
        WorkloadGroup group = createGroupAndIndex(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);

        SearchResponse response = searchIn(group).actionGet();
        assertHitCount(response, 1);
        assertBusy(() -> assertTrue("the search counts as a completion", sum(group, WorkloadGroupState::getTotalCompletions) > 0));
        assertEquals(0, sum(group, WorkloadGroupState::getTotalRejections));
        assertEquals(0, sum(group, WorkloadGroupState::getFailures));
    }

    private WorkloadGroup createGroupAndIndex(MutableWorkloadGroupFragment.ResiliencyMode mode) throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(WorkloadManagementSettings.WLM_MODE_SETTING.getKey(), "enabled"))
        );
        WorkloadGroup group = new WorkloadGroup("group", new MutableWorkloadGroupFragment(mode, Map.of(ResourceType.CPU, 0.5)));
        client().execute(TestClusterUpdateTransportAction.ACTION, new TestClusterUpdateRequest(group, "PUT"))
            .actionGet(TimeValue.timeValueSeconds(10));
        // Every node must apply the group before its state exists; until then lookups fall back to the default group.
        assertBusy(() -> {
            for (WorkloadGroupService service : internalCluster().getInstances(WorkloadGroupService.class)) {
                assertNotNull(service.getWorkloadGroupsStateAccessor().getWorkloadGroupStateMap().get(group.get_id()));
            }
        });

        assertAcked(prepareCreate(INDEX).setSettings(Settings.builder().put("index.number_of_shards", 1)));
        client().prepareIndex(INDEX).setSource("n", 1).get();
        refresh(INDEX);
        ensureGreen(INDEX);
        return group;
    }

    private ActionFuture<SearchResponse> searchIn(WorkloadGroup group) {
        Client client = client().filterWithHeader(Map.of(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, group.get_id()));
        return client.prepareSearch(INDEX).execute();
    }

    private void setCpuUsage(WorkloadGroup group, double usage) {
        for (WorkloadGroupService service : internalCluster().getInstances(WorkloadGroupService.class)) {
            groupState(service, group).getResourceState().get(ResourceType.CPU).setLastRecordedUsage(usage);
        }
    }

    /** Sums a stat for the group across all nodes. */
    private long sum(WorkloadGroup group, ToLongFunction<WorkloadGroupState> stat) {
        long total = 0;
        for (WorkloadGroupService service : internalCluster().getInstances(WorkloadGroupService.class)) {
            total += stat.applyAsLong(groupState(service, group));
        }
        return total;
    }

    private static WorkloadGroupState groupState(WorkloadGroupService service, WorkloadGroup group) {
        return service.getWorkloadGroupsStateAccessor().getWorkloadGroupState(group.get_id());
    }
}
