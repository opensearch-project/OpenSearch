/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadGroupSearchCancelAfterTimeIntervalResolverTests extends OpenSearchTestCase {

    private static final String GROUP_ID = "group_id";
    private static final String CANCEL_AFTER = WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey();
    private static final String OVERRIDE = WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES.getKey();
    private static final TimeValue GROUP_INTERVAL = TimeValue.timeValueSeconds(1);
    private static final TimeValue LONGER_INTERVAL = TimeValue.timeValueHours(1);

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private WorkloadManagementSettings workloadManagementSettings;
    private WorkloadGroupSearchCancelAfterTimeIntervalResolver resolver;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        clusterService = mock(ClusterService.class);
        setWorkloadGroups();
        workloadManagementSettings = mock(WorkloadManagementSettings.class);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        WorkloadGroupService workloadGroupService = new WorkloadGroupService(
            mock(WorkloadGroupTaskCancellationService.class),
            clusterService,
            threadPool,
            workloadManagementSettings,
            null,
            new WorkloadGroupsStateAccessor(),
            new HashSet<>(),
            new HashSet<>()
        );
        resolver = new WorkloadGroupSearchCancelAfterTimeIntervalResolver(workloadGroupService);
    }

    public void testGroupIntervalAppliesWhenTheRequestSetNone() {
        setWorkloadGroups(group(withOverride(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL), randomFrom(true, false, null))));
        assertEquals(GROUP_INTERVAL, resolveTagged(null));
    }

    public void testRequestIntervalWinsByDefault() {
        setWorkloadGroups(group(withOverride(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL), randomFrom(false, null))));
        assertEquals(LONGER_INTERVAL, resolveTagged(LONGER_INTERVAL));
        assertEquals(SearchService.NO_TIMEOUT, resolveTagged(SearchService.NO_TIMEOUT));
    }

    public void testGroupIntervalWinsWithOverrideRequestValues() {
        setWorkloadGroups(group(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL).put(OVERRIDE, true).build()));
        assertEquals(GROUP_INTERVAL, resolveTagged(LONGER_INTERVAL));
        assertEquals(GROUP_INTERVAL, resolveTagged(SearchService.NO_TIMEOUT));
    }

    public void testGroupThatTurnsOffCancellation() {
        setWorkloadGroups(group(Settings.builder().put(CANCEL_AFTER, SearchService.NO_TIMEOUT).put(OVERRIDE, true).build()));
        // With override_request_values, the group turns cancellation off even for a request that asked for it.
        assertEquals(SearchService.NO_TIMEOUT, resolveTagged(LONGER_INTERVAL));
        assertEquals(SearchService.NO_TIMEOUT, resolveTagged(SearchService.NO_TIMEOUT));
        assertEquals(SearchService.NO_TIMEOUT, resolveTagged(null));
        for (Boolean overrideRequestValues : Arrays.asList(false, null)) {
            setWorkloadGroups(group(withOverride(Settings.builder().put(CANCEL_AFTER, SearchService.NO_TIMEOUT), overrideRequestValues)));
            // Without it, the request's own interval wins, and only a request that set none gets the group's -1.
            assertEquals("override [" + overrideRequestValues + "]", LONGER_INTERVAL, resolveTagged(LONGER_INTERVAL));
            assertEquals("override [" + overrideRequestValues + "]", SearchService.NO_TIMEOUT, resolveTagged(null));
        }
    }

    public void testZeroAndNegativeGroupIntervalsAreAppliedAsIs() {
        // Pins current behavior, pending a decision on rejecting these values: validation accepts them, and none of them equals
        // -1 (NO_TIMEOUT, which is -1ms), so the coordinator schedules them and the search is cancelled at once (see
        // TimeoutTaskCancellationUtilityTests). The request parameter and the cluster setting parse the same way.
        Map<String, TimeValue> accepted = Map.of(
            "0",
            TimeValue.ZERO,
            "0s",
            TimeValue.ZERO,
            "-1s",
            TimeValue.timeValueSeconds(-1),
            "-1m",
            TimeValue.timeValueMinutes(-1)
        );
        for (Map.Entry<String, TimeValue> interval : accepted.entrySet()) {
            Settings searchSettings = Settings.builder().put(CANCEL_AFTER, interval.getKey()).build();
            WorkloadGroupSearchSettings.validate(searchSettings);
            setWorkloadGroups(group(searchSettings));
            TimeValue resolved = resolveTagged(null);
            assertEquals(interval.getKey(), interval.getValue(), resolved);
            assertNotEquals(interval.getKey(), SearchService.NO_TIMEOUT, resolved);
        }
    }

    public void testGroupWithoutTheSettingKeepsTheRequestsInterval() {
        setWorkloadGroups(
            group(Settings.builder().put(WorkloadGroupSearchSettings.WLM_SEARCH_TIMEOUT.getKey(), "30s").put(OVERRIDE, true).build())
        );
        assertNull(resolveTagged(null));
        assertEquals(LONGER_INTERVAL, resolveTagged(LONGER_INTERVAL));
        assertEquals(SearchService.NO_TIMEOUT, resolveTagged(SearchService.NO_TIMEOUT));
    }

    public void testUntaggedSearchKeepsTheRequestsInterval() {
        setWorkloadGroups(group(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL).put(OVERRIDE, true).build()));
        SearchRequest request = request(LONGER_INTERVAL);
        assertEquals(LONGER_INTERVAL, resolver.resolve(task(request), request, LONGER_INTERVAL));
        request = request(null);
        assertNull(resolver.resolve(task(request), request, null));
    }

    public void testSearchTaggedToAGroupNotInTheClusterStateKeepsTheRequestsInterval() {
        setWorkloadGroups(group(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL).put(OVERRIDE, true).build()));
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "unknown_group_id");
        SearchRequest request = request(LONGER_INTERVAL);
        assertEquals(LONGER_INTERVAL, resolver.resolve(task(request), request, LONGER_INTERVAL));
    }

    public void testGroupWithoutSearchSettingsKeepsTheRequestsInterval() throws Exception {
        // A WorkloadGroup replaces absent search settings with empty ones, so only a mock has none.
        WorkloadGroup workloadGroup = mock(WorkloadGroup.class);
        when(workloadGroup.getSettings()).thenReturn(null);
        WorkloadGroupService workloadGroupService = mock(WorkloadGroupService.class);
        when(workloadGroupService.getCurrentWorkloadGroup()).thenReturn(workloadGroup);
        resolver = new WorkloadGroupSearchCancelAfterTimeIntervalResolver(workloadGroupService);
        assertNull(resolveTagged(null));
        assertEquals(LONGER_INTERVAL, resolveTagged(LONGER_INTERVAL));
        assertEquals(SearchService.NO_TIMEOUT, resolveTagged(SearchService.NO_TIMEOUT));
    }

    public void testFailedGroupLookupIsLeftToTheSearchAction() {
        // TransportSearchAction logs a resolver's failure and keeps the request's interval.
        WorkloadGroupService workloadGroupService = mock(WorkloadGroupService.class);
        when(workloadGroupService.getCurrentWorkloadGroup()).thenThrow(new IllegalStateException("cluster state unavailable"));
        resolver = new WorkloadGroupSearchCancelAfterTimeIntervalResolver(workloadGroupService);
        expectThrows(IllegalStateException.class, () -> resolveTagged(LONGER_INTERVAL));
    }

    public void testMalformedGroupIntervalIsLeftToTheSearchAction() {
        // Search settings are validated when a group is created, so only a group built without validation gets here.
        setWorkloadGroups(unvalidatedGroup(Settings.builder().put(CANCEL_AFTER, "not-a-time-value").put(OVERRIDE, true).build()));
        expectThrows(IllegalArgumentException.class, () -> resolveTagged(LONGER_INTERVAL));
    }

    public void testGroupWithoutTheSettingDoesNotReadOverrideRequestValues() {
        // A malformed override_request_values would fail to resolve every search had it been read for a group without an interval.
        setWorkloadGroups(unvalidatedGroup(Settings.builder().put(OVERRIDE, "not-a-boolean").build()));
        assertNull(resolveTagged(null));
        assertEquals(LONGER_INTERVAL, resolveTagged(LONGER_INTERVAL));
    }

    public void testAppliesTheGroupIntervalInEveryWlmMode() {
        // As WorkloadGroupRequestOperationListener applies the group's other search settings onto the request in every mode.
        setWorkloadGroups(group(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL).build()));
        for (WlmMode mode : WlmMode.values()) {
            when(workloadManagementSettings.getWlmMode()).thenReturn(mode);
            assertEquals("mode [" + mode + "]", GROUP_INTERVAL, resolveTagged(null));
        }
    }

    public void testTaskOutsideWorkloadManagementGetsTheGroupIntervalOfItsThreadContext() {
        // The group's interval applies whatever the task, as the group's other search settings do.
        setWorkloadGroups(group(Settings.builder().put(CANCEL_AFTER, GROUP_INTERVAL).build()));
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, GROUP_ID);
        CancellableTask task = new CancellableTask(
            1,
            "transport",
            SearchAction.NAME,
            "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        ) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return false;
            }
        };
        assertEquals(GROUP_INTERVAL, resolver.resolve(task, request(null), null));
    }

    /** Resolves a search tagged to {@link #GROUP_ID} whose request asked for {@code requested}. */
    private TimeValue resolveTagged(TimeValue requested) {
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, GROUP_ID);
            SearchRequest request = request(requested);
            return resolver.resolve(task(request), request, requested);
        }
    }

    private static SearchRequest request(TimeValue cancelAfterTimeInterval) {
        SearchRequest request = new SearchRequest("index");
        request.setCancelAfterTimeInterval(cancelAfterTimeInterval);
        return request;
    }

    private static SearchTask task(SearchRequest request) {
        return request.createTask(1, "transport", SearchAction.NAME, TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    private static Settings withOverride(Settings.Builder settings, Boolean overrideRequestValues) {
        if (overrideRequestValues != null) {
            settings.put(OVERRIDE, overrideRequestValues);
        }
        return settings.build();
    }

    private static WorkloadGroup group(Settings searchSettings) {
        return new WorkloadGroup(
            "name",
            GROUP_ID,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.MEMORY, 0.5),
                searchSettings
            ),
            System.currentTimeMillis()
        );
    }

    /** A group whose search settings skipped validation, as only a group from outside the create and update APIs can. */
    private static WorkloadGroup unvalidatedGroup(Settings searchSettings) {
        return new WorkloadGroup(
            "name",
            GROUP_ID,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.MEMORY, 0.5),
                searchSettings,
                Settings.EMPTY,
                false
            ),
            System.currentTimeMillis()
        );
    }

    private void setWorkloadGroups(WorkloadGroup... workloadGroups) {
        Metadata.Builder metadata = Metadata.builder();
        for (WorkloadGroup workloadGroup : workloadGroups) {
            metadata.put(workloadGroup);
        }
        when(clusterService.state()).thenReturn(ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build());
    }
}
