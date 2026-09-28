/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WorkloadGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadGroupTask;

import java.util.HashMap;
import java.util.Map;

public class WorkloadGroupTaskResourceTrackingTests extends OpenSearchTestCase {
    ThreadPool threadPool;
    WorkloadGroupResourceUsageTrackerService workloadGroupResourceUsageTrackerService;
    TaskResourceTrackingService taskResourceTrackingService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("workload-management-tracking-thread-pool");
        taskResourceTrackingService = new TaskResourceTrackingService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        workloadGroupResourceUsageTrackerService = new WorkloadGroupResourceUsageTrackerService(taskResourceTrackingService);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testValidWorkloadGroupTasksCase() {
        taskResourceTrackingService.setTaskResourceTrackingEnabled(true);
        WorkloadGroupTask task = new SearchTask(1, "test", "test", () -> "Test", TaskId.EMPTY_TASK_ID, new HashMap<>());
        taskResourceTrackingService.startTracking(task);

        // since the workload group id is not set we should not track this task
        Map<String, WorkloadGroupLevelResourceUsageView> resourceUsageViewMap = workloadGroupResourceUsageTrackerService
            .constructWorkloadGroupLevelUsageViews();
        assertTrue(resourceUsageViewMap.isEmpty());

        // Now since this task has a valid workloadGroupId header it should be tracked
        try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testHeader");
            task.setWorkloadGroupId(threadPool.getThreadContext());
            resourceUsageViewMap = workloadGroupResourceUsageTrackerService.constructWorkloadGroupLevelUsageViews();
            assertFalse(resourceUsageViewMap.isEmpty());
        }
    }
}
