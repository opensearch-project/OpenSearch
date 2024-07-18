/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup.cancellation;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.search.querygroup.QueryGroupLevelResourceUsageView;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.MockitoAnnotations;

public class DefaultTaskCancellationTests extends OpenSearchTestCase {

    private static class TestTaskCancellationImpl extends DefaultTaskCancellation {

        public TestTaskCancellationImpl(
            TaskSelectionStrategy taskSelectionStrategy,
            Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews,
            Set<QueryGroup> activeQueryGroups
        ) {
            super(taskSelectionStrategy, queryGroupLevelViews, activeQueryGroups);
        }

        public List<QueryGroup> getQueryGroupsToCancelFrom() {
            return new ArrayList<>(activeQueryGroups);
        }
    }

    private TaskSelectionStrategy taskSelectionStrategy;
    private Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews;
    private Set<QueryGroup> activeQueryGroups;
    private DefaultTaskCancellation taskCancellation;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        queryGroupLevelViews = new HashMap<>();
        activeQueryGroups = new HashSet<>();
        taskCancellation = new TestTaskCancellationImpl(
            new TaskSelectionStrategyTests.TestTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups
        );
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThreshold() {
        String id = "queryGroup1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        long threshold = 10L;
        QueryGroup queryGroup1 = QueryGroupTestHelpers.createQueryGroupMock(id, resourceTypeStr, threshold, usage);
        // QueryGroup.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        // when(queryGroup1.getThresholdInLong(ResourceType.fromName(resourceTypeStr))).thenReturn(resourceLimitMock);
        QueryGroupLevelResourceUsageView mockView = QueryGroupTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        queryGroupLevelViews.put(id, mockView);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup1);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    // public void testGetCancellableTasksFrom_returnsNoTasksWhenBreachingThreshold() {
    // String id = "querygroup1";
    // String resourceTypeStr = "CPU";
    // long usage = 50L;
    // long threshold = 100L;
    // QueryGroup querygroup1 = QueryGroupTestHelpers.createQueryGroupMock(id, resourceTypeStr, threshold, usage);
    // QueryGroup.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
    // when(querygroup1.getResourceLimitFor(SystemResource.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
    // QueryGroupLevelResourceUsageView mockView = QueryGroupTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
    // queryGroupLevelViews.put(id, mockView);
    // activeQueryGroups.add(querygroup1);
    //
    // List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(querygroup1);
    // assertTrue(cancellableTasksFrom.isEmpty());
    // }
    //
    // public void testCancelTasks_cancelsGivenTasks() {
    // String id = "querygroup1";
    // String resourceTypeStr = "CPU";
    // long usage = 50L;
    // long threshold = 10L;
    // QueryGroup querygroup1 = QueryGroupTestHelpers.createQueryGroupMock(id, resourceTypeStr, threshold, usage);
    // QueryGroup.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
    // when(querygroup1.getResourceLimitFor(SystemResource.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
    // QueryGroupLevelResourceUsageView mockView = QueryGroupTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
    // queryGroupLevelViews.put(id, mockView);
    // activeQueryGroups.add(querygroup1);
    //
    // TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
    // new TaskSelectionStrategyTests.TestTaskSelectionStrategy(),
    // queryGroupLevelViews,
    // activeQueryGroups
    // );
    //
    // List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks();
    // assertEquals(2, cancellableTasksFrom.size());
    // assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
    // assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    //
    // taskCancellation.cancelTasks();
    // assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
    // assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
    // }
    //
    // public void testGetAllCancellableTasks_ReturnsNoTasksWhenNotBreachingThresholds() {
    // String id = "querygroup1";
    // String resourceTypeStr = "CPU";
    // long usage = 50L;
    // long threshold = 100L;
    // QueryGroup querygroup1 = QueryGroupTestHelpers.createQueryGroupMock(id, resourceTypeStr, threshold, usage);
    // QueryGroup.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
    // when(querygroup1.getResourceLimitFor(SystemResource.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
    // QueryGroupLevelResourceUsageView mockView = QueryGroupTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
    // queryGroupLevelViews.put(id, mockView);
    // activeQueryGroups.add(querygroup1);
    //
    // List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks();
    // assertTrue(allCancellableTasks.isEmpty());
    // }
    //
    // public void testGetAllCancellableTasks_ReturnsTasksWhenBreachingThresholds() {
    // String id = "querygroup1";
    // String resourceTypeStr = "CPU";
    // long usage = 100L;
    // long threshold = 50L;
    // QueryGroup querygroup1 = QueryGroupTestHelpers.createQueryGroupMock(id, resourceTypeStr, threshold, usage);
    // QueryGroup.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
    // when(querygroup1.getResourceLimitFor(SystemResource.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
    // QueryGroupLevelResourceUsageView mockView = QueryGroupTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
    // queryGroupLevelViews.put(id, mockView);
    // activeQueryGroups.add(querygroup1);
    //
    // List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks();
    // assertEquals(2, allCancellableTasks.size());
    // assertEquals(1234, allCancellableTasks.get(0).getTask().getId());
    // assertEquals(4321, allCancellableTasks.get(1).getTask().getId());
    // }
    //
    // public void testGetCancellableTasksFrom_returnsTasksEvenWhenquerygroupIdNotFound() {
    // String querygroup_id1 = "querygroup1";
    // String querygroup_id2 = "querygroup2";
    // String resourceTypeStr = "CPU";
    // long usage = 50L;
    // long threshold = 10L;
    // QueryGroup querygroup1 = QueryGroupTestHelpers.createQueryGroupMock(querygroup_id1, resourceTypeStr, threshold, usage);
    // QueryGroup querygroup2 = QueryGroupTestHelpers.createQueryGroupMock(querygroup_id2, resourceTypeStr, threshold, usage);
    // QueryGroup.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
    // when(querygroup1.getResourceLimitFor(SystemResource.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
    // QueryGroupLevelResourceUsageView mockView = QueryGroupTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
    // queryGroupLevelViews.put(querygroup_id1, mockView);
    // activeQueryGroups.add(querygroup1);
    // activeQueryGroups.add(querygroup2);
    //
    // List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(querygroup1);
    // assertEquals(2, cancellableTasksFrom.size());
    // assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
    // assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    // }
}
