/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.cancellation;

import org.opensearch.cluster.metadata.Sandbox;
import org.opensearch.search.sandboxing.SandboxLevelResourceUsageView;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
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

import static org.opensearch.search.sandboxing.cancellation.SandboxTestHelpers.createResourceLimitMock;
import static org.mockito.Mockito.when;

public class AbstractTaskCancellationTests extends OpenSearchTestCase {

    private class TestTaskCancellationImpl extends AbstractTaskCancellation {

        public TestTaskCancellationImpl(
            TaskSelectionStrategy taskSelectionStrategy,
            Map<String, SandboxLevelResourceUsageView> sandboxLevelViews,
            Set<Sandbox> activeSandboxes
        ) {
            super(taskSelectionStrategy, sandboxLevelViews, activeSandboxes);
        }

        @Override
        List<Sandbox> getSandboxesToCancelFrom() {
            return new ArrayList<>(activeSandboxes);
        }
    }

    private TaskSelectionStrategy taskSelectionStrategy;
    private Map<String, SandboxLevelResourceUsageView> sandboxLevelViews;
    private Set<Sandbox> activeSandboxes;
    private AbstractTaskCancellation taskCancellation;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        sandboxLevelViews = new HashMap<>();
        activeSandboxes = new HashSet<>();
        taskCancellation = new TestTaskCancellationImpl(
            new TaskSelectionStrategyTests.TestTaskSelectionStrategy(),
            sandboxLevelViews,
            activeSandboxes
        );
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThreshold() {
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        long threshold = 10L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, threshold, usage);
        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox1.getResourceLimitFor(SandboxResourceType.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(sandbox1);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(4321, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(1234, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsNoTasksWhenBreachingThreshold() {
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        long threshold = 100L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, threshold, usage);
        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox1.getResourceLimitFor(SandboxResourceType.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);
        activeSandboxes.add(sandbox1);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(sandbox1);
        assertTrue(cancellableTasksFrom.isEmpty());
    }

    public void testCancelTasks_cancelsGivenTasks() {
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        long threshold = 10L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, threshold, usage);
        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox1.getResourceLimitFor(SandboxResourceType.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);
        activeSandboxes.add(sandbox1);

        TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
            new TaskSelectionStrategyTests.TestTaskSelectionStrategy(),
            sandboxLevelViews,
            activeSandboxes
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks();
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(4321, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(1234, cancellableTasksFrom.get(1).getTask().getId());

        taskCancellation.cancelTasks();
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
    }

    public void testGetAllCancellableTasks_ReturnsNoTasksWhenNotBreachingThresholds() {
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        long threshold = 100L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, threshold, usage);
        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox1.getResourceLimitFor(SandboxResourceType.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);
        activeSandboxes.add(sandbox1);

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks();
        assertTrue(allCancellableTasks.isEmpty());
    }

    public void testGetAllCancellableTasks_ReturnsTasksWhenBreachingThresholds() {
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 100L;
        long threshold = 50L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, threshold, usage);
        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox1.getResourceLimitFor(SandboxResourceType.fromString(resourceTypeStr))).thenReturn(resourceLimitMock);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);
        activeSandboxes.add(sandbox1);

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks();
        assertEquals(2, allCancellableTasks.size());
        assertEquals(4321, allCancellableTasks.get(0).getTask().getId());
        assertEquals(1234, allCancellableTasks.get(1).getTask().getId());
    }
}
