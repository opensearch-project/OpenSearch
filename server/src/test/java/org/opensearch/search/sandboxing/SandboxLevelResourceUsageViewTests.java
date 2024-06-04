/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing;

import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.search.sandboxing.cancellation.SandboxCancellationStrategyTestHelpers.getRandomTask;

public class SandboxLevelResourceUsageViewTests extends OpenSearchTestCase {
    Map<SandboxResourceType, Long> resourceUsage;
    List<Task> activeTasks;

    public void setUp() throws Exception {
        super.setUp();
        resourceUsage = Map.of(SandboxResourceType.fromString("JVM"), 34L, SandboxResourceType.fromString("CPU"), 12L);
        activeTasks = List.of(getRandomTask(4321));
    }

    public void testGetResourceUsageData() {
        SandboxLevelResourceUsageView sandboxLevelResourceUsageView = new SandboxLevelResourceUsageView("1234", resourceUsage, activeTasks);
        Map<SandboxResourceType, Long> resourceUsageData = sandboxLevelResourceUsageView.getResourceUsageData();
        assertTrue(assertResourceUsageData(resourceUsageData));
    }

    public void testGetResourceUsageDataDefault() {
        SandboxLevelResourceUsageView sandboxLevelResourceUsageView = new SandboxLevelResourceUsageView("1234");
        Map<SandboxResourceType, Long> resourceUsageData = sandboxLevelResourceUsageView.getResourceUsageData();
        assertTrue(resourceUsageData.isEmpty());
    }

    public void testGetActiveTasks() {
        SandboxLevelResourceUsageView sandboxLevelResourceUsageView = new SandboxLevelResourceUsageView("1234", resourceUsage, activeTasks);
        List<Task> activeTasks = sandboxLevelResourceUsageView.getActiveTasks();
        assertEquals(1, activeTasks.size());
        assertEquals(4321, activeTasks.get(0).getId());
    }

    public void testGetActiveTasksDefault() {
        SandboxLevelResourceUsageView sandboxLevelResourceUsageView = new SandboxLevelResourceUsageView("1234");
        List<Task> activeTasks = sandboxLevelResourceUsageView.getActiveTasks();
        assertTrue(activeTasks.isEmpty());
    }

    private boolean assertResourceUsageData(Map<SandboxResourceType, Long> resourceUsageData) {
        return resourceUsageData.get(SandboxResourceType.fromString("JVM")) == 34L
            && resourceUsageData.get(SandboxResourceType.fromString("CPU")) == 12L;
    }
}
