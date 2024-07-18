/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.querygroup;

import org.opensearch.search.resourcetypes.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.search.querygroup.cancellation.QueryGroupTestHelpers.getRandomTask;

public class QueryGroupLevelResourceUsageViewTests extends OpenSearchTestCase {
    Map<ResourceType, Long> resourceUsage;
    List<Task> activeTasks;

    public void setUp() throws Exception {
        super.setUp();
        resourceUsage = Map.of(ResourceType.fromName("JVM"), 34L, ResourceType.fromName("CPU"), 12L);
        activeTasks = List.of(getRandomTask(4321));
    }

    public void testGetResourceUsageData() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
            "1234",
            resourceUsage,
            activeTasks
        );
        Map<ResourceType, Long> resourceUsageData = queryGroupLevelResourceUsageView.getResourceUsageData();
        assertTrue(assertResourceUsageData(resourceUsageData));
    }

    public void testGetResourceUsageDataDefault() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView("1234");
        Map<ResourceType, Long> resourceUsageData = queryGroupLevelResourceUsageView.getResourceUsageData();
        assertTrue(resourceUsageData.isEmpty());
    }

    public void testGetActiveTasks() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
            "1234",
            resourceUsage,
            activeTasks
        );
        List<Task> activeTasks = queryGroupLevelResourceUsageView.getActiveTasks();
        assertEquals(1, activeTasks.size());
        assertEquals(4321, activeTasks.get(0).getId());
    }

    public void testGetActiveTasksDefault() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView("1234");
        List<Task> activeTasks = queryGroupLevelResourceUsageView.getActiveTasks();
        assertTrue(activeTasks.isEmpty());
    }

    private boolean assertResourceUsageData(Map<ResourceType, Long> resourceUsageData) {
        return resourceUsageData.get(ResourceType.fromName("JVM")) == 34L && resourceUsageData.get(ResourceType.fromName("CPU")) == 12L;
    }
}
