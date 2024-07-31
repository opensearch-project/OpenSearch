/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.search.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.wlm.QueryGroupTestHelpers.getRandomTask;

public class QueryGroupLevelResourceUsageViewTests extends OpenSearchTestCase {
    Map<ResourceType, Long> resourceUsage;
    List<Task> activeTasks;

    public void setUp() throws Exception {
        super.setUp();
        resourceUsage = Map.of(ResourceType.fromName("memory"), 34L, ResourceType.fromName("cpu"), 12L);
        activeTasks = List.of(getRandomTask(4321));
    }

    public void testGetResourceUsageData() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
            resourceUsage,
            activeTasks
        );
        Map<ResourceType, Long> resourceUsageData = queryGroupLevelResourceUsageView.getResourceUsageData();
        assertTrue(assertResourceUsageData(resourceUsageData));
    }

    public void testGetActiveTasks() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
            resourceUsage,
            activeTasks
        );
        List<Task> activeTasks = queryGroupLevelResourceUsageView.getActiveTasks();
        assertEquals(1, activeTasks.size());
        assertEquals(4321, activeTasks.get(0).getId());
    }

    private boolean assertResourceUsageData(Map<ResourceType, Long> resourceUsageData) {
        return resourceUsageData.get(ResourceType.fromName("memory")) == 34L && resourceUsageData.get(ResourceType.fromName("cpu")) == 12L;
    }
}
