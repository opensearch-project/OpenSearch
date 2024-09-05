/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableQueryGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.tracker.ResourceUsageUtil.CpuUsageUtil;
import org.opensearch.wlm.tracker.ResourceUsageUtil.MemoryUsageUtil;

import java.util.Map;

import static org.opensearch.wlm.cancellation.TaskCanceller.MIN_VALUE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceUsageUtilTests extends OpenSearchTestCase {
    ResourceUsageUtil sut;
    ResourceUsageUtilFactory resourceUsageUtilFactory;
    WorkloadManagementSettings settings;

    public void setUp() throws Exception {
        super.setUp();
        resourceUsageUtilFactory = ResourceUsageUtilFactory.getInstance();
        settings = mock(WorkloadManagementSettings.class);
    }

    public void testFactoryClass() {
        assertTrue(resourceUsageUtilFactory.getInstanceForResourceType(ResourceType.CPU) instanceof CpuUsageUtil);
        assertTrue(resourceUsageUtilFactory.getInstanceForResourceType(ResourceType.MEMORY) instanceof MemoryUsageUtil);
        assertThrows(IllegalArgumentException.class, () -> resourceUsageUtilFactory.getInstanceForResourceType(null));
    }

    public void testCpuUsageUtil() {
        sut = resourceUsageUtilFactory.getInstanceForResourceType(ResourceType.CPU);
        QueryGroup queryGroup = new QueryGroup(
            "testQG",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.50))
        );
        when(settings.getNodeLevelCpuCancellationThreshold()).thenReturn(0.9);

        assertExpectedValues(queryGroup);
    }

    public void testMemoryUsageUtil() {
        sut = resourceUsageUtilFactory.getInstanceForResourceType(ResourceType.MEMORY);
        QueryGroup queryGroup = new QueryGroup(
            "testQG",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.50))
        );
        when(settings.getNodeLevelMemoryCancellationThreshold()).thenReturn(0.9);
        assertExpectedValues(queryGroup);
    }

    private void assertExpectedValues(QueryGroup queryGroup) {
        double normalisedThreshold = sut.getNormalisedThreshold(queryGroup, settings);
        assertEquals(0.45, normalisedThreshold, MIN_VALUE);

        assertEquals(0.1, sut.getExcessUsage(queryGroup, 0.55, settings), MIN_VALUE);
        assertTrue(sut.isBreachingThresholdFor(queryGroup, 0.55, settings));
    }
}
