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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTaskCancellationTest extends OpenSearchTestCase {
    @Mock
    private TaskSelectionStrategy mockStrategy;

    @Mock
    private SandboxLevelResourceUsageView mockView;

    private Map<String, SandboxLevelResourceUsageView> sandboxLevelViews;
    private Set<Sandbox> activeSandboxes;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        sandboxLevelViews = new HashMap<>();
        activeSandboxes = new HashSet<>();
    }

    public void testConstructor() {
        DefaultTaskCancellation cancellation = new DefaultTaskCancellation(mockStrategy, sandboxLevelViews, activeSandboxes);
        assertNotNull(cancellation);
    }

    public void testGetSandboxesToCancelFrom_whenNotAllSandboxesAreBreachingForDifferentResourceTypes() {
        // setup mocks for sandbox1
        Sandbox sandbox1 = createSandboxMock("sandbox1", "CPU", 10L, 50L);
        // setup mocks for sandbox2
        Sandbox sandbox2 = createSandboxMock("sandbox2", "JVM", 100L, 50L);
        // add both sandboxes to active sandboxes
        Collections.addAll(activeSandboxes, sandbox1, sandbox2);
        // create a new instance of DefaultTaskCancellation and call getSandboxesToCancelFrom
        DefaultTaskCancellation cancellation = new DefaultTaskCancellation(mockStrategy, sandboxLevelViews, activeSandboxes);
        List<Sandbox> result = cancellation.getSandboxesToCancelFrom();

        // only sandbox1 should be returned as it is breaching the threshold
        assertEquals(1, result.size());
        assertTrue(result.contains(sandbox1));
        assertFalse(result.contains(sandbox2));
    }

    public void testGetSandboxesToCancelFrom_whenNotAllSandboxesAreBreachingForSameResourceType() {
        // setup mocks for sandbox1
        Sandbox sandbox1 = createSandboxMock("sandbox1", "CPU", 10L, 50L);
        // setup mocks for sandbox2
        Sandbox sandbox2 = createSandboxMock("sandbox2", "CPU", 100L, 50L);
        // add both sandboxes to active sandboxes
        Collections.addAll(activeSandboxes, sandbox1, sandbox2);
        // create a new instance of DefaultTaskCancellation and call getSandboxesToCancelFrom
        DefaultTaskCancellation cancellation = new DefaultTaskCancellation(mockStrategy, sandboxLevelViews, activeSandboxes);
        List<Sandbox> result = cancellation.getSandboxesToCancelFrom();

        // only sandbox1 should be returned as it is breaching the threshold
        assertEquals(1, result.size());
        assertTrue(result.contains(sandbox1));
        assertFalse(result.contains(sandbox2));
    }

    public void testGetSandboxesToCancelFrom_whenAllSandboxesAreBreachingForDifferentResourceTypes() {
        // setup mocks for sandbox1
        Sandbox sandbox1 = createSandboxMock("sandbox1", "CPU", 10L, 50L);
        // setup mocks for sandbox2
        Sandbox sandbox2 = createSandboxMock("sandbox2", "JVM", 10L, 50L);
        // add both sandboxes to active sandboxes
        Collections.addAll(activeSandboxes, sandbox1, sandbox2);
        // create a new instance of DefaultTaskCancellation and call getSandboxesToCancelFrom
        DefaultTaskCancellation cancellation = new DefaultTaskCancellation(mockStrategy, sandboxLevelViews, activeSandboxes);
        List<Sandbox> result = cancellation.getSandboxesToCancelFrom();

        // Both sandboxes should be returned as it is breaching the threshold
        assertEquals(2, result.size());
        assertTrue(result.contains(sandbox1));
        assertTrue(result.contains(sandbox2));
    }

    public void testGetSandboxesToCancelFrom_whenAllSandboxesAreBreachingForSameResourceType() {
        // setup mocks for sandbox1
        Sandbox sandbox1 = createSandboxMock("sandbox1", "CPU", 10L, 50L);
        // setup mocks for sandbox2
        Sandbox sandbox2 = createSandboxMock("sandbox2", "CPU", 10L, 50L);
        // add both sandboxes to active sandboxes
        Collections.addAll(activeSandboxes, sandbox1, sandbox2);
        // create a new instance of DefaultTaskCancellation and call getSandboxesToCancelFrom
        DefaultTaskCancellation cancellation = new DefaultTaskCancellation(mockStrategy, sandboxLevelViews, activeSandboxes);
        List<Sandbox> result = cancellation.getSandboxesToCancelFrom();

        // Both sandboxes should be returned as it is breaching the threshold
        assertEquals(2, result.size());
        assertTrue(result.contains(sandbox1));
        assertTrue(result.contains(sandbox2));
    }

    // Utility methods
    private Sandbox createSandboxMock(String id, String resourceTypeStr, Long threshold, Long usage) {
        Sandbox sandbox = Mockito.mock(Sandbox.class);
        when(sandbox.getId()).thenReturn(id);

        Sandbox.ResourceLimit resourceLimitMock = createResourceLimitMock(resourceTypeStr, threshold);
        when(sandbox.getResourceLimits()).thenReturn(Collections.singletonList(resourceLimitMock));

        SandboxLevelResourceUsageView mockView = createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);

        return sandbox;
    }

    private Sandbox.ResourceLimit createResourceLimitMock(String resourceTypeStr, Long threshold) {
        Sandbox.ResourceLimit resourceLimitMock = mock(Sandbox.ResourceLimit.class);
        SandboxResourceType resourceType = SandboxResourceType.fromString(resourceTypeStr);
        when(resourceLimitMock.getResourceType()).thenReturn(resourceType);
        when(resourceLimitMock.getThreshold()).thenReturn(threshold);
        return resourceLimitMock;
    }

    private SandboxLevelResourceUsageView createResourceUsageViewMock(String resourceTypeStr, Long usage) {
        SandboxLevelResourceUsageView mockView = mock(SandboxLevelResourceUsageView.class);
        SandboxResourceType resourceType = SandboxResourceType.fromString(resourceTypeStr);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        return mockView;
    }
}
