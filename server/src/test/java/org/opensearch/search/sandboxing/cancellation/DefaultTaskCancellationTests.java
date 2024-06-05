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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultTaskCancellationTests extends OpenSearchTestCase {
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
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 10L, usage);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);

        // setup mocks for sandbox2
        id = "sandbox2";
        resourceTypeStr = "JVM";
        usage = 50L;
        Sandbox sandbox2 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 100L, usage);
        SandboxLevelResourceUsageView mockView2 = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView2);
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
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 10L, usage);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);

        // setup mocks for sandbox2
        id = "sandbox2";
        resourceTypeStr = "CPU";
        usage = 50L;
        Sandbox sandbox2 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 100L, usage);
        SandboxLevelResourceUsageView mockView2 = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView2);
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
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 10L, usage);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);

        // setup mocks for sandbox2
        id = "sandbox2";
        resourceTypeStr = "JVM";
        usage = 50L;
        Sandbox sandbox2 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 10L, usage);
        SandboxLevelResourceUsageView mockView2 = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView2);
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
        String id = "sandbox1";
        String resourceTypeStr = "CPU";
        long usage = 50L;
        Sandbox sandbox1 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 10L, usage);
        SandboxLevelResourceUsageView mockView = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView);

        // setup mocks for sandbox2
        id = "sandbox2";
        resourceTypeStr = "CPU";
        usage = 50L;
        Sandbox sandbox2 = SandboxTestHelpers.createSandboxMock(id, resourceTypeStr, 10L, usage);
        SandboxLevelResourceUsageView mockView2 = SandboxTestHelpers.createResourceUsageViewMock(resourceTypeStr, usage);
        sandboxLevelViews.put(id, mockView2);
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
}
