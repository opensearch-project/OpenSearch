/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.cluster.deployment.DeploymentState.DRAIN;
import static org.opensearch.cluster.deployment.DeploymentState.FINISH;

public class DeploymentManagerServiceTests extends OpenSearchTestCase {

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
    }

    public void testStartDeployment() {
        ClusterState state = emptyState();
        ClusterState newState = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);

        DeploymentMetadata metadata = newState.metadata().custom(DeploymentMetadata.TYPE);
        assertNotNull(metadata);
        assertEquals(1, metadata.getDeployments().size());
        assertTrue(metadata.getDeployments().containsKey("dep-1"));

        Deployment deployment = metadata.getDeployments().get("dep-1");
        assertEquals(DRAIN, deployment.getState());
        assertEquals(Map.of("zone", "zone-1"), deployment.getNodeAttributes());
    }

    public void testStartMultipleDeployments() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);
        state = DeploymentManagerService.innerTransitionDeployment("dep-2", Map.of("zone", "zone-2"), state, DRAIN);

        DeploymentMetadata metadata = state.metadata().custom(DeploymentMetadata.TYPE);
        assertNotNull(metadata);
        assertEquals(2, metadata.getDeployments().size());
        assertTrue(metadata.getDeployments().containsKey("dep-1"));
        assertTrue(metadata.getDeployments().containsKey("dep-2"));
    }

    public void testStartDeploymentRejectsOverlappingAttributes() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeploymentManagerService.innerTransitionDeployment("dep-2", Map.of("zone", "zone-1"), finalState, DRAIN)
        );
        assertTrue(e.getMessage().contains("already targets attribute zone=zone-1"));
    }

    public void testStartDeploymentRejectsInconsistentKeys() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeploymentManagerService.innerTransitionDeployment("dep-2", Map.of("rack", "rack-1"), finalState, DRAIN)
        );
        assertTrue(e.getMessage().contains("must match existing deployment keys"));
    }

    public void testFinishDeployment() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);
        state = DeploymentManagerService.innerTransitionDeployment("dep-2", Map.of("zone", "zone-2"), state, DRAIN);
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", state, FINISH);

        DeploymentMetadata metadata = state.metadata().custom(DeploymentMetadata.TYPE);
        assertNotNull(metadata);
        assertEquals(1, metadata.getDeployments().size());
        assertFalse(metadata.getDeployments().containsKey("dep-1"));
        assertTrue(metadata.getDeployments().containsKey("dep-2"));
    }

    public void testFinishDeploymentRemovesCustomWhenEmpty() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", state, FINISH);

        DeploymentMetadata metadata = state.metadata().custom(DeploymentMetadata.TYPE);
        assertNull(metadata);
    }

    public void testFinishNonexistentDeployment() {
        ClusterState state = emptyState();
        ClusterState newState = DeploymentManagerService.innerTransitionDeployment("nonexistent", state, FINISH);
        assertSame(state, newState);
    }
}
