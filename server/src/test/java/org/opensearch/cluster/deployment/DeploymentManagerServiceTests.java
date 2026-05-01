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

public class DeploymentManagerServiceTests extends OpenSearchTestCase {

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
    }

    public void testStartDeployment() {
        ClusterState state = emptyState();
        ClusterState newState = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);

        DeploymentMetadata metadata = newState.metadata().custom(DeploymentMetadata.TYPE);
        assertNotNull(metadata);
        assertEquals(1, metadata.getDeployments().size());
        assertTrue(metadata.getDeployments().containsKey("dep-1"));

        Deployment deployment = metadata.getDeployments().get("dep-1");
        assertEquals(DeploymentState.DRAIN, deployment.getState());
        assertEquals(Map.of("zone", "zone-1"), deployment.getNodeAttributes());
    }

    public void testStartMultipleDeployments() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);
        state = DeploymentManagerService.innerStartDeployment("dep-2", Map.of("zone", "zone-2"), state);

        DeploymentMetadata metadata = state.metadata().custom(DeploymentMetadata.TYPE);
        assertNotNull(metadata);
        assertEquals(2, metadata.getDeployments().size());
        assertTrue(metadata.getDeployments().containsKey("dep-1"));
        assertTrue(metadata.getDeployments().containsKey("dep-2"));
    }

    public void testStartDeploymentRejectsOverlappingAttributes() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeploymentManagerService.innerStartDeployment("dep-2", Map.of("zone", "zone-1"), finalState)
        );
        assertTrue(e.getMessage().contains("already targets the same attributes"));
    }

    public void testStartDeploymentRejectsInconsistentKeys() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeploymentManagerService.innerStartDeployment("dep-2", Map.of("rack", "rack-1"), finalState)
        );
        assertTrue(e.getMessage().contains("must match existing deployment keys"));
    }

    public void testFinishDeployment() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);
        state = DeploymentManagerService.innerStartDeployment("dep-2", Map.of("zone", "zone-2"), state);
        state = DeploymentManagerService.innerFinishDeployment("dep-1", state);

        DeploymentMetadata metadata = state.metadata().custom(DeploymentMetadata.TYPE);
        assertNotNull(metadata);
        assertEquals(1, metadata.getDeployments().size());
        assertFalse(metadata.getDeployments().containsKey("dep-1"));
        assertTrue(metadata.getDeployments().containsKey("dep-2"));
    }

    public void testFinishDeploymentRemovesCustomWhenEmpty() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);
        state = DeploymentManagerService.innerFinishDeployment("dep-1", state);

        DeploymentMetadata metadata = state.metadata().custom(DeploymentMetadata.TYPE);
        assertNull(metadata);
    }

    public void testFinishNonexistentDeployment() {
        ClusterState state = emptyState();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DeploymentManagerService.innerFinishDeployment("nonexistent", state)
        );
        assertTrue(e.getMessage().contains("not found"));
    }
}
