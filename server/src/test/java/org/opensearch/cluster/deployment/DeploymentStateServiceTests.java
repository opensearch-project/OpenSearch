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
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.cluster.deployment.DeploymentState.DRAIN;

public class DeploymentStateServiceTests extends OpenSearchTestCase {

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
    }

    public void testGetSpecificDeployment() {
        ClusterState state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), emptyState(), DRAIN);
        AtomicReference<ClusterState> stateRef = new AtomicReference<>(state);
        DeploymentStateService service = new DeploymentStateService(stateRef::get);

        Deployment deployment = service.getDeployment("dep-1");
        assertNotNull(deployment);
        assertEquals(DRAIN, deployment.getState());
        assertEquals(Map.of("zone", "zone-1"), deployment.getNodeAttributes());
    }

    public void testGetNonexistentDeployment() {
        ClusterState state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), emptyState(), DRAIN);
        AtomicReference<ClusterState> stateRef = new AtomicReference<>(state);
        DeploymentStateService service = new DeploymentStateService(stateRef::get);

        Deployment deployment = service.getDeployment("nonexistent");
        assertNull(deployment);
    }

    public void testGetAllDeployments() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerTransitionDeployment("dep-1", Map.of("zone", "zone-1"), state, DRAIN);
        state = DeploymentManagerService.innerTransitionDeployment("dep-2", Map.of("zone", "zone-2"), state, DRAIN);
        AtomicReference<ClusterState> stateRef = new AtomicReference<>(state);
        DeploymentStateService service = new DeploymentStateService(stateRef::get);

        Map<String, Deployment> deployments = service.getAllDeployments();
        assertEquals(2, deployments.size());
        assertTrue(deployments.containsKey("dep-1"));
        assertTrue(deployments.containsKey("dep-2"));
    }

    public void testGetAllDeploymentsWhenNone() {
        AtomicReference<ClusterState> stateRef = new AtomicReference<>(emptyState());
        DeploymentStateService service = new DeploymentStateService(stateRef::get);

        Map<String, Deployment> deployments = service.getAllDeployments();
        assertTrue(deployments.isEmpty());
    }
}
