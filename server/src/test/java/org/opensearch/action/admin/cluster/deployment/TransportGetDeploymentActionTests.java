/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.deployment;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.deployment.Deployment;
import org.opensearch.cluster.deployment.DeploymentManagerService;
import org.opensearch.cluster.deployment.DeploymentState;
import org.opensearch.cluster.deployment.DeploymentStateService;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TransportGetDeploymentActionTests extends OpenSearchTestCase {

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
    }

    public void testGetSpecificDeployment() {
        ClusterState state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), emptyState());
        AtomicReference<ClusterState> stateRef = new AtomicReference<>(state);
        DeploymentStateService service = new DeploymentStateService(stateRef::get);

        Deployment deployment = service.getDeployment("dep-1");
        assertNotNull(deployment);
        assertEquals(DeploymentState.DRAIN, deployment.getState());
        assertEquals(Map.of("zone", "zone-1"), deployment.getNodeAttributes());
    }

    public void testGetNonexistentDeployment() {
        ClusterState state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), emptyState());
        AtomicReference<ClusterState> stateRef = new AtomicReference<>(state);
        DeploymentStateService service = new DeploymentStateService(stateRef::get);

        Deployment deployment = service.getDeployment("nonexistent");
        assertNull(deployment);
    }

    public void testGetAllDeployments() {
        ClusterState state = emptyState();
        state = DeploymentManagerService.innerStartDeployment("dep-1", Map.of("zone", "zone-1"), state);
        state = DeploymentManagerService.innerStartDeployment("dep-2", Map.of("zone", "zone-2"), state);
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
