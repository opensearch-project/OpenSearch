/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.function.Predicate;

/**
 * Tests for {@link ClusterManagerNodeChangePredicate}.
 * Covers both the {@code build(ClusterState)} and {@code build(long, String)} overloads.
 */
public class ClusterManagerNodeChangePredicateTests extends OpenSearchTestCase {

    private DiscoveryNode clusterManagerNode;
    private DiscoveryNode otherClusterManagerNode;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterManagerNode = new DiscoveryNode("old_master", buildNewFakeTransportAddress(), Version.CURRENT);
        otherClusterManagerNode = new DiscoveryNode("new_master", buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private ClusterState buildState(DiscoveryNode clusterManagerNode, long version) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        if (clusterManagerNode != null) {
            nodesBuilder.add(clusterManagerNode);
            nodesBuilder.clusterManagerNodeId(clusterManagerNode.getId());
        }
        nodesBuilder.add(otherClusterManagerNode);
        return ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder).version(version).build();
    }

    public void testRejectsNullNewClusterManager() {
        ClusterState currentState = buildState(clusterManagerNode, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertFalse(predicate.test(buildState(null, 5)));
    }

    public void testAcceptsDifferentClusterManager() {
        ClusterState currentState = buildState(clusterManagerNode, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertTrue(predicate.test(buildState(otherClusterManagerNode, 1)));
    }

    public void testAcceptsHigherVersionSameClusterManager() {
        ClusterState currentState = buildState(clusterManagerNode, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertTrue(predicate.test(buildState(clusterManagerNode, 2)));
    }

    public void testRejectsSameVersionSameClusterManager() {
        ClusterState currentState = buildState(clusterManagerNode, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertFalse(predicate.test(buildState(clusterManagerNode, 1)));
    }

    public void testRejectsLowerVersionSameClusterManager() {
        ClusterState currentState = buildState(clusterManagerNode, 5);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertFalse(predicate.test(buildState(clusterManagerNode, 3)));
    }

    public void testNullCurrentClusterManagerRejectsNullNewClusterManager() {
        ClusterState currentState = buildState(null, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertFalse(predicate.test(buildState(null, 5)));
    }

    public void testNullCurrentClusterManagerAcceptsNewClusterManager() {
        ClusterState currentState = buildState(null, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);

        assertTrue(predicate.test(buildState(clusterManagerNode, 1)));
    }

    public void testBuildPrimitivesWithNullCurrentClusterManagerAcceptsNewClusterManager() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, null);

        assertTrue(predicate.test(buildState(clusterManagerNode, 1)));
    }

    public void testBuildPrimitivesWithNullCurrentClusterManagerRejectsNullNewClusterManager() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, null);

        assertFalse(predicate.test(buildState(null, 5)));
    }

    public void testBothOverloadsAgreeOnClusterManagerChange() {
        ClusterState currentState = buildState(clusterManagerNode, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(
            currentState.version(),
            currentState.nodes().getClusterManagerNode().getEphemeralId()
        );
        ClusterState newState = buildState(otherClusterManagerNode, 5);

        assertEquals(fromState.test(newState), fromPrimitives.test(newState));
    }

    public void testBothOverloadsAgreeOnVersionBump() {
        ClusterState currentState = buildState(clusterManagerNode, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(
            currentState.version(),
            currentState.nodes().getClusterManagerNode().getEphemeralId()
        );
        ClusterState newState = buildState(clusterManagerNode, 10);

        assertEquals(fromState.test(newState), fromPrimitives.test(newState));
    }

    public void testBothOverloadsAgreeOnNoChange() {
        ClusterState currentState = buildState(clusterManagerNode, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(
            currentState.version(),
            currentState.nodes().getClusterManagerNode().getEphemeralId()
        );
        ClusterState newState = buildState(clusterManagerNode, 5);

        assertEquals(fromState.test(newState), fromPrimitives.test(newState));
    }

    public void testBothOverloadsAgreeOnNullCurrentClusterManager() {
        ClusterState currentState = buildState(null, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(currentState.version(), null);

        assertEquals(fromState.test(buildState(clusterManagerNode, 5)), fromPrimitives.test(buildState(clusterManagerNode, 5)));
        assertEquals(fromState.test(buildState(null, 10)), fromPrimitives.test(buildState(null, 10)));
    }
}
