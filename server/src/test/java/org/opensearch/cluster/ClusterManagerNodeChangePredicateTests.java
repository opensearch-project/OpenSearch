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

    private DiscoveryNode nodeA;
    private DiscoveryNode nodeB;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        nodeA = new DiscoveryNode("node_a", buildNewFakeTransportAddress(), Version.CURRENT);
        nodeB = new DiscoveryNode("node_b", buildNewFakeTransportAddress(), Version.CURRENT);
    }

    private ClusterState buildState(DiscoveryNode masterNode, long version) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(nodeA);
        nodesBuilder.add(nodeB);
        if (masterNode != null) {
            nodesBuilder.clusterManagerNodeId(masterNode.getId());
        }
        return ClusterState.builder(new ClusterName("test")).nodes(nodesBuilder).version(version).build();
    }

    // ---- Tests for build(ClusterState) ----

    public void testBuildClusterState_rejectsNullNewMaster() {
        ClusterState currentState = buildState(nodeA, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(null, 5);
        assertFalse(predicate.test(newState));
    }

    public void testBuildClusterState_acceptsDifferentMaster() {
        ClusterState currentState = buildState(nodeA, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(nodeB, 1);
        assertTrue(predicate.test(newState));
    }

    public void testBuildClusterState_acceptsHigherVersionSameMaster() {
        ClusterState currentState = buildState(nodeA, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(nodeA, 2);
        assertTrue(predicate.test(newState));
    }

    public void testBuildClusterState_rejectsSameVersionSameMaster() {
        ClusterState currentState = buildState(nodeA, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(nodeA, 1);
        assertFalse(predicate.test(newState));
    }

    public void testBuildClusterState_rejectsLowerVersionSameMaster() {
        ClusterState currentState = buildState(nodeA, 5);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(nodeA, 3);
        assertFalse(predicate.test(newState));
    }

    public void testBuildClusterState_currentMasterNull_rejectsNullNewMaster() {
        ClusterState currentState = buildState(null, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(null, 5);
        assertFalse(predicate.test(newState));
    }

    public void testBuildClusterState_currentMasterNull_acceptsAnyNewMaster() {
        ClusterState currentState = buildState(null, 1);
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(currentState);
        ClusterState newState = buildState(nodeA, 1);
        assertTrue(predicate.test(newState));
    }

    // ---- Tests for build(long, String) ----

    public void testBuildPrimitives_rejectsNullNewMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, nodeA.getEphemeralId());
        ClusterState newState = buildState(null, 5);
        assertFalse(predicate.test(newState));
    }

    public void testBuildPrimitives_acceptsDifferentMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, nodeA.getEphemeralId());
        ClusterState newState = buildState(nodeB, 1);
        assertTrue(predicate.test(newState));
    }

    public void testBuildPrimitives_acceptsHigherVersionSameMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, nodeA.getEphemeralId());
        ClusterState newState = buildState(nodeA, 2);
        assertTrue(predicate.test(newState));
    }

    public void testBuildPrimitives_rejectsSameVersionSameMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, nodeA.getEphemeralId());
        ClusterState newState = buildState(nodeA, 1);
        assertFalse(predicate.test(newState));
    }

    public void testBuildPrimitives_rejectsLowerVersionSameMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(5, nodeA.getEphemeralId());
        ClusterState newState = buildState(nodeA, 3);
        assertFalse(predicate.test(newState));
    }

    public void testBuildPrimitives_nullCurrentMaster_rejectsNullNewMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, null);
        ClusterState newState = buildState(null, 5);
        assertFalse(predicate.test(newState));
    }

    public void testBuildPrimitives_nullCurrentMaster_acceptsAnyNewMaster() {
        Predicate<ClusterState> predicate = ClusterManagerNodeChangePredicate.build(1, null);
        ClusterState newState = buildState(nodeA, 1);
        assertTrue(predicate.test(newState));
    }

    // ---- Tests verifying both overloads produce equivalent results ----

    public void testBothOverloadsAgree_masterChanged() {
        ClusterState currentState = buildState(nodeA, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(
            currentState.version(),
            currentState.nodes().getClusterManagerNode().getEphemeralId()
        );
        ClusterState newState = buildState(nodeB, 5);
        assertEquals(fromState.test(newState), fromPrimitives.test(newState));
    }

    public void testBothOverloadsAgree_versionBumped() {
        ClusterState currentState = buildState(nodeA, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(
            currentState.version(),
            currentState.nodes().getClusterManagerNode().getEphemeralId()
        );
        ClusterState newState = buildState(nodeA, 10);
        assertEquals(fromState.test(newState), fromPrimitives.test(newState));
    }

    public void testBothOverloadsAgree_noChange() {
        ClusterState currentState = buildState(nodeA, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(
            currentState.version(),
            currentState.nodes().getClusterManagerNode().getEphemeralId()
        );
        ClusterState newState = buildState(nodeA, 5);
        assertEquals(fromState.test(newState), fromPrimitives.test(newState));
    }

    public void testBothOverloadsAgree_nullCurrentMaster() {
        ClusterState currentState = buildState(null, 5);
        Predicate<ClusterState> fromState = ClusterManagerNodeChangePredicate.build(currentState);
        Predicate<ClusterState> fromPrimitives = ClusterManagerNodeChangePredicate.build(currentState.version(), null);

        ClusterState newStateWithMaster = buildState(nodeA, 5);
        assertEquals(fromState.test(newStateWithMaster), fromPrimitives.test(newStateWithMaster));

        ClusterState newStateNoMaster = buildState(null, 10);
        assertEquals(fromState.test(newStateNoMaster), fromPrimitives.test(newStateNoMaster));
    }
}
