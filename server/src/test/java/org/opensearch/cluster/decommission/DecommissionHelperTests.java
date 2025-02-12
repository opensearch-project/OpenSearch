/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.decommission.DecommissionHelper.addVotingConfigExclusionsForNodesToBeDecommissioned;
import static org.opensearch.cluster.decommission.DecommissionHelper.deleteDecommissionAttributeInClusterState;
import static org.opensearch.cluster.decommission.DecommissionHelper.filterNodesWithDecommissionAttribute;
import static org.opensearch.cluster.decommission.DecommissionHelper.nodeCommissioned;
import static org.opensearch.cluster.decommission.DecommissionHelper.registerDecommissionAttributeInClusterState;

public class DecommissionHelperTests extends OpenSearchTestCase {

    private static DiscoveryNode node1, node2, node3, dataNode;
    private static ClusterState initialClusterState;

    public void testRegisterAndDeleteDecommissionAttributeInClusterState() {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone2");
        ClusterState updatedState = registerDecommissionAttributeInClusterState(
            initialClusterState,
            decommissionAttribute,
            randomAlphaOfLength(10)
        );
        assertEquals(decommissionAttribute, updatedState.metadata().decommissionAttributeMetadata().decommissionAttribute());
        updatedState = deleteDecommissionAttributeInClusterState(updatedState);
        assertNull(updatedState.metadata().decommissionAttributeMetadata());
    }

    public void testAddVotingConfigExclusionsForNodesToBeDecommissioned() {
        Set<String> nodeIdToBeExcluded = Set.of("node2");
        ClusterState updatedState = addVotingConfigExclusionsForNodesToBeDecommissioned(
            initialClusterState,
            nodeIdToBeExcluded,
            TimeValue.timeValueMinutes(1),
            10
        );
        CoordinationMetadata.VotingConfigExclusion v1 = new CoordinationMetadata.VotingConfigExclusion(node2);
        assertTrue(
            updatedState.coordinationMetadata().getVotingConfigExclusions().contains(new CoordinationMetadata.VotingConfigExclusion(node2))
        );
        assertEquals(1, updatedState.coordinationMetadata().getVotingConfigExclusions().size());
    }

    public void testFilterNodes() {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone1");
        Set<DiscoveryNode> filteredNodes = filterNodesWithDecommissionAttribute(initialClusterState, decommissionAttribute, true);
        assertTrue(filteredNodes.contains(node1));
        assertEquals(1, filteredNodes.size());
        filteredNodes = filterNodesWithDecommissionAttribute(initialClusterState, decommissionAttribute, false);
        assertTrue(filteredNodes.contains(node1));
        assertTrue(filteredNodes.contains(dataNode));
        assertEquals(2, filteredNodes.size());
    }

    public void testNodeCommissioned() {
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone1");
        DecommissionStatus decommissionStatus = randomFrom(
            DecommissionStatus.IN_PROGRESS,
            DecommissionStatus.DRAINING,
            DecommissionStatus.SUCCESSFUL
        );
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            decommissionStatus,
            randomAlphaOfLength(10)
        );
        Metadata metadata = Metadata.builder().putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build();
        assertTrue(nodeCommissioned(node2, metadata));
        assertFalse(nodeCommissioned(node1, metadata));
        DecommissionStatus commissionStatus = randomFrom(DecommissionStatus.FAILED, DecommissionStatus.INIT);
        decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute, commissionStatus, randomAlphaOfLength(10));
        metadata = Metadata.builder().putCustom(DecommissionAttributeMetadata.TYPE, decommissionAttributeMetadata).build();
        assertTrue(nodeCommissioned(node2, metadata));
        assertTrue(nodeCommissioned(node1, metadata));
        metadata = Metadata.builder().removeCustom(DecommissionAttributeMetadata.TYPE).build();
        assertTrue(nodeCommissioned(node2, metadata));
        assertTrue(nodeCommissioned(node1, metadata));
    }

    @BeforeClass
    public static void createBaseClusterState() {
        node1 = makeDiscoveryNode("node1", "zone1");
        node2 = makeDiscoveryNode("node2", "zone2");
        node3 = makeDiscoveryNode("node3", "zone3");
        dataNode = new DiscoveryNode(
            "data",
            "data",
            buildNewFakeTransportAddress(),
            singletonMap("zone", "zone1"),
            emptySet(),
            Version.CURRENT
        );
        final CoordinationMetadata.VotingConfiguration allNodesConfig = CoordinationMetadata.VotingConfiguration.of(node1, node2, node3);
        initialClusterState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                new DiscoveryNodes.Builder().add(node1)
                    .add(node2)
                    .add(node3)
                    .add(dataNode)
                    .localNodeId(node1.getId())
                    .clusterManagerNodeId(node1.getId())
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastAcceptedConfiguration(allNodesConfig)
                            .lastCommittedConfiguration(allNodesConfig)
                            .build()
                    )
            )
            .build();
    }

    private static DiscoveryNode makeDiscoveryNode(String name, String zone) {
        return new DiscoveryNode(
            name,
            name,
            buildNewFakeTransportAddress(),
            singletonMap("zone", zone),
            singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
    }
}
