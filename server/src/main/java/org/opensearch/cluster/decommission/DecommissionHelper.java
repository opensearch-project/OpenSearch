/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.decommission;

import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.opensearch.action.admin.cluster.configuration.VotingConfigExclusionsHelper.resolveVotingConfigExclusionsAndCheckMaximum;
import static org.opensearch.action.admin.cluster.configuration.VotingConfigExclusionsHelper.addExclusionAndGetState;

/**
 * Static helper utilities to execute decommission
 *
 * @opensearch.internal
 */
public class DecommissionHelper {

    static ClusterState registerDecommissionAttributeInClusterState(
        ClusterState currentState,
        DecommissionAttribute decommissionAttribute,
        String requestID
    ) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(decommissionAttribute, requestID);
        return ClusterState.builder(currentState)
            .metadata(Metadata.builder(currentState.metadata()).decommissionAttributeMetadata(decommissionAttributeMetadata))
            .build();
    }

    static ClusterState deleteDecommissionAttributeInClusterState(ClusterState currentState) {
        Metadata metadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(metadata);
        mdBuilder.removeCustom(DecommissionAttributeMetadata.TYPE);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    static ClusterState addVotingConfigExclusionsForNodesToBeDecommissioned(
        ClusterState currentState,
        Set<String> nodeIdsToBeExcluded,
        TimeValue decommissionActionTimeout,
        final int maxVotingConfigExclusions
    ) {
        AddVotingConfigExclusionsRequest request = new AddVotingConfigExclusionsRequest(
            Strings.EMPTY_ARRAY,
            nodeIdsToBeExcluded.toArray(String[]::new),
            Strings.EMPTY_ARRAY,
            decommissionActionTimeout
        );
        Set<VotingConfigExclusion> resolvedExclusion = resolveVotingConfigExclusionsAndCheckMaximum(
            request,
            currentState,
            maxVotingConfigExclusions
        );
        return addExclusionAndGetState(currentState, resolvedExclusion, maxVotingConfigExclusions);
    }

    static Set<DiscoveryNode> filterNodesWithDecommissionAttribute(
        ClusterState clusterState,
        DecommissionAttribute decommissionAttribute,
        boolean onlyClusterManagerNodes
    ) {
        Set<DiscoveryNode> nodesWithDecommissionAttribute = new HashSet<>();
        Iterator<DiscoveryNode> nodesIter = onlyClusterManagerNodes
            ? clusterState.nodes().getClusterManagerNodes().values().iterator()
            : clusterState.nodes().getNodes().values().iterator();

        while (nodesIter.hasNext()) {
            final DiscoveryNode node = nodesIter.next();
            if (nodeHasDecommissionedAttribute(node, decommissionAttribute)) {
                nodesWithDecommissionAttribute.add(node);
            }
        }
        return nodesWithDecommissionAttribute;
    }

    /**
     * Utility method to check if the node has decommissioned attribute
     *
     * @param discoveryNode node to check on
     * @param decommissionAttribute attribute to be checked with
     * @return true or false based on whether node has decommissioned attribute
     */
    public static boolean nodeHasDecommissionedAttribute(DiscoveryNode discoveryNode, DecommissionAttribute decommissionAttribute) {
        String nodeAttributeValue = discoveryNode.getAttributes().get(decommissionAttribute.attributeName());
        return nodeAttributeValue != null && nodeAttributeValue.equals(decommissionAttribute.attributeValue());
    }

    /**
     * Utility method to check if the node is commissioned or not
     *
     * @param discoveryNode node to check on
     * @param metadata metadata present current which will be used to check the commissioning status of the node
     * @return if the node is commissioned or not
     */
    public static boolean nodeCommissioned(DiscoveryNode discoveryNode, Metadata metadata) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = metadata.decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null) {
            DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
            DecommissionStatus status = decommissionAttributeMetadata.status();
            if (decommissionAttribute != null && status != null) {
                if (nodeHasDecommissionedAttribute(discoveryNode, decommissionAttribute)
                    && (status.equals(DecommissionStatus.IN_PROGRESS)
                        || status.equals(DecommissionStatus.SUCCESSFUL)
                        || status.equals(DecommissionStatus.DRAINING))) {
                    return false;
                }
            }
        }
        return true;
    }
}
