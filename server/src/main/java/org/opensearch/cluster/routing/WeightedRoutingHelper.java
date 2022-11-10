/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.transport.NodeNotConnectedException;

import java.util.Map;
import java.util.stream.Stream;

/**
 * * WeightedRouting helper class
 */

public class WeightedRoutingHelper {

    public static boolean isInternalFailure(Exception e) {
        return e instanceof NoShardAvailableActionException
            || e instanceof UnavailableShardsException
            || e instanceof NodeNotConnectedException;
    }

    public static boolean shardInWeighedAwayAZ(String nodeId, ClusterState clusterState) {
        DiscoveryNode targetNode = clusterState.nodes().get(nodeId);
        WeightedRoutingMetadata weightedRoutingMetadata = clusterState.metadata().weightedRoutingMetadata();
        if (weightedRoutingMetadata != null) {
            WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
            if (weightedRouting != null) {
                // Fetch weighted routing attributes with weight set as zero
                Stream<String> keys = weightedRouting.weights()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().intValue() == 0)
                    .map(Map.Entry::getKey);
                return keys != null && targetNode.getAttributes().get("zone").equals(keys.findFirst().get());
            }

        }
        return false;

    }


}
