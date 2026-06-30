/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;

import java.util.Map;
import java.util.stream.Stream;

/**
 *  Utils for Weighted Routing
 *
 * @opensearch.internal
 */
public class WeightedRoutingUtils {

    /**
     * This function checks if the node is weighed away ie weighted routing weight is set to 0,
     *
     * @param nodeId the node
     * @return true if the node has attribute value with shard routing weight set to zero, else false
     */
    public static boolean isWeighedAway(String nodeId, ClusterState clusterState) {
        DiscoveryNode node = clusterState.nodes().get(nodeId);
        if (node == null) {
            return false;
        }
        WeightedRoutingMetadata weightedRoutingMetadata = clusterState.metadata().weightedRoutingMetadata();
        if (weightedRoutingMetadata != null) {
            WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
            if (weightedRouting != null && weightedRouting.isSet()) {
                // Fetch weighted routing attributes with weight set as zero
                Stream<String> keys = weightedRouting.weights()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().intValue() == WeightedRoutingMetadata.WEIGHED_AWAY_WEIGHT)
                    .map(Map.Entry::getKey);

                for (Object key : keys.toArray()) {
                    if (node.getAttributes().get(weightedRouting.attributeName()).equals(key.toString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean shouldPerformWeightedRouting(boolean ignoreWeightedRouting, WeightedRoutingMetadata weightedRoutingMetadata) {
        return !ignoreWeightedRouting && weightedRoutingMetadata != null && weightedRoutingMetadata.getWeightedRouting().isSet();
    }

    public static boolean shouldPerformStrictWeightedRouting(
        boolean isStrictWeightedShardRouting,
        boolean ignoreWeightedRouting,
        WeightedRoutingMetadata weightedRoutingMetadata
    ) {
        return isStrictWeightedShardRouting && shouldPerformWeightedRouting(ignoreWeightedRouting, weightedRoutingMetadata);
    }
}
