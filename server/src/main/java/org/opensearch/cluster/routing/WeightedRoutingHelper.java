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
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Helper class for Weighted Routing Changes
 */

public class WeightedRoutingHelper {
    /**
     * shard search requests are retried on shard copies belonging to az with shard routing weight zero in case of
     * internal failures
     *
     * @param e
     * @return
     */
    public static boolean isInternalFailure(Exception e) {
        return e instanceof NoShardAvailableActionException
            || e instanceof UnavailableShardsException
            || e instanceof NodeNotConnectedException
            || e instanceof NodeDisconnectedException;
    }

    /**
     * This function checks if the shard copy is present in az with weighted routing weight zero.
     *
     * @param nodeId the node with the shard copy
     * @param clusterState
     * @return true if the node is in weigh away az ie az with shard routing weight set to zero, else false
     */
    public static boolean shardInWeighedAwayAZ(String nodeId, ClusterState clusterState) {
        DiscoveryNode node = clusterState.nodes().get(nodeId);
        AtomicBoolean shardInWeighedAwayAZ = new AtomicBoolean(false);
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
                keys.forEach(key -> {
                    if (node.getAttributes().get("zone").equals(key)) {
                        shardInWeighedAwayAZ.set(true);
                    }
                });
            }
        }
        return shardInWeighedAwayAZ.get();
    }
}
