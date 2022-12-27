/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.search.SearchShardIterator;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * This class contains logic to decide if retryable shard search requests can be tried on shard copies present in data
 * nodes whose attribute value weight for weighted shard routing is set to zero.
 */

public class FailOpenRouting {

    private static final Logger logger = LogManager.getLogger(FailOpenRouting.class);

    /**
     * *
     * @param e exception causing failure for previous shard copy
     * @return true if exception is due to cluster availability issues
     */
    private static boolean isInternalFailure(Exception e) {
        return e instanceof NoShardAvailableActionException
            || e instanceof UnavailableShardsException
            || e instanceof NodeNotConnectedException
            || e instanceof NodeDisconnectedException;
    }

    /**
     * This function checks if the shard is present in data node with weighted routing weight set to 0,
     * In such cases we fail open, if shard search request for the shard from other shard copies fail with non
     * retryable exception.
     *
     * @param nodeId the node with the shard copy
     * @param clusterState the cluster state
     * @return true if the node has attribute value with shard routing weight set to zero, else false
     */
    private static boolean shardInNodeWithZeroWeightedRouting(String nodeId, ClusterState clusterState) {
        DiscoveryNode node = clusterState.nodes().get(nodeId);
        AtomicBoolean shardInNodeWithZeroWeightedRouting = new AtomicBoolean(false);
        WeightedRoutingMetadata weightedRoutingMetadata = clusterState.metadata().weightedRoutingMetadata();
        if (weightedRoutingMetadata != null) {
            WeightedRouting weightedRouting = weightedRoutingMetadata.getWeightedRouting();
            // TODO: check weighted routing has weights set after merging versioning changes
            if (weightedRouting != null) {
                // Fetch weighted routing attributes with weight set as zero
                Stream<String> keys = weightedRouting.weights()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().intValue() == 0)
                    .map(Map.Entry::getKey);
                keys.forEach(key -> {
                    if (node.getAttributes().get(weightedRouting.attributeName()).equals(key)) {
                        shardInNodeWithZeroWeightedRouting.set(true);
                    }
                });
            }
        }
        return shardInNodeWithZeroWeightedRouting.get();
    }

    /**
     * This function returns next shard copy to retry search request in case of failure from previous copy returned
     * by the iterator. It has the logic to fail open ie request shard copies present in nodes with weighted shard
     * routing weight set to zero
     *
     * @param shardIt Shard Iterator containing order in which shard copies for a shard need to be requested
     * @param e exception causing failure for previous shard copy
     * @param clusterState the cluster state
     * @return the next shard copy
     */
    public static SearchShardTarget findNext(final SearchShardIterator shardIt, Exception e, ClusterState clusterState) {
        SearchShardTarget next = shardIt.nextOrNull();
        while (next != null && shardInNodeWithZeroWeightedRouting(next.getNodeId(), clusterState)) {

            if (canFailOpen(e)) {
                logFailOpen(next.getShardId());
                break;
            }
            next = shardIt.nextOrNull();
        }
        return next;
    }

    /**
     * This function returns next shard copy to retry search request in case of failure from previous copy returned
     * by the iterator. It has the logic to fail open ie request shard copies present in nodes with weighted shard
     * routing weight set to zero
     *
     * @param shardsIt Shard Iterator containing order in which shard copies for a shard need to be requested
     * @param failure exception causing failure for previous shard copy
     * @param clusterState the cluster state
     * @return the next shard copy
     */
    public static ShardRouting findNext(final ShardsIterator shardsIt, Exception failure, ClusterState clusterState) {
        ShardRouting next = shardsIt.nextOrNull();

        while (next != null && shardInNodeWithZeroWeightedRouting(next.currentNodeId(), clusterState)) {
            if (canFailOpen(failure)) {
                logFailOpen(next.shardId());
                break;
            }
            next = shardsIt.nextOrNull();
        }
        return next;
    }

    private static void logFailOpen(ShardId shardID) {
        logger.info(() -> new ParameterizedMessage("{}: Fail open executed", shardID));
    }

    /**
     * *
     * @param e exception causing failure for previous shard copy
     * @return true if can fail open ie request shard copies present in nodes with weighted shard
     * routing weight set to zero
     */
    private static boolean canFailOpen(Exception e) {
        return isInternalFailure(e);
    }

}
