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
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardIterator;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class contains logic to find next shard to retry search request in case of failure from other shard copy.
 * This decides if retryable shard search requests can be tried on shard copies present in data
 * nodes whose attribute value weight for weighted shard routing is set to zero.
 */

public class FailOpenRouting {

    private static final Logger logger = LogManager.getLogger(FailOpenRouting.class);

    /* exception causing failure for previous shard copy */
    private Exception exception;

    private ClusterState clusterState;

    public FailOpenRouting(Exception e, ClusterState clusterState) {
        this.exception = e;
        this.clusterState = clusterState;
    }

    /**
     * *
     * @return true if exception is due to cluster availability issues
     */
    private boolean isInternalFailure() {
        if (exception instanceof OpenSearchException) {
            return ((OpenSearchException) exception).status().getStatus() / 100 == 5;
        }
        return false;
    }

    /**
     * This function checks if the shard is present in data node with weighted routing weight set to 0,
     * In such cases we fail open, if shard search request for the shard from other shard copies fail with non
     * retryable exception.
     *
     * @param nodeId the node with the shard copy
     * @return true if the node has attribute value with shard routing weight set to zero, else false
     */
    private boolean shardInNodeWithZeroWeightedRouting(String nodeId) {
        DiscoveryNode node = clusterState.nodes().get(nodeId);
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

                for (Object key : keys.toArray()) {
                    if (node.getAttributes().get(weightedRouting.attributeName()).equals(key.toString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * This function returns next shard copy to retry search request in case of failure from previous copy returned
     * by the iterator. It has the logic to fail open ie request shard copies present in nodes with weighted shard
     * routing weight set to zero
     *
     * @param shardIt Shard Iterator containing order in which shard copies for a shard need to be requested
     * @return the next shard copy
     */
    public SearchShardTarget findNext(final SearchShardIterator shardIt) {
        SearchShardTarget next = shardIt.nextOrNull();
        while (next != null && shardInNodeWithZeroWeightedRouting(next.getNodeId())) {

            if (canFailOpen(next.getShardId())) {
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
     * @return the next shard copy
     */
    public ShardRouting findNext(final ShardsIterator shardsIt) {
        ShardRouting next = shardsIt.nextOrNull();

        while (next != null && shardInNodeWithZeroWeightedRouting(next.currentNodeId())) {
            if (canFailOpen(next.shardId())) {
                logFailOpen(next.shardId());
                break;
            }
            next = shardsIt.nextOrNull();
        }
        return next;
    }

    private void logFailOpen(ShardId shardID) {
        logger.info(() -> new ParameterizedMessage("{}: Fail open executed", shardID));
    }

    /**
     * *
     * @return true if can fail open ie request shard copies present in nodes with weighted shard
     * routing weight set to zero
     */
    private boolean canFailOpen(ShardId shardId) {
        return isInternalFailure() || hasInActiveShardCopies(shardId);
    }

    private boolean hasInActiveShardCopies(ShardId shardId) {
        List<ShardRouting> shards = clusterState.routingTable().shardRoutingTable(shardId).shards();
        for (ShardRouting shardRouting : shards) {
            if (!shardRouting.active()) {
                return true;
            }
        }
        return false;
    }

}
