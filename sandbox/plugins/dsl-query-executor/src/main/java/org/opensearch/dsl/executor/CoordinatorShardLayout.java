/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.HashMap;
import java.util.Map;

/**
 * Reads how many shards of one index the coordinator would send to the busiest single node
 * ("S_node"), the input the fan-out width divides by.
 */
final class CoordinatorShardLayout {

    private static final Logger logger = LogManager.getLogger(CoordinatorShardLayout.class);

    private CoordinatorShardLayout() {}

    /**
     * Shards of {@code concreteIndex} on the node carrying the most of them, per the given cluster-state
     * snapshot.
     *
     * @param state the one cluster-state snapshot the whole request is planned against
     * @param routing the coordinator's operation routing
     * @param concreteIndex the resolved concrete index name
     * @return the busiest node's shard count, never 0 — an index whose shards are all unassigned, or a
     *         routing read that fails outright, yields 1, because a wrong fan-out width must never fail
     *         a search
     */
    static int shardsOnBusiestNode(ClusterState state, OperationRouting routing, String concreteIndex) {
        try {
            GroupShardsIterator<ShardIterator> groups = routing.searchShards(state, new String[] { concreteIndex }, null, null);
            Map<String, Integer> perNode = new HashMap<>();
            for (ShardIterator shardIt : groups) {
                // Consuming this iterator is safe: searchShards builds a fresh GroupShardsIterator per
                // call and nothing else sees ours. Never hand a partly-consumed one on.
                ShardRouting shard = shardIt.nextOrNull();
                if (shard == null || shard.currentNodeId() == null) {
                    // Unassigned or otherwise unplaceable: skip it, never throw. Mirrors the engine's own
                    // shard-target resolver, which skips on either null.
                    continue;
                }
                perNode.merge(shard.currentNodeId(), 1, Integer::sum);
            }
            return perNode.values().stream().mapToInt(Integer::intValue).max().orElse(1);
        } catch (RuntimeException e) {
            // Fail secure into the narrowest-fan-out direction rather than onto the query path: this value
            // only advises how many sub-plans may run at once, so a routing read that throws (a
            // concurrently deleted index is the realistic one) must degrade, not turn a valid _search into
            // an error.
            logger.debug(
                "the shard layout of [{}] could not be read [{}]; treating the busiest node as holding one shard",
                concreteIndex,
                e
            );
            return 1;
        }
    }
}
