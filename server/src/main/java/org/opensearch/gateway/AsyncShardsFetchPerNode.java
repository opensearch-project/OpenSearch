/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.shard.ShardId;

import java.util.Map;


/**
 * This class is responsible for fetching shard data from nodes. It is analogous to AsyncShardFetch class except
 * that we fetch batch of shards in this class from single transport request to a node.
 * @param <T>
 *
 * @opensearch.internal
 */
public abstract class AsyncShardsFetchPerNode<T extends BaseNodeResponse> implements Releasable {

    /**
     * An action that lists the relevant shard data that needs to be fetched.
     */
    public interface Lister<NodesResponse extends BaseNodesResponse<NodeResponse>, NodeResponse extends BaseNodeResponse> {
        void list(DiscoveryNode[] nodes, Map<ShardId, String> shardIdsWithCustomDataPath, ActionListener<NodesResponse> listener);
    }
}
