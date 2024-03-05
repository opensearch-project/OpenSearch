/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;

import java.util.Map;

import reactor.util.annotation.NonNull;

/**
 * Store node level responses of transport actions like {@link TransportNodesListGatewayStartedShards} or
 * {@link TransportNodesListShardStoreMetadata} using the given functionalities.
 * <p>
 * initData : how to initialize an entry of shard cache for a node.
 * putData : how to store the response of transport action in the cache.
 * getData : how to populate the stored data for any shard allocators like {@link PrimaryShardAllocator} or
 * {@link ReplicaShardAllocator}
 *
 * @param <K> Response type of transport action which has the data to be stored in the cache.
 */
public interface NodeCache<K extends BaseNodeResponse> {

    /**
     * Initialize cache's entry for a node.
     *
     * @param node for which node we need to initialize the cache.
     */
    void initData(DiscoveryNode node);

    /**
     * Store the response in the cache from node.
     *
     * @param node     node from which we got the response.
     * @param response shard metadata coming from node.
     */
    void putData(DiscoveryNode node, K response);

    /**
     * Populate the response from cache.
     *
     * @param node node for which we need the response.
     * @return actual response.
     */
    K getData(DiscoveryNode node);

    /**
     * Get actual map object of the cache
     *
     * @return map of nodeId and NodeEntry extending BaseNodeEntry
     */
    @NonNull
    Map<String, ? extends BaseShardCache.BaseNodeEntry> getCache();

    /**
     * Cleanup cached data for this shard once it's started. Cleanup only happens at shard level. Node entries will
     * automatically be cleaned up once shards are assigned.
     *
     * @param shardId for which we need to free up the cached data.
     */
    void deleteData(ShardId shardId);
}
