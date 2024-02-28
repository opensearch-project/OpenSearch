/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache implementation of transport actions returning single shard related data in the response.
 *
 * @param <K> Response type of transport action.
 */
public class ShardCache<K extends BaseNodeResponse> extends BaseShardCache<K> {

    private final Map<String, NodeEntry<K>> cache = new HashMap<>();

    public ShardCache(Logger logger, String logKey, String type) {
        super(logger, logKey, type);
    }

    @Override
    public void initData(DiscoveryNode node) {
        cache.put(node.getId(), new NodeEntry<>(node.getId()));
    }

    @Override
    public void putData(DiscoveryNode node, K response) {
        cache.get(node.getId()).doneFetching(response);
    }

    @Override
    public K getData(DiscoveryNode node) {
        return cache.get(node.getId()).getValue();
    }

    @Override
    public Map<String, ? extends BaseNodeEntry> getCache() {
        return cache;
    }

    @Override
    public void clearShardCache(ShardId shardId) {
        cache.clear();
    }

    @Override
    public List<ShardId> getFailedShards() {
        // Single shard cache does not need to return that shard itself because handleFailure will take care of retries
        return Collections.emptyList();
    }

    /**
     * A node entry, holding the state of the fetched data for a specific shard
     * for a giving node.
     */
    static class NodeEntry<T extends BaseNodeResponse> extends BaseShardCache.BaseNodeEntry {
        @Nullable
        private T value;

        void doneFetching(T value) {
            super.doneFetching();
            this.value = value;
        }

        NodeEntry(String nodeId) {
            super(nodeId);
        }

        T getValue() {
            return value;
        }

    }
}
