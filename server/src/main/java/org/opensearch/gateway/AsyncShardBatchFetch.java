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
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import reactor.util.annotation.NonNull;

/**
 * Implementation of AsyncShardFetch with batching support. This class is responsible for executing the fetch
 * part using the base class {@link AsyncShardFetch}. Other functionalities needed for a batch are only written here.
 * This separation also takes care of the extra generic type V which is only needed for batch
 * transport actions like {@link TransportNodesListGatewayStartedShardsBatch} and
 * {@link org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch}.
 *
 * @param <T> Response type of the transport action.
 * @param <V> Data type of shard level response.
 *
 * @opensearch.internal
 */
public abstract class AsyncShardBatchFetch<T extends BaseNodeResponse, V> extends AsyncShardFetch<T> {

    @SuppressWarnings("unchecked")
    AsyncShardBatchFetch(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardAttributesMap,
        AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
        String batchId,
        Class<V> clazz,
        V emptyShardResponse,
        Predicate<V> emptyShardResponsePredicate,
        ShardBatchResponseFactory<T, V> responseFactory,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        super(
            logger,
            type,
            shardAttributesMap,
            action,
            batchId,
            new ShardBatchCache<>(
                logger,
                type,
                shardAttributesMap,
                "BatchID=[" + batchId + "]",
                clazz,
                emptyShardResponse,
                emptyShardResponsePredicate,
                responseFactory,
                clusterManagerMetrics
            )
        );
    }

    /**
     * Remove a shard from the cache maintaining a full batch of shards. This is needed to clear the shard once it's
     * assigned or failed.
     *
     * @param shardId shardId to be removed from the batch.
     */
    public synchronized void clearShard(ShardId shardId) {
        this.shardAttributesMap.remove(shardId);
        this.cache.deleteShard(shardId);
    }

    public boolean hasEmptyCache() {
        return this.cache.getCache().isEmpty();
    }

    public AsyncShardFetchCache<T> getCache() {
        return this.cache;
    }

    /**
     * Cache implementation of transport actions returning batch of shards related data in the response.
     * Store node level responses of transport actions like {@link TransportNodesListGatewayStartedShardsBatch} or
     * {@link org.opensearch.indices.store.TransportNodesListShardStoreMetadataBatch} with memory efficient caching
     * approach. This cache class is not thread safe, all of its methods are being called from
     * {@link AsyncShardFetch} class which has synchronized blocks present to handle multiple threads.
     *
     * @param <T> Response type of transport action.
     * @param <V> Data type of shard level response.
     */
    static class ShardBatchCache<T extends BaseNodeResponse, V> extends AsyncShardFetchCache<T> {
        private final Map<String, NodeEntry<V>> cache;
        private final Map<ShardId, Integer> shardIdToArray;
        private final int batchSize;
        private final Class<V> shardResponseClass;
        private final ShardBatchResponseFactory<T, V> responseFactory;
        private final V emptyResponse;
        private final Predicate<V> emptyShardResponsePredicate;
        private final Logger logger;

        public ShardBatchCache(
            Logger logger,
            String type,
            Map<ShardId, ShardAttributes> shardAttributesMap,
            String logKey,
            Class<V> clazz,
            V emptyResponse,
            Predicate<V> emptyShardResponsePredicate,
            ShardBatchResponseFactory<T, V> responseFactory,
            ClusterManagerMetrics clusterManagerMetrics
        ) {
            super(Loggers.getLogger(logger, "_" + logKey), type, clusterManagerMetrics);
            this.batchSize = shardAttributesMap.size();
            this.emptyShardResponsePredicate = emptyShardResponsePredicate;
            cache = new HashMap<>();
            shardIdToArray = new HashMap<>();
            fillShardIdKeys(shardAttributesMap.keySet());
            this.shardResponseClass = clazz;
            this.emptyResponse = emptyResponse;
            this.logger = logger;
            this.responseFactory = responseFactory;
        }

        @Override
        @NonNull
        public Map<String, ? extends BaseNodeEntry> getCache() {
            return cache;
        }

        @Override
        public void deleteShard(ShardId shardId) {
            if (shardIdToArray.containsKey(shardId)) {
                Integer shardIdIndex = shardIdToArray.remove(shardId);
                for (String nodeId : cache.keySet()) {
                    cache.get(nodeId).clearShard(shardIdIndex);
                }
            }
        }

        @Override
        public void initData(DiscoveryNode node) {
            cache.put(node.getId(), new NodeEntry<>(node.getId(), shardResponseClass, batchSize, emptyShardResponsePredicate));
        }

        /**
         * Put the response received from data nodes into the cache.
         * Get shard level data from batch, then filter out if any shards received failures.
         * After that complete storing the data at node level and mark fetching as done.
         *
         * @param node     node from which we got the response.
         * @param response shard metadata coming from node.
         */
        @Override
        public void putData(DiscoveryNode node, T response) {
            NodeEntry<V> nodeEntry = cache.get(node.getId());
            Map<ShardId, V> batchResponse = responseFactory.getShardBatchData(response);
            nodeEntry.doneFetching(batchResponse, shardIdToArray);
        }

        @Override
        public T getData(DiscoveryNode node) {
            return this.responseFactory.getNewResponse(node, getBatchData(cache.get(node.getId())));
        }

        private HashMap<ShardId, V> getBatchData(NodeEntry<V> nodeEntry) {
            V[] nodeShardEntries = nodeEntry.getData();
            boolean[] emptyResponses = nodeEntry.getEmptyShardResponse();
            HashMap<ShardId, V> shardData = new HashMap<>();
            for (Map.Entry<ShardId, Integer> shardIdEntry : shardIdToArray.entrySet()) {
                ShardId shardId = shardIdEntry.getKey();
                Integer arrIndex = shardIdEntry.getValue();
                if (emptyResponses[arrIndex]) {
                    shardData.put(shardId, emptyResponse);
                } else if (nodeShardEntries[arrIndex] != null) {
                    // ignore null responses here
                    shardData.put(shardId, nodeShardEntries[arrIndex]);
                }
            }
            return shardData;
        }

        private void fillShardIdKeys(Set<ShardId> shardIds) {
            int shardIdIndex = 0;
            for (ShardId shardId : shardIds) {
                this.shardIdToArray.putIfAbsent(shardId, shardIdIndex++);
            }
        }

        /**
         * A node entry, holding the state of the fetched data for a specific shard
         * for a giving node.
         */
        static class NodeEntry<V> extends BaseNodeEntry {
            private final V[] shardData;
            private final boolean[] emptyShardResponse; // we can not rely on null entries of the shardData array,
            // those null entries means that we need to ignore those entries. Empty responses on the other hand are
            // actually needed in allocation/explain API response. So instead of storing full empty response object
            // in cache, it's better to just store a boolean and create that object on the fly just before
            // decision-making.
            private final Predicate<V> emptyShardResponsePredicate;

            NodeEntry(String nodeId, Class<V> clazz, int batchSize, Predicate<V> emptyShardResponsePredicate) {
                super(nodeId);
                this.shardData = (V[]) Array.newInstance(clazz, batchSize);
                this.emptyShardResponse = new boolean[batchSize];
                this.emptyShardResponsePredicate = emptyShardResponsePredicate;
            }

            void doneFetching(Map<ShardId, V> shardDataFromNode, Map<ShardId, Integer> shardIdKey) {
                fillShardData(shardDataFromNode, shardIdKey);
                super.doneFetching();
            }

            void clearShard(Integer shardIdIndex) {
                this.shardData[shardIdIndex] = null;
                emptyShardResponse[shardIdIndex] = false;
            }

            V[] getData() {
                return this.shardData;
            }

            boolean[] getEmptyShardResponse() {
                return emptyShardResponse;
            }

            private void fillShardData(Map<ShardId, V> shardDataFromNode, Map<ShardId, Integer> shardIdKey) {
                for (Map.Entry<ShardId, V> shardData : shardDataFromNode.entrySet()) {
                    if (shardData.getValue() != null) {
                        ShardId shardId = shardData.getKey();
                        if (emptyShardResponsePredicate.test(shardData.getValue())) {
                            this.emptyShardResponse[shardIdKey.get(shardId)] = true;
                            this.shardData[shardIdKey.get(shardId)] = null;
                        } else {
                            this.shardData[shardIdKey.get(shardId)] = shardData.getValue();
                        }
                    }
                }
            }
        }
    }
}
