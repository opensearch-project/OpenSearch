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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of AsyncShardFetch with batching support. This class is responsible for executing the fetch
 * part using the base class {@link AsyncShardFetch}. Other functionalities needed for a batch are only written here.
 * Cleanup of failed shards is necessary in a batch and based on that a reroute should be triggered to take care of
 * those in the next run. This separation also takes care of the extra generic type V which is only needed for batch
 * transport actions like {@link TransportNodesListGatewayStartedShardsBatch}.
 *
 * @param <T> Response type of the transport action.
 * @param <V> Data type of shard level response.
 *
 * @opensearch.internal
 */
public abstract class AsyncShardBatchFetch<T extends BaseNodeResponse, V extends BaseShardResponse> extends AsyncShardFetch<T> {

    private final Consumer<ShardId> removeShardFromBatch;
    private final List<ShardId> failedShards;

    @SuppressWarnings("unchecked")
    AsyncShardBatchFetch(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardAttributesMap,
        AsyncShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
        String batchId,
        Class<V> clazz,
        BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseGetter,
        Function<T, Map<ShardId, V>> shardsBatchDataGetter,
        Supplier<V> emptyResponseBuilder,
        Consumer<ShardId> handleFailedShard
    ) {
        super(logger, type, shardAttributesMap, action, batchId);
        this.removeShardFromBatch = handleFailedShard;
        this.failedShards = new ArrayList<>();
        this.cache = new ShardBatchCache<>(
            logger,
            type,
            shardAttributesMap,
            "BatchID=[" + batchId + "]",
            clazz,
            responseGetter,
            shardsBatchDataGetter,
            emptyResponseBuilder,
            this::cleanUpFailedShards
        );
    }

    public synchronized FetchResult<T> fetchData(DiscoveryNodes nodes, Map<ShardId, Set<String>> ignoreNodes) {
        if (failedShards.isEmpty() == false) {
            // trigger a reroute if there are any shards failed, to make sure they're picked up in next run
            logger.trace("triggering another reroute for failed shards in {}", reroutingKey);
            reroute("shards-failed", "shards failed in " + reroutingKey);
        }
        return super.fetchData(nodes, ignoreNodes);
    }

    private void cleanUpFailedShards(ShardId shardId) {
        shardAttributesMap.remove(shardId);
        removeShardFromBatch.accept(shardId);
        failedShards.add(shardId);
    }

    /**
     * Remove a shard from the cache maintaining a full batch of shards. This is needed to clear the shard once it's
     * assigned or failed.
     *
     * @param shardId shardId to be removed from the batch.
     */
    public void clearShard(ShardId shardId) {
        this.shardAttributesMap.remove(shardId);
        this.cache.deleteShard(shardId);
    }

    /**
     * Cache implementation of transport actions returning batch of shards related data in the response. It'll
     *
     * @param <T> Response type of transport action.
     * @param <V> Data type of shard level response.
     */
    public static class ShardBatchCache<T extends BaseNodeResponse, V extends BaseShardResponse> extends AsyncShardFetchCache<T> {
        private final Map<String, NodeEntry<V>> cache = new HashMap<>();
        private final Map<ShardId, Integer> shardIdKey = new HashMap<>();
        private final AtomicInteger shardIdIndex = new AtomicInteger();
        private final int batchSize;
        private final Class<V> shardResponseClass;
        private final BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseConstructor;
        private final Map<Integer, ShardId> shardIdReverseKey = new HashMap<>();
        private final Function<T, Map<ShardId, V>> shardsBatchDataGetter;
        private final Supplier<V> emptyResponseBuilder;
        private final Consumer<ShardId> handleFailedShard;

        public ShardBatchCache(
            Logger logger,
            String type,
            Map<ShardId, ShardAttributes> shardAttributesMap,
            String logKey,
            Class<V> clazz,
            BiFunction<DiscoveryNode, Map<ShardId, V>, T> responseGetter,
            Function<T, Map<ShardId, V>> shardsBatchDataGetter,
            Supplier<V> emptyResponseBuilder,
            Consumer<ShardId> handleFailedShard
        ) {
            super(Loggers.getLogger(logger, "_" + logKey), type);
            this.batchSize = shardAttributesMap.size();
            fillShardIdKeys(shardAttributesMap.keySet());
            this.shardResponseClass = clazz;
            this.responseConstructor = responseGetter;
            this.shardsBatchDataGetter = shardsBatchDataGetter;
            this.emptyResponseBuilder = emptyResponseBuilder;
            this.handleFailedShard = handleFailedShard;
        }

        @Override
        public Map<String, ? extends BaseNodeEntry> getCache() {
            return cache;
        }

        @Override
        public void deleteShard(ShardId shardId) {
            if (shardIdKey.containsKey(shardId)) {
                Integer shardIdIndex = shardIdKey.remove(shardId);
                for (String nodeId : cache.keySet()) {
                    cache.get(nodeId).clearShard(shardIdIndex);
                }
            }
        }

        @Override
        public Map<DiscoveryNode, T> getCacheData(DiscoveryNodes nodes, Set<String> failedNodes) {
            refreshReverseIdMap();
            return super.getCacheData(nodes, failedNodes);
        }

        /**
         * Build a reverse map to get shardId from the array index, this will be used to construct the response which
         * PrimaryShardBatchAllocator or ReplicaShardBatchAllocator are looking for.
         */
        private void refreshReverseIdMap() {
            shardIdReverseKey.clear();
            for (ShardId shardId : shardIdKey.keySet()) {
                shardIdReverseKey.putIfAbsent(shardIdKey.get(shardId), shardId);
            }
        }

        @Override
        public void initData(DiscoveryNode node) {
            cache.put(node.getId(), new NodeEntry<>(node.getId(), shardResponseClass, batchSize));
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
            Map<ShardId, V> batchResponse = shardsBatchDataGetter.apply(response);
            filterFailedShards(batchResponse);
            nodeEntry.doneFetching(batchResponse, shardIdKey);
        }

        /**
         * Return the shard for which we got unhandled exceptions.
         *
         * @param batchResponse response from one node for the batch.
         */
        private void filterFailedShards(Map<ShardId, V> batchResponse) {
            logger.trace("filtering failed shards");
            for (Iterator<ShardId> it = batchResponse.keySet().iterator(); it.hasNext();) {
                ShardId shardId = it.next();
                if (batchResponse.get(shardId) != null) {
                    if (batchResponse.get(shardId).getException() != null) {
                        // handle per shard level exceptions, process other shards, only throw out this shard from
                        // the batch
                        Exception shardException = batchResponse.get(shardId).getException();
                        // if the request got rejected or timed out, we need to try it again next time...
                        if (retryableException(shardException)) {
                            logger.trace(
                                "got unhandled retryable exception for shard {} {}",
                                shardId.toString(),
                                shardException.toString()
                            );
                            handleFailedShard.accept(shardId);
                            // remove this failed entry. So, while storing the data, we don't need to re-process it.
                            it.remove();
                        }
                    }
                }
            }
        }

        @Override
        public T getData(DiscoveryNode node) {
            return this.responseConstructor.apply(node, getBatchData(cache.get(node.getId())));
        }

        private HashMap<ShardId, V> getBatchData(NodeEntry<V> nodeEntry) {
            V[] nodeShardEntries = nodeEntry.getData();
            boolean[] emptyResponses = nodeEntry.getEmptyShardResponse();
            HashMap<ShardId, V> shardData = new HashMap<>();
            for (Integer shardIdIndex : shardIdKey.values()) {
                if (emptyResponses[shardIdIndex]) {
                    shardData.put(shardIdReverseKey.get(shardIdIndex), emptyResponseBuilder.get());
                } else if (nodeShardEntries[shardIdIndex] != null) {
                    // ignore null responses here
                    shardData.put(shardIdReverseKey.get(shardIdIndex), nodeShardEntries[shardIdIndex]);
                }
            }
            return shardData;
        }

        private void fillShardIdKeys(Set<ShardId> shardIds) {
            for (ShardId shardId : shardIds) {
                this.shardIdKey.putIfAbsent(shardId, shardIdIndex.getAndIncrement());
            }
            this.shardIdKey.keySet().removeIf(shardId -> {
                if (!shardIds.contains(shardId)) {
                    deleteShard(shardId);
                    return true;
                } else {
                    return false;
                }
            });
        }

        /**
         * A node entry, holding the state of the fetched data for a specific shard
         * for a giving node.
         */
        static class NodeEntry<V extends BaseShardResponse> extends BaseNodeEntry {
            private final V[] shardData;
            private final boolean[] emptyShardResponse;

            NodeEntry(String nodeId, Class<V> clazz, int batchSize) {
                super(nodeId);
                this.shardData = (V[]) Array.newInstance(clazz, batchSize);
                this.emptyShardResponse = new boolean[batchSize];
            }

            void doneFetching(Map<ShardId, V> shardDataFromNode, Map<ShardId, Integer> shardIdKey) {
                fillShardData(shardDataFromNode, shardIdKey);
                super.doneFetching();
            }

            void clearShard(Integer shardIdIndex) {
                this.shardData[shardIdIndex] = null;
            }

            V[] getData() {
                return this.shardData;
            }

            boolean[] getEmptyShardResponse() {
                return emptyShardResponse;
            }

            private void fillShardData(Map<ShardId, V> shardDataFromNode, Map<ShardId, Integer> shardIdKey) {
                for (ShardId shardId : shardDataFromNode.keySet()) {
                    if (shardDataFromNode.get(shardId) != null) {
                        if (shardDataFromNode.get(shardId).isEmpty()) {
                            this.emptyShardResponse[shardIdKey.get(shardId)] = true;
                            this.shardData[shardIdKey.get(shardId)] = null;
                        } else if (shardDataFromNode.get(shardId).getException() == null) {
                            this.shardData[shardIdKey.get(shardId)] = shardDataFromNode.get(shardId);
                        }
                        // if exception is not null, we got unhandled failure for the shard which needs to be ignored
                    }
                }
            }
        }
    }
}
