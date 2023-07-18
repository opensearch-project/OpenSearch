/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.transport.ReceiveTimeoutTransportException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;

public abstract class AsyncBatchShardFetch<T extends BaseNodeResponse> implements Releasable {

    /**
     * An action that lists the relevant shard data that needs to be fetched.
     */
    public interface Lister<NodesResponse extends BaseNodesResponse<NodeResponse>, NodeResponse extends BaseNodeResponse> {
        void list(DiscoveryNode[] nodes,Map<ShardId, String> shardIdsWithCustomDataPath, ActionListener<NodesResponse> listener);
    }

    protected final Logger logger;
    protected final String type;
    private final String batchUUID;
    protected Map<ShardId,String> shardsToCustomDataPathMap;
    private Map<ShardId, Set<String>> ignoredShardToNodes = new HashMap<>();
    private final AsyncBatchShardFetch.Lister<BaseNodesResponse<T>, T> action;
    private final Map<String, AsyncBatchShardFetch.NodeEntry<T>> cache = new HashMap<>();
    private final Set<String> nodesToIgnore = new HashSet<>();
    private final AtomicLong round = new AtomicLong();
    private boolean closed;

    @SuppressWarnings("unchecked")
    protected AsyncBatchShardFetch(
        Logger logger,
        String type,
        Map<ShardId, String> shardsToCustomDataPathMap,
        AsyncBatchShardFetch.Lister<? extends BaseNodesResponse<T>, T> action,
        String batchUUID
    ) {
        this.logger = logger;
        this.type = type;
        this.shardsToCustomDataPathMap = shardsToCustomDataPathMap;
        this.action = (AsyncBatchShardFetch.Lister<BaseNodesResponse<T>, T>) action;
        this.batchUUID = batchUUID;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    /**
     * Returns the number of async fetches that are currently ongoing.
     */
    public synchronized int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncBatchShardFetch.NodeEntry<T> nodeEntry : cache.values()) {
            if (nodeEntry.isFetching()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Fetches the data for the relevant shard. If there any ongoing async fetches going on, or new ones have
     * been initiated by this call, the result will have no data.
     * <p>
     * The ignoreNodes are nodes that are supposed to be ignored for this round, since fetching is async, we need
     * to keep them around and make sure we add them back when all the responses are fetched and returned.
     */
    public synchronized AsyncBatchShardFetch.FetchResult<T> fetchData(DiscoveryNodes nodes, Map<ShardId, Set<String>> ignoreNodes) {
        if (closed) {
            throw new IllegalStateException(batchUUID + ": can't fetch data on closed async fetch");
        }

        // create a flat map for all ignore nodes in a batch.
        // This information is only used for retrying. Fetching is still a broadcast to all available nodes
        nodesToIgnore.addAll(ignoreNodes.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()));
        fillShardCacheWithDataNodes(cache, nodes);
        List<AsyncBatchShardFetch.NodeEntry<T>> nodesToFetch = findNodesToFetch(cache);
        if (nodesToFetch.isEmpty() == false) {
            // mark all node as fetching and go ahead and async fetch them
            // use a unique round id to detect stale responses in processAsyncFetch
            final long fetchingRound = round.incrementAndGet();
            for (AsyncBatchShardFetch.NodeEntry<T> nodeEntry : nodesToFetch) {
                nodeEntry.markAsFetching(fetchingRound);
            }
            DiscoveryNode[] discoNodesToFetch = nodesToFetch.stream()
                .map(AsyncBatchShardFetch.NodeEntry::getNodeId)
                .map(nodes::get)
                .toArray(DiscoveryNode[]::new);
            asyncFetch(discoNodesToFetch, fetchingRound);
        }

        // if we are still fetching, return null to indicate it
        if (hasAnyNodeFetching(cache)) {
            return new AsyncBatchShardFetch.FetchResult<>(null, emptyMap());
        } else {
            // nothing to fetch, yay, build the return value
            Map<DiscoveryNode, T> fetchData = new HashMap<>();
            Set<String> failedNodes = new HashSet<>();
            for (Iterator<Map.Entry<String, AsyncBatchShardFetch.NodeEntry<T>>> it = cache.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, AsyncBatchShardFetch.NodeEntry<T>> entry = it.next();
                String nodeId = entry.getKey();
                AsyncBatchShardFetch.NodeEntry<T> nodeEntry = entry.getValue();

                DiscoveryNode node = nodes.get(nodeId);
                if (node != null) {
                    if (nodeEntry.isFailed()) {
                        // if its failed, remove it from the list of nodes, so if this run doesn't work
                        // we try again next round to fetch it again
                        it.remove();
                        failedNodes.add(nodeEntry.getNodeId());
                    } else {
                        if (nodeEntry.getValue() != null) {
                            fetchData.put(node, nodeEntry.getValue());
                        }
                    }
                }
            }
            Set<String> allIgnoreNodes = unmodifiableSet(new HashSet<>(nodesToIgnore));
            // clear the nodes to ignore, we had a successful run in fetching everything we can
            // we need to try them if another full run is needed
            nodesToIgnore.clear();

            // if at least one node failed, make sure to have a protective reroute
            // here, just case this round won't find anything, and we need to retry fetching data
            if (failedNodes.isEmpty() == false || allIgnoreNodes.isEmpty() == false) {
                reroute(batchUUID, "nodes failed [" + failedNodes.size() + "], ignored [" + allIgnoreNodes.size() + "]");
            }

            return new AsyncBatchShardFetch.FetchResult<>(fetchData, ignoredShardToNodes);
        }
    }

    /**
     * Called by the response handler of the async action to fetch data. Verifies that its still working
     * on the same cache generation, otherwise the results are discarded. It then goes and fills the relevant data for
     * the shard (response + failures), issuing a reroute at the end of it to make sure there will be another round
     * of allocations taking this new data into account.
     */
    protected synchronized void processAsyncFetch(List<T> responses, List<FailedNodeException> failures, long fetchingRound) {
        if (closed) {
            // we are closed, no need to process this async fetch at all
            logger.trace("{} ignoring fetched [{}] results, already closed", batchUUID, type);
            return;
        }
        logger.trace("{} processing fetched [{}] results", batchUUID, type);

        if (responses != null) {
            for (T response : responses) {
                AsyncBatchShardFetch.NodeEntry<T> nodeEntry = cache.get(response.getNode().getId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace(
                            "{} received response for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            batchUUID,
                            nodeEntry.getNodeId(),
                            type,
                            nodeEntry.getFetchingRound(),
                            fetchingRound
                        );
                    } else if (nodeEntry.isFailed()) {
                        logger.trace(
                            "{} node {} has failed for [{}] (failure [{}])",
                            batchUUID,
                            nodeEntry.getNodeId(),
                            type,
                            nodeEntry.getFailure()
                        );
                    } else {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        logger.trace("{} marking {} as done for [{}], result is [{}]", batchUUID, nodeEntry.getNodeId(), type, response);
                        nodeEntry.doneFetching(response);
                    }
                }
            }
        }
        if (failures != null) {
            for (FailedNodeException failure : failures) {
                logger.trace("{} processing failure {} for [{}]", batchUUID, failure, type);
                AsyncBatchShardFetch.NodeEntry<T> nodeEntry = cache.get(failure.nodeId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace(
                            "{} received failure for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            batchUUID,
                            nodeEntry.getNodeId(),
                            type,
                            nodeEntry.getFetchingRound(),
                            fetchingRound
                        );
                    } else if (nodeEntry.isFailed() == false) {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        Throwable unwrappedCause = ExceptionsHelper.unwrapCause(failure.getCause());
                        // if the request got rejected or timed out, we need to try it again next time...
                        if (unwrappedCause instanceof OpenSearchRejectedExecutionException
                            || unwrappedCause instanceof ReceiveTimeoutTransportException
                            || unwrappedCause instanceof OpenSearchTimeoutException) {
                            nodeEntry.restartFetching();
                        } else {
                            logger.warn(
                                () -> new ParameterizedMessage(
                                    "{}: failed to list shard for {} on node [{}]",
                                    batchUUID,
                                    type,
                                    failure.nodeId()
                                ),
                                failure
                            );
                            nodeEntry.doneFetching(failure.getCause());
                        }
                    }
                }
            }
        }
        reroute(batchUUID, "post_response");
    }

    /**
     * Implement this in order to scheduled another round that causes a call to fetch data.
     */
    protected abstract void reroute(String batchUUID, String reason);

    /**
     * Clear cache for node, ensuring next fetch will fetch a fresh copy.
     */
    synchronized void clearCacheForNode(String nodeId) {
        cache.remove(nodeId);
    }

    /**
     * Fills the shard fetched data with new (data) nodes and a fresh NodeEntry, and removes from
     * it nodes that are no longer part of the state.
     */
    private void fillShardCacheWithDataNodes(Map<String, AsyncBatchShardFetch.NodeEntry<T>> shardCache, DiscoveryNodes nodes) {
        // verify that all current data nodes are there
        for (final DiscoveryNode node : nodes.getDataNodes().values()) {
            if (shardCache.containsKey(node.getId()) == false) {
                shardCache.put(node.getId(), new AsyncBatchShardFetch.NodeEntry<T>(node.getId()));
            }
        }
        // remove nodes that are not longer part of the data nodes set
        shardCache.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
    }

    /**
     * Finds all the nodes that need to be fetched. Those are nodes that have no
     * data, and are not in fetch mode.
     */
    private List<AsyncBatchShardFetch.NodeEntry<T>> findNodesToFetch(Map<String, AsyncBatchShardFetch.NodeEntry<T>> shardCache) {
        List<AsyncBatchShardFetch.NodeEntry<T>> nodesToFetch = new ArrayList<>();
        for (AsyncBatchShardFetch.NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.hasData() == false && nodeEntry.isFetching() == false) {
                nodesToFetch.add(nodeEntry);
            }
        }
        return nodesToFetch;
    }

    /**
     * Are there any nodes that are fetching data?
     */
    private boolean hasAnyNodeFetching(Map<String, AsyncBatchShardFetch.NodeEntry<T>> shardCache) {
        for (AsyncBatchShardFetch.NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.isFetching()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Async fetches data for the provided shard with the set of nodes that need to be fetched from.
     */
    // visible for testing
    void asyncFetch(final DiscoveryNode[] nodes, long fetchingRound) {
        logger.trace("{} fetching [{}] from {}", batchUUID, type, nodes);
        action.list(nodes, shardsToCustomDataPathMap, new ActionListener<BaseNodesResponse<T>>() {
            @Override
            public void onResponse(BaseNodesResponse<T> response) {
                processAsyncFetch(response.getNodes(), response.failures(), fetchingRound);
            }

            @Override
            public void onFailure(Exception e) {
                List<FailedNodeException> failures = new ArrayList<>(nodes.length);
                for (final DiscoveryNode node : nodes) {
                    failures.add(new FailedNodeException(node.getId(), "total failure in fetching", e));
                }
                processAsyncFetch(null, failures, fetchingRound);
            }
        });
    }

    /**
     * The result of a fetch operation. Make sure to first check {@link #hasData()} before
     * fetching the actual data.
     */
    public static class FetchResult<T extends BaseNodeResponse> {

        private final Map<DiscoveryNode, T> data;
        private Map<ShardId, Set<String>> ignoredShardToNodes = new HashMap<>();

        public FetchResult(Map<DiscoveryNode, T> data, Map<ShardId, Set<String>> ignoreNodes) {
            this.data = data;
            this.ignoredShardToNodes = ignoreNodes;
        }

        /**
         * Does the result actually contain data? If not, then there are on going fetch
         * operations happening, and it should wait for it.
         */
        public boolean hasData() {
            return data != null;
        }

        /**
         * Returns the actual data, note, make sure to check {@link #hasData()} first and
         * only use this when there is an actual data.
         */
        public Map<DiscoveryNode, T> getData() {
            assert data != null : "getData should only be called if there is data to be fetched, please check hasData first";
            return this.data;
        }

        /**
         * Process any changes needed to the allocation based on this fetch result.
         */
        public void processAllocation(RoutingAllocation allocation) {
            for(Map.Entry<ShardId, Set<String>> entry : ignoredShardToNodes.entrySet()) {
                ShardId shardId = entry.getKey();
                Set<String> ignoreNodes = entry.getValue();
                if (ignoreNodes.isEmpty() == false) {
                    ignoreNodes.forEach(nodeId -> allocation.addIgnoreShardForNode(shardId, nodeId));
                }
            }

        }
    }

    /**
     * The result of a fetch operation. Make sure to first check {@link #hasData()} before
     * fetching the actual data.
     */
    public static class AdaptedResultsForShard<T extends BaseNodeResponse> {

        private final ShardId shardId;
        private final Map<DiscoveryNode, T> data;
        private final Set<String> ignoreNodes;

        public AdaptedResultsForShard(ShardId shardId, Map<DiscoveryNode, T> data, Set<String> ignoreNodes) {
            this.shardId = shardId;
            this.data = data;
            this.ignoreNodes = ignoreNodes;
        }

        /**
         * Does the result actually contain data? If not, then there are on going fetch
         * operations happening, and it should wait for it.
         */
        public boolean hasData() {
            return data != null;
        }

        /**
         * Returns the actual data, note, make sure to check {@link #hasData()} first and
         * only use this when there is an actual data.
         */
        public Map<DiscoveryNode, T> getData() {
            assert data != null : "getData should only be called if there is data to be fetched, please check hasData first";
            return this.data;
        }
    }

    /**
     * A node entry, holding the state of the fetched data for a specific shard
     * for a giving node.
     */
    static class NodeEntry<T> {
        private final String nodeId;
        private boolean fetching;
        @Nullable
        private T value;
        private boolean valueSet;
        private Throwable failure;
        private long fetchingRound;

        NodeEntry(String nodeId) {
            this.nodeId = nodeId;
        }

        String getNodeId() {
            return this.nodeId;
        }

        boolean isFetching() {
            return fetching;
        }

        void markAsFetching(long fetchingRound) {
            assert fetching == false : "double marking a node as fetching";
            this.fetching = true;
            this.fetchingRound = fetchingRound;
        }

        void doneFetching(T value) {
            assert fetching : "setting value but not in fetching mode";
            assert failure == null : "setting value when failure already set";
            this.valueSet = true;
            this.value = value;
            this.fetching = false;
        }

        void doneFetching(Throwable failure) {
            assert fetching : "setting value but not in fetching mode";
            assert valueSet == false : "setting failure when already set value";
            assert failure != null : "setting failure can't be null";
            this.failure = failure;
            this.fetching = false;
        }

        void restartFetching() {
            assert fetching : "restarting fetching, but not in fetching mode";
            assert valueSet == false : "value can't be set when restarting fetching";
            assert failure == null : "failure can't be set when restarting fetching";
            this.fetching = false;
        }

        boolean isFailed() {
            return failure != null;
        }

        boolean hasData() {
            return valueSet || failure != null;
        }

        Throwable getFailure() {
            assert hasData() : "getting failure when data has not been fetched";
            return failure;
        }

        @Nullable
        T getValue() {
            assert failure == null : "trying to fetch value, but its marked as failed, check isFailed";
            assert valueSet : "value is not set, hasn't been fetched yet";
            return value;
        }

        long getFetchingRound() {
            return fetchingRound;
        }
    }
}
