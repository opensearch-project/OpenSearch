/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
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
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.shard.ShardId;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for fetching shard data from nodes. It is analogous to AsyncShardFetch class since it fetches
 * the data in asynchronous manner too.
 * @param <T>
 */
public abstract class AsyncShardsFetchPerNode<T extends BaseNodeResponse> implements Releasable {

    /**
     * An action that lists the relevant shard data that needs to be fetched.
     */
    public interface Lister<NodesResponse extends BaseNodesResponse<NodeResponse>, NodeResponse extends BaseNodeResponse> {
        void list(DiscoveryNode[] nodes, Map<ShardId,String> shardsIdMap, ActionListener<NodesResponse> listener);
    }

    protected final Logger logger;
    protected final String type;
    protected Map<ShardId,String> shardsToCustomDataPathMap;
    private final AsyncShardsFetchPerNode.Lister<BaseNodesResponse<T>, T> action;
    protected final Map<String, AsyncShardsFetchPerNode.NodeEntry<T>> cache = new HashMap<>();

    private final Set<String> nodesToIgnore = new HashSet<>();
    private final AtomicLong round = new AtomicLong();
    private boolean closed;

    @SuppressWarnings("unchecked")
    protected AsyncShardsFetchPerNode(
        Logger logger,
        String type,
        Map<ShardId, String> shardsToCustomDataPathMap,
        AsyncShardsFetchPerNode.Lister<? extends BaseNodesResponse<T>, T> action
    ) {
        this.logger = logger;
        this.type = type;
        this.action = (AsyncShardsFetchPerNode.Lister<BaseNodesResponse<T>, T>) action;
        this.shardsToCustomDataPathMap = shardsToCustomDataPathMap;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    protected abstract void reroute(String reason);

    /**
     * Clear cache for node, ensuring next fetch will fetch a fresh copy.
     */
    synchronized void clearCacheForNode(String nodeId) {
        cache.remove(nodeId);
    }

    /** This function is copy-pasted from AsyncShardFetch.java
     * Fills the shard fetched data with new (data) nodes and a fresh NodeEntry, and removes from
     * it nodes that are no longer part of the state.
     */
    private void fillShardCacheWithDataNodes(Map<String, AsyncShardsFetchPerNode.NodeEntry<T>> shardCache, DiscoveryNodes nodes) {
        // verify that all current data nodes are there
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getDataNodes()) {
            DiscoveryNode node = cursor.value;
            if (shardCache.containsKey(node.getId()) == false) {
                shardCache.put(node.getId(), new AsyncShardsFetchPerNode.NodeEntry<T>(node.getId()));
            }
        }
        // remove nodes that are not longer part of the data nodes set
        shardCache.keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
    }

    /**
     * This function is copy-pasted from AsyncShardFetch.java
     * Finds all the nodes that need to be fetched. Those are nodes that have no
     * data, and are not in fetch mode.
     */
    private List<AsyncShardsFetchPerNode.NodeEntry<T>> findNodesToFetch(Map<String, AsyncShardsFetchPerNode.NodeEntry<T>> shardCache) {
        List<AsyncShardsFetchPerNode.NodeEntry<T>> nodesToFetch = new ArrayList<>();
        for (AsyncShardsFetchPerNode.NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.hasData() == false && nodeEntry.isFetching() == false) {
                nodesToFetch.add(nodeEntry);
            }
        }
        return nodesToFetch;
    }

    /**
     * This function is copy-pasted from AsyncShardFetch.java
     * Are there any nodes that are fetching data?
     */
    private boolean hasAnyNodeFetching(Map<String, AsyncShardsFetchPerNode.NodeEntry<T>> shardCache) {
        for (AsyncShardsFetchPerNode.NodeEntry<T> nodeEntry : shardCache.values()) {
            if (nodeEntry.isFetching()) {
                return true;
            }
        }
        return false;
    }

    /**
     * This function is copy-pasted from AsyncShardFetch.java, fetchData(). Here we have modified the
     * logging part for better debuggability and testing purpose
     * @param nodes
     * @return
     */
    public synchronized AsyncShardsFetchPerNode.TestFetchResult<T> testFetchData(DiscoveryNodes nodes){
        if (closed) {
            throw new IllegalStateException("TEST: can't fetch data from nodes on closed async fetch");
        }

        logger.info("TEST- Fetching Unassigned Shards per node");
        fillShardCacheWithDataNodes(cache, nodes);
        List<AsyncShardsFetchPerNode.NodeEntry<T>> nodesToFetch = findNodesToFetch(cache);
        if (nodesToFetch.isEmpty() == false) {
            // mark all node as fetching and go ahead and async fetch them
            // use a unique round id to detect stale responses in processAsyncFetch
            final long fetchingRound = round.incrementAndGet();
            for (AsyncShardsFetchPerNode.NodeEntry<T> nodeEntry : nodesToFetch) {
                nodeEntry.markAsFetching(fetchingRound);
            }
            DiscoveryNode[] discoNodesToFetch = nodesToFetch.stream()
                .map(AsyncShardsFetchPerNode.NodeEntry::getNodeId)
                .map(nodes::get)
                .toArray(DiscoveryNode[]::new);
            asyncFetchShardPerNode(discoNodesToFetch, fetchingRound);
        }

        if (hasAnyNodeFetching(cache)) {
            return new AsyncShardsFetchPerNode.TestFetchResult<>( null);
        } else {
            // nothing to fetch, yay, build the return value
            Map<DiscoveryNode, T> fetchData = new HashMap<>();
            Set<String> failedNodes = new HashSet<>();
            for (Iterator<Map.Entry<String, AsyncShardsFetchPerNode.NodeEntry<T>>> it = cache.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, AsyncShardsFetchPerNode.NodeEntry<T>> entry = it.next();
                String nodeId = entry.getKey();
                AsyncShardsFetchPerNode.NodeEntry<T> nodeEntry = entry.getValue();

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

            // if at least one node failed, make sure to have a protective reroute
            // here, just case this round won't find anything, and we need to retry fetching data
            if (failedNodes.isEmpty() == false ) {
                reroute("TEST--> nodes failed [" + failedNodes.size() );
            }

            return new AsyncShardsFetchPerNode.TestFetchResult<>(fetchData);
        }
    }

    /** This function is copy-pasted from AsyncShardFetch.java (asyncFetch()), with more verbose logging
     * Async fetches data for the provided shard with the set of nodes that need to be fetched from.
     */
    void asyncFetchShardPerNode(final DiscoveryNode[] nodes, long fetchingRound) {
        logger.info("Fetching Unassigned Shards per node");
        action.list(nodes, shardsToCustomDataPathMap, new ActionListener<BaseNodesResponse<T>>() {
            @Override
            public void onResponse(BaseNodesResponse<T> tBaseNodesResponse) {
                processTestAsyncFetch(tBaseNodesResponse.getNodes(),tBaseNodesResponse.failures(), fetchingRound);
            }

            @Override
            public void onFailure(Exception e) {

                List<FailedNodeException> failures = new ArrayList<>(nodes.length);
                for (final DiscoveryNode node : nodes) {
                    failures.add(new FailedNodeException(node.getId(), "Total failure in fetching", e));
                }
                processTestAsyncFetch(null, failures, fetchingRound);
            }
        });
    }


    /** This function is copy-pasted from AsyncShardFetch.java (processAsyncFetch()), with more verbose logging.
     *
     * Called by the response handler of the async action to fetch data. Verifies that its still working
     * on the same cache generation, otherwise the results are discarded. It then goes and fills the relevant data for
     * the shard (response + failures), issuing a reroute at the end of it to make sure there will be another round
     * of allocations taking this new data into account.
     */
    protected synchronized void processTestAsyncFetch(List<T> responses, List<FailedNodeException> failures, long fetchingRound){
        if (closed) {
            // we are closed, no need to process this async fetch at all
            logger.trace("TEST-Ignoring fetched [{}] results, already closed", type);
            return;
        }

        logger.trace("TEST-processing fetched results");

        if (responses != null) {
            for (T response : responses) {
                AsyncShardsFetchPerNode.NodeEntry<T> nodeEntry = cache.get(response.getNode().getId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.info(
                            "TEST--> received response for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                            nodeEntry.getNodeId(),
                            type,
                            nodeEntry.getFetchingRound(),
                            fetchingRound
                        );
                    } else if (nodeEntry.isFailed()) {
                        logger.info(
                            "node {} has failed for [{}] (failure [{}])",
                            nodeEntry.getNodeId(),
                            type,
                            nodeEntry.getFailure()
                        );
                    } else {
                        // if the entry is there, for the right fetching round and not marked as failed already, process it
                        logger.info("TEST--> marking {} as done for [{}], result is [{}]", nodeEntry.getNodeId(), type, response);
                        nodeEntry.doneFetching(response);
                    }
                }
            }
        }
        if (failures != null) {
            for (FailedNodeException failure : failures) {
                logger.trace("processing failure {} for [{}]", failure, type);
                AsyncShardsFetchPerNode.NodeEntry<T> nodeEntry = cache.get(failure.nodeId());
                if (nodeEntry != null) {
                    if (nodeEntry.getFetchingRound() != fetchingRound) {
                        assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
                        logger.trace(
                            "received failure for [{}] from node {} for an older fetching round (expected: {} but was: {})",
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
                                    "failed to list shard for {} on node [{}]",
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

        reroute("TEST_post_response");
    }

    protected synchronized void updateBatchOfShards(Map<ShardId, String> shardsToCustomDataPathMap){

        // update only when current batch is completed
        if(hasAnyNodeFetching(cache)==false && shardsToCustomDataPathMap.isEmpty()==false){
           this.shardsToCustomDataPathMap= shardsToCustomDataPathMap;

           // not intelligent enough right now to invalidate the diff.
            // When batching the diff we can make it more intelligent
           cache.values().forEach(NodeEntry::invalidateCurrentData);
        }
    }

    /**
     * Analogous to FetchResult in AsyncShardFetch.java, but currently we dont accommodate ignoreNodes
     * @param <T>
     */
    public static class TestFetchResult<T extends BaseNodeResponse> {

        private final Map<DiscoveryNode, T> nodesToShards;

        public TestFetchResult(Map<DiscoveryNode, T> nodesToShards) {
            this.nodesToShards = nodesToShards;
        }

        public Map<DiscoveryNode, T> getNodesToShards() {
            return nodesToShards;
        }

        public boolean hasData() {
            return nodesToShards != null;
        }

    }


    /**
     * A node entry, holding the state of the fetched data for a batch of shards
     * for a giving node.
     *
     * It is analogous to NodeEntry in AsyncShardFetch.java
     */
    static class NodeEntry<T> {

        /* Copied and derived from AsyncShardFetch.java. Starts*/
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
        /* Copied and derived from AsyncShardFetch.java. Ends*/

        void invalidateCurrentData() {
            this.value=null;
            valueSet=false;
            fetchingRound=0;
            failure=null;
        }
    }


}
