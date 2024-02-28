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
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.ReceiveTimeoutTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import reactor.util.annotation.NonNull;

/**
 * Common functionalities of a cache for storing shard metadata. Cache maintains node level responses.
 * Setting up the cache is required from implementation class. While set up, we need 3 functionalities from the user.
 * initData : how to initialize an entry of shard cache for a node.
 * putData : how to store the response of transport action in the cache.
 * getData : how to populate the stored data for any shard allocators like {@link PrimaryShardAllocator} or
 * {@link ReplicaShardAllocator}
 *
 * @param <K> Response type of transport action which has the data to be stored in the cache.
 */
public abstract class BaseShardCache<K extends BaseNodeResponse> {
    private final Logger logger;
    private final String logKey;
    private final String type;

    protected BaseShardCache(Logger logger, String logKey, String type) {
        this.logger = logger;
        this.logKey = logKey;
        this.type = type;
    }

    /**
     * Initialize cache's entry for a node.
     *
     * @param node for which node we need to initialize the cache.
     */
    public abstract void initData(DiscoveryNode node);

    /**
     * Store the response in the cache from node.
     *
     * @param node     node from which we got the response.
     * @param response shard metadata coming from node.
     */
    public abstract void putData(DiscoveryNode node, K response);

    /**
     * Populate the response from cache.
     *
     * @param node node for which we need the response.
     * @return actual response.
     */
    public abstract K getData(DiscoveryNode node);

    /**
     * Provide the list of shards which got failures, these shards should be removed
     * @return list of failed shards
     */
    public abstract List<ShardId> getFailedShards();


    @NonNull
    public abstract Map<String, ? extends BaseNodeEntry> getCache();

    public abstract void clearShardCache(ShardId shardId);

    /**
     * Returns the number of fetches that are currently ongoing.
     */
    public int getInflightFetches() {
        int count = 0;
        for (BaseNodeEntry nodeEntry : getCache().values()) {
            if (nodeEntry.isFetching()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Fills the shard fetched data with new (data) nodes and a fresh NodeEntry, and removes from
     * it nodes that are no longer part of the state.
     */
    public void fillShardCacheWithDataNodes(DiscoveryNodes nodes) {
        // verify that all current data nodes are there
        for (final DiscoveryNode node : nodes.getDataNodes().values()) {
            if (getCache().containsKey(node.getId()) == false) {
                initData(node);
            }
        }
        // remove nodes that are not longer part of the data nodes set
        getCache().keySet().removeIf(nodeId -> !nodes.nodeExists(nodeId));
    }

    /**
     * Finds all the nodes that need to be fetched. Those are nodes that have no
     * data, and are not in fetch mode.
     */
    public List<String> findNodesToFetch() {
        List<String> nodesToFetch = new ArrayList<>();
        for (BaseNodeEntry nodeEntry : getCache().values()) {
            if (nodeEntry.hasData() == false && nodeEntry.isFetching() == false) {
                nodesToFetch.add(nodeEntry.getNodeId());
            }
        }
        return nodesToFetch;
    }

    /**
     * Are there any nodes that are fetching data?
     */
    public boolean hasAnyNodeFetching() {
        for (BaseNodeEntry nodeEntry : getCache().values()) {
            if (nodeEntry.isFetching()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the data from cache, ignore the failed entries. Use getData functional interface to get the data, as
     * different implementations may have different ways to populate the data from cache.
     *
     * @param nodes       Discovery nodes for which we need to return the cache data.
     * @param failedNodes return failedNodes with the nodes where fetch has failed.
     * @return Map of cache data for every DiscoveryNode.
     */
    public Map<DiscoveryNode, K> populateCache(DiscoveryNodes nodes, Set<String> failedNodes) {
        Map<DiscoveryNode, K> fetchData = new HashMap<>();
        for (Iterator<? extends Map.Entry<String, ? extends BaseNodeEntry>> it = getCache().entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, BaseNodeEntry> entry = (Map.Entry<String, BaseNodeEntry>) it.next();
            String nodeId = entry.getKey();
            BaseNodeEntry nodeEntry = entry.getValue();

            DiscoveryNode node = nodes.get(nodeId);
            if (node != null) {
                if (nodeEntry.isFailed()) {
                    // if its failed, remove it from the list of nodes, so if this run doesn't work
                    // we try again next round to fetch it again
                    it.remove();
                    failedNodes.add(nodeEntry.getNodeId());
                } else {
                    K nodeResponse = getData(node);
                    if (nodeResponse != null) {
                        fetchData.put(node, nodeResponse);
                    }
                }
            }
        }
        return fetchData;
    }

    public void processResponses(List<K> responses, long fetchingRound) {
        for (K response : responses) {
            BaseNodeEntry nodeEntry = getCache().get(response.getNode().getId());
            if (nodeEntry != null) {
                if (validateNodeResponse(nodeEntry, fetchingRound)) {
                    // if the entry is there, for the right fetching round and not marked as failed already, process it
                    logger.trace("{} marking {} as done for [{}], result is [{}]", logKey, nodeEntry.getNodeId(), type, response);
                    putData(response.getNode(), response);
                }
            }
        }
    }

    public boolean validateNodeResponse(BaseNodeEntry nodeEntry, long fetchingRound) {
        if (nodeEntry.getFetchingRound() != fetchingRound) {
            assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
            logger.trace(
                "{} received response for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                logKey,
                nodeEntry.getNodeId(),
                type,
                nodeEntry.getFetchingRound(),
                fetchingRound
            );
            return false;
        } else if (nodeEntry.isFailed()) {
            logger.trace("{} node {} has failed for [{}] (failure [{}])", logKey, nodeEntry.getNodeId(), type, nodeEntry.getFailure());
            return false;
        }
        return true;
    }

    public void handleNodeFailure(BaseNodeEntry nodeEntry, FailedNodeException failure, long fetchingRound) {
        if (nodeEntry.getFetchingRound() != fetchingRound) {
            assert nodeEntry.getFetchingRound() > fetchingRound : "node entries only replaced by newer rounds";
            logger.trace(
                "{} received failure for [{}] from node {} for an older fetching round (expected: {} but was: {})",
                logKey,
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
                    () -> new ParameterizedMessage("{}: failed to list shard for {} on node [{}]", logKey, type, failure.nodeId()),
                    failure
                );
                nodeEntry.doneFetching(failure.getCause());
            }
        }
    }

    public void processFailures(List<FailedNodeException> failures, long fetchingRound) {
        for (FailedNodeException failure : failures) {
            logger.trace("{} processing failure {} for [{}]", logKey, failure, type);
            BaseNodeEntry nodeEntry = getCache().get(failure.nodeId());
            if (nodeEntry != null) {
                handleNodeFailure(nodeEntry, failure, fetchingRound);
            }
        }
    }

    public void remove(String nodeId) {
        this.getCache().remove(nodeId);
    }

    public void markAsFetching(List<String> nodeIds, long fetchingRound) {
        for (String nodeId : nodeIds) {
            getCache().get(nodeId).markAsFetching(fetchingRound);
        }
    }

    /**
     * A node entry, holding only node level fetching related information.
     * Actual metadata of shard is stored in child classes.
     */
    static class BaseNodeEntry {
        private final String nodeId;
        private boolean fetching;
        private boolean valueSet;
        private Throwable failure;
        private long fetchingRound;

        BaseNodeEntry(String nodeId) {
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

        void doneFetching() {
            assert fetching : "setting value but not in fetching mode";
            assert failure == null : "setting value when failure already set";
            this.valueSet = true;
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

        long getFetchingRound() {
            return fetchingRound;
        }
    }
}
