/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.tiering.TieringValidationResult;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;

/**
 * Validator class to validate the tiering requests of the index
 * @opensearch.experimental
 */
public class TieringRequestValidator {

    private static final Logger logger = LogManager.getLogger(TieringRequestValidator.class);

    /**
     * Validates the tiering request for indices going from hot to warm tier
     *
     * @param currentState current cluster state
     * @param concreteIndices set of indices to be validated
     * @param clusterInfo the current nodes usage info for the cluster
     * @param diskThresholdSettings the disk threshold settings of the cluster
     * @return result of the validation
     */
    public static TieringValidationResult validateHotToWarm(
        final ClusterState currentState,
        final Set<Index> concreteIndices,
        final ClusterInfo clusterInfo,
        final DiskThresholdSettings diskThresholdSettings
    ) {
        final String indexNames = concreteIndices.stream().map(Index::getName).collect(Collectors.joining(", "));
        validateSearchNodes(currentState, indexNames);
        validateDiskThresholdWaterMarkNotBreached(currentState, clusterInfo, diskThresholdSettings, indexNames);

        final TieringValidationResult tieringValidationResult = new TieringValidationResult(concreteIndices);

        for (Index index : concreteIndices) {
            if (!validateHotIndex(currentState, index)) {
                tieringValidationResult.addToRejected(index, "index is not in the HOT tier");
                continue;
            }
            if (!validateRemoteStoreIndex(currentState, index)) {
                tieringValidationResult.addToRejected(index, "index is not backed up by the remote store");
                continue;
            }
            if (!validateOpenIndex(currentState, index)) {
                tieringValidationResult.addToRejected(index, "index is closed");
                continue;
            }
            if (!validateIndexHealth(currentState, index)) {
                tieringValidationResult.addToRejected(index, "index is red");
                continue;
            }
        }

        validateEligibleNodesCapacity(clusterInfo, currentState, tieringValidationResult);
        logger.info(
            "Successfully accepted indices for tiering are [{}], rejected indices are [{}]",
            tieringValidationResult.getAcceptedIndices(),
            tieringValidationResult.getRejectedIndices()
        );

        return tieringValidationResult;
    }

    /**
     * Validates that there are eligible nodes with the search role in the current cluster state.
     * (only for the dedicated case - to be removed later)
     *
     * @param currentState the current cluster state
     * @param indexNames the names of the indices being validated
     * @throws IllegalArgumentException if there are no eligible search nodes in the cluster
     */
    static void validateSearchNodes(final ClusterState currentState, final String indexNames) {
        if (getEligibleNodes(currentState).isEmpty()) {
            final String errorMsg = "Rejecting tiering request for indices ["
                + indexNames
                + "] because there are no nodes found with the search role";
            logger.warn(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
    }

    /**
     * Validates that the specified index has the remote store setting enabled.
     *
     * @param state the current cluster state
     * @param index the index to be validated
     * @return true if the remote store setting is enabled for the index, false otherwise
     */
    static boolean validateRemoteStoreIndex(final ClusterState state, final Index index) {
        return IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(state.metadata().getIndexSafe(index).getSettings());
    }

    /**
     * Validates that the specified index is in the "hot" tiering state.
     *
     * @param state the current cluster state
     * @param index the index to be validated
     * @return true if the index is in the "hot" tiering state, false otherwise
     */
    static boolean validateHotIndex(final ClusterState state, final Index index) {
        return IndexModule.TieringState.HOT.name().equals(INDEX_TIERING_STATE.get(state.metadata().getIndexSafe(index).getSettings()));
    }

    /**
     * Validates the health of the specified index in the current cluster state.
     *
     * @param currentState the current cluster state
     * @param index the index to be validated
     * @return true if the index health is not in the "red" state, false otherwise
     */
    static boolean validateIndexHealth(final ClusterState currentState, final Index index) {
        final IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
        final IndexMetadata indexMetadata = currentState.metadata().index(index);
        final ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);
        return !ClusterHealthStatus.RED.equals(indexHealth.getStatus());
    }

    /**
     * Validates that the specified index is in the open state in the current cluster state.
     *
     * @param currentState the current cluster state
     * @param index the index to be validated
     * @return true if the index is in the open state, false otherwise
     */
    static boolean validateOpenIndex(final ClusterState currentState, final Index index) {
        return currentState.metadata().index(index).getState() == IndexMetadata.State.OPEN;
    }

    /**
     * Validates that the disk threshold low watermark is not breached on all the eligible nodes in the cluster.
     *
     * @param currentState the current cluster state
     * @param clusterInfo the current nodes usage info for the cluster
     * @param diskThresholdSettings the disk threshold settings of the cluster
     * @param indexNames the names of the indices being validated
     * @throws IllegalArgumentException if the disk threshold low watermark is breached on all eligible nodes
     */
    static void validateDiskThresholdWaterMarkNotBreached(
        final ClusterState currentState,
        final ClusterInfo clusterInfo,
        final DiskThresholdSettings diskThresholdSettings,
        final String indexNames
    ) {
        final Map<String, DiskUsage> usages = clusterInfo.getNodeLeastAvailableDiskUsages();
        if (usages == null) {
            logger.trace("skipping monitor as no disk usage information is available");
            return;
        }
        final Set<String> nodeIds = getEligibleNodes(currentState).stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        for (String node : nodeIds) {
            final DiskUsage nodeUsage = usages.get(node);
            if (nodeUsage != null && nodeUsage.getFreeBytes() > diskThresholdSettings.getFreeBytesThresholdLow().getBytes()) {
                return;
            }
        }
        throw new IllegalArgumentException(
            "Disk threshold low watermark is breached on all the search nodes, rejecting tiering request for indices: " + indexNames
        );
    }

    /**
     * Validates the capacity of eligible nodes in the cluster to accommodate the specified indices
     * and adds the rejected indices to tieringValidationResult
     *
     * @param clusterInfo the current nodes usage info for the cluster
     * @param currentState the current cluster state
     * @param tieringValidationResult contains the indices to validate
     */
    static void validateEligibleNodesCapacity(
        final ClusterInfo clusterInfo,
        final ClusterState currentState,
        final TieringValidationResult tieringValidationResult
    ) {

        final Set<String> eligibleNodeIds = getEligibleNodes(currentState).stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        long totalAvailableBytesInWarmTier = getTotalAvailableBytesInWarmTier(
            clusterInfo.getNodeLeastAvailableDiskUsages(),
            eligibleNodeIds
        );

        Map<Index, Long> indexSizes = new HashMap<>();
        for (Index index : tieringValidationResult.getAcceptedIndices()) {
            indexSizes.put(index, getIndexPrimaryStoreSize(currentState, clusterInfo, index.getName()));
        }

        if (indexSizes.values().stream().mapToLong(Long::longValue).sum() < totalAvailableBytesInWarmTier) {
            return;
        }
        HashMap<Index, Long> sortedIndexSizes = indexSizes.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, HashMap::new));

        long requestIndexBytes = 0L;
        for (Index index : sortedIndexSizes.keySet()) {
            requestIndexBytes += sortedIndexSizes.get(index);
            if (requestIndexBytes >= totalAvailableBytesInWarmTier) {
                tieringValidationResult.addToRejected(index, "insufficient node capacity");
            }
        }
    }

    /**
     * Calculates the total size of the specified index in the cluster.
     * Note: This function only accounts for the primary shard size.
     *
     * @param clusterState the current state of the cluster
     * @param clusterInfo  the current nodes usage info for the cluster
     * @param index        the name of the index for which the total size is to be calculated
     * @return the total size of the specified index in the cluster
     */
    static long getIndexPrimaryStoreSize(ClusterState clusterState, ClusterInfo clusterInfo, String index) {
        long totalIndexSize = 0;
        List<ShardRouting> shardRoutings = clusterState.routingTable().allShards(index);
        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting.primary()) {
                totalIndexSize += clusterInfo.getShardSize(shardRouting, 0);
            }
        }
        return totalIndexSize;
    }

    /**
     * Calculates the total available bytes in the warm tier of the cluster.
     *
     * @param usages the current disk usage of the cluster
     * @param nodeIds the set of warm nodes ids in the cluster
     * @return the total available bytes in the warm tier
     */
    static long getTotalAvailableBytesInWarmTier(final Map<String, DiskUsage> usages, final Set<String> nodeIds) {
        long totalAvailableBytes = 0;
        for (String node : nodeIds) {
            totalAvailableBytes += usages.get(node).getFreeBytes();
        }
        return totalAvailableBytes;
    }

    /**
     * Retrieves the set of eligible(search) nodes from the current cluster state.
     *
     * @param currentState the current cluster state
     * @return the set of eligible nodes
     */
    static Set<DiscoveryNode> getEligibleNodes(final ClusterState currentState) {
        final Map<String, DiscoveryNode> nodes = currentState.getNodes().getDataNodes();
        return nodes.values().stream().filter(DiscoveryNode::isSearchNode).collect(Collectors.toSet());
    }
}
