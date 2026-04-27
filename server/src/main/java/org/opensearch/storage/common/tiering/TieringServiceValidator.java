/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingPool;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.DiskThresholdEvaluator;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.storage.common.tiering.TieringRejectionException.RejectionReason;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.TieringState.HOT;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM_TO_HOT;
import static org.opensearch.storage.common.tiering.TieringUtils.isCCRFollowerIndex;

/**
 * Validator for tiering service operations in OpenSearch.
 */
public class TieringServiceValidator {
    private static final Logger logger = LogManager.getLogger(TieringServiceValidator.class);
    private static final long GB_IN_BYTES = 1024 * 1024 * 1024;
    private static final long MIN_DISK_SPACE_BUFFER = 20 * GB_IN_BYTES;
    private static final double CLUSTER_WRITE_BLOCK_THRESHOLD_MULTIPLIER = 0.2;
    private static final long DEFAULT_FALLBACK_SHARD_SIZE = 0L;

    /**
     * Validates that the specified index is in the required initial tiering state.
     * Validation on the final tiering state is done by before calling this validation.
     *
     * @param state the current cluster state
     * @param index the index to be validated
     * @param currentTieringType tiering type
     */
    /**
     * Validates that the index is in the correct initial state for the requested tiering operation.
     */
    private static void validateIndexCurrentState(
        final ClusterState state,
        final Index index,
        final IndexModule.TieringState currentTieringType
    ) {
        final String indexState = state.getMetadata().getIndexSafe(index).getSettings().get(INDEX_TIERING_STATE.getKey(), HOT.toString());
        boolean isCurrentStateValid = true;
        if (currentTieringType.equals(HOT_TO_WARM) && indexState.equals(IndexModule.TieringState.HOT.toString()) == false
            || currentTieringType.equals(WARM_TO_HOT) && indexState.equals(IndexModule.TieringState.WARM.toString()) == false) {
            isCurrentStateValid = false;
        }
        if (isCurrentStateValid == false) {
            String targetState = currentTieringType.equals(HOT_TO_WARM) ? "WARM" : "HOT";
            String message = String.format(
                Locale.ROOT,
                "Cannot migrate index [%s] to %s tier: index is currently in [%s] tier. "
                    + "Index must be in %s tier to perform this migration.",
                index.getName(),
                targetState,
                indexState,
                currentTieringType.equals(HOT_TO_WARM) ? "HOT" : "WARM"
            );
            throw new TieringRejectionException(RejectionReason.INVALID_TIER_TRANSITION, new IllegalArgumentException(message));
        }
    }

    /**
     * Performs common validations for tiering operations.
     */
    /**
     * Performs common validations applicable to all tiering operations.
     */
    public static void validateCommon(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Index index,
        Integer tieringIndicesSize,
        Integer maxConcurrentTieringRequests,
        Integer jvmActiveUsageThresholdPercent,
        IndexModule.TieringState targetIndexType,
        IndexModule.TieringState tieringType,
        ShardLimitValidator shardLimitValidator
    ) {
        if (targetIndexType.equals(IndexModule.TieringState.WARM)) {
            validateWarmNodes(clusterState, index);
        }
        validateRemoteStoreEnabled(clusterState, index);
        validateShardLimit(clusterState, index, targetIndexType, shardLimitValidator);
        validateCCRIndex(clusterState, index, tieringType);
        validateIndexCurrentState(clusterState, index, tieringType);
        validateIndexHealth(clusterState, index, targetIndexType);
        enforceBackpressure(index, tieringIndicesSize, maxConcurrentTieringRequests, tieringType);
        checkJVMMemoryUtilizationThreshold(clusterState, clusterInfo, jvmActiveUsageThresholdPercent, targetIndexType);
    }

    /**
     * Performs common validations for hot to warm tiering operations.
     */
    /**
     * Validates a hot-to-warm tiering request including common checks, disk threshold, and file cache usage.
     */
    public static void validateHotToWarmTiering(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Set<Index> tieringIndices,
        Integer maxConcurrentTieringRequests,
        DiskThresholdEvaluator diskThresholdEvaluator,
        Integer fileCacheActiveUsageThreshold,
        Integer jvmActiveUsageThresholdPercent,
        Index index,
        ShardLimitValidator shardLimitValidator
    ) {
        validateCommon(
            clusterState,
            clusterInfo,
            index,
            tieringIndices.size(),
            maxConcurrentTieringRequests,
            jvmActiveUsageThresholdPercent,
            WARM,
            HOT_TO_WARM,
            shardLimitValidator
        );
        validateWarmNodeDiskThresholdWaterMarkLow(clusterState, clusterInfo, tieringIndices, index, diskThresholdEvaluator);
        // TODO: Enable file cache active usage validation once AggregateFileCacheStats is available in ClusterInfo
        // checkFileCacheActiveUsage(clusterState,clusterInfo, fileCacheActiveUsageThreshold);
    }

    /**
     * Performs common validations for warm to hot tiering operations.
     */
    /**
     * Validates a warm-to-hot tiering request including common checks, hot node capacity, and largest shard space.
     */
    public static void validateWarmToHotTiering(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Set<Index> tieringIndices,
        Integer maxConcurrentTieringRequests,
        Integer jvmActiveUsageThresholdPercent,
        Index index,
        ShardLimitValidator shardLimitValidator
    ) {
        validateCommon(
            clusterState,
            clusterInfo,
            index,
            tieringIndices.size(),
            maxConcurrentTieringRequests,
            jvmActiveUsageThresholdPercent,
            HOT,
            WARM_TO_HOT,
            shardLimitValidator
        );
        validateEligibleHotNodesCapacity(clusterState, clusterInfo, tieringIndices, index);
        validateSpaceForLargestShard(clusterState, clusterInfo, index, HOT, MIN_DISK_SPACE_BUFFER);
    }

    /**
     * Validates that the index is not in RED health status before tiering operation.
     */
    private static void validateIndexHealth(ClusterState clusterState, Index index, IndexModule.TieringState finalIndexType) {
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
        final IndexMetadata indexMetadata = clusterState.metadata().index(index);
        final ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);
        if (ClusterHealthStatus.RED.equals(indexHealth.getStatus())) {
            final String errorMsg = "Rejecting tiering request because index ["
                + index.getName()
                + "] is in RED status and cannot be migrated to ["
                + finalIndexType.toString()
                + "]";
            throw new TieringRejectionException(RejectionReason.INDEX_RED_STATUS, new IllegalArgumentException(errorMsg));
        }
    }

    /**
     * Validates that the index has remote store enabled before tiering operation.
     */
    /**
     * Validates that the index has remote store enabled.
     */
    private static void validateRemoteStoreEnabled(ClusterState clusterState, Index index) {
        final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(index);
        boolean isRemoteStoreEnabled = indexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false);
        if (!isRemoteStoreEnabled) {
            final String errorMsg = "Rejecting tiering request because index [" + index.getName() + "] does not have remote store enabled";
            throw new TieringRejectionException(RejectionReason.REMOTE_STORE_NOT_ENABLED, new IllegalArgumentException(errorMsg));
        }
    }

    // Visible for Testing
    public static void validateCCRIndex(ClusterState currentState, Index index, IndexModule.TieringState finalIndexType) {
        final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
        if (finalIndexType == IndexModule.TieringState.WARM && isCCRFollowerIndex(indexMetadata.getSettings())) {
            final StringBuilder errorMsgBuilder = new StringBuilder("Rejecting migration request because index [").append(index.getName())
                .append("] is a cross-cluster-replicated index and cannot be migrated to [")
                .append(finalIndexType.toString())
                .append("].")
                .append("To avoid migration failures, Stop cross-cluster-replication for this index and retry.");
            logger.warn(errorMsgBuilder.toString());
            throw new TieringRejectionException(
                RejectionReason.CCR_INDEX_REJECTION,
                new IllegalArgumentException(errorMsgBuilder.toString())
            );
        }
    }

    /**
     * Validates that the cluster shard limit will not be exceeded after tiering.
     */
    private static void validateShardLimit(
        ClusterState currentState,
        Index index,
        IndexModule.TieringState finalIndexType,
        ShardLimitValidator shardLimitValidator
    ) {
        final IndexMetadata indexMetadata = currentState.metadata().getIndexSafe(index);
        final Settings settings = indexMetadata.getSettings();
        final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);
        final int numberOfReplicas = Math.min(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), 1);
        final int shardsToCreate = numberOfShards * (1 + numberOfReplicas);
        final Optional<String> shardLimit = shardLimitValidator.checkShardLimit(
            shardsToCreate,
            currentState,
            finalIndexType == HOT ? RoutingPool.LOCAL_ONLY : RoutingPool.REMOTE_CAPABLE
        );
        if (shardLimit.isPresent()) {
            final ValidationException e = new ValidationException();
            e.addValidationError(shardLimit.get());
            throw new TieringRejectionException(RejectionReason.SHARD_LIMIT_EXCEEDED, e);
        }
    }

    /**
     * Validates that there are eligible nodes with the warm role in the current cluster state.
     *
     * @param clusterState the current cluster state
     * @param index the name of the indices being validated
     * @throws IllegalArgumentException if there are no eligible warm nodes in the cluster
     */
    private static void validateWarmNodes(final ClusterState clusterState, final Index index) {
        if (getTargetTierNodes(clusterState, WARM).isEmpty()) {
            final String errorMsg = "Rejecting tiering request for index ["
                + index.getName()
                + "] because there are no nodes found with the warm role";
            throw new TieringRejectionException(RejectionReason.NO_WARM_NODES, new IllegalArgumentException(errorMsg));
        }
    }

    /**
     * Enforces backpressure on tiering operations to prevent system overload.
     *
     * @param index The index for which the tiering operation is being requested.
     * @param currentTieringIndices The current number of indices undergoing tiering operations.
     * @param maxConcurrentTieringRequests The maximum allowed number of concurrent tiering operations.
     * @throws OpenSearchRejectedExecutionException If the number of current tiering operations
     *         equals or exceeds the migration queue size, indicating system overload.
     */
    private static void enforceBackpressure(
        Index index,
        Integer currentTieringIndices,
        int maxConcurrentTieringRequests,
        IndexModule.TieringState tieringState
    ) {
        if (currentTieringIndices >= maxConcurrentTieringRequests) {
            String errorMsg = "Rejecting tiering request for index ["
                + index.getName()
                + "] because there are too many in-flight tiering requests of type ["
                + tieringState.toString()
                + "]";
            throw new TieringRejectionException(
                RejectionReason.CONCURRENT_LIMIT_EXCEEDED,
                new OpenSearchRejectedExecutionException(errorMsg)
            );
        }
    }

    /**
     * Checks if JVM memory utilization threshold is exceeded on all nodes of the target tier.
     * This validation ensures there are available nodes in the target tier with sufficient
     * JVM memory capacity to handle new tiering requests.
     *
     * @param clusterState Current state of the cluster containing node information
     * @param clusterInfo Information about cluster resources including node statistics
     * @param targetIndexType Target tier type (e.g., HOT, WARM) where the index will be moved
     * @throws IllegalArgumentException if all nodes in the target tier exceed the JVM memory threshold
     */
    public static void checkJVMMemoryUtilizationThreshold(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Integer jvmActiveUsageThresholdPercent,
        IndexModule.TieringState targetIndexType
    ) {
        Set<DiscoveryNode> targetNodes = getTargetTierNodes(clusterState, targetIndexType);
        Map<String, NodeResourceUsageStats> nodeStats = clusterInfo.getNodeResourceUsageStats();

        boolean allNodesExceededThreshold = targetNodes.stream()
            .allMatch(node -> isNodeExceedingJVMThreshold(nodeStats, node, jvmActiveUsageThresholdPercent));

        if (allNodesExceededThreshold) {
            String errorMsg = "Rejecting tiering request as JVM Utilization is high on all [" + targetIndexType.name() + "] nodes";
            throw new TieringRejectionException(RejectionReason.HIGH_JVM_UTILIZATION, new IllegalArgumentException(errorMsg));
        }
    }

    private static boolean isNodeExceedingJVMThreshold(
        Map<String, NodeResourceUsageStats> nodeStats,
        DiscoveryNode node,
        Integer jvmActiveUsageThresholdPercent
    ) {
        if (nodeStats == null) return false;
        NodeResourceUsageStats stats = nodeStats.get(node.getId());
        return stats != null && stats.getMemoryUtilizationPercent() >= jvmActiveUsageThresholdPercent;
    }

    private static void checkFileCacheActiveUsage(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Integer fileCacheActiveUsageThreshold
    ) {
        Set<DiscoveryNode> targetNodes = getTargetTierNodes(clusterState, WARM);
        Map<String, AggregateFileCacheStats> fileCacheStatsMap = clusterInfo.getNodeFileCacheStats();

        boolean allNodesExceededFileCacheThreshold = targetNodes.stream()
            .allMatch(node -> isNodeExceedingFileCacheThreshold(fileCacheStatsMap, node, fileCacheActiveUsageThreshold));

        if (allNodesExceededFileCacheThreshold) {
            String errorMsg = "Rejecting tiering request as FileCache Active Usage is high on all Warm nodes";
            throw new TieringRejectionException(RejectionReason.HIGH_FILE_CACHE_UTILIZATION, new IllegalArgumentException(errorMsg));
        }
    }

    private static boolean isNodeExceedingFileCacheThreshold(
        Map<String, AggregateFileCacheStats> fileCacheStatsMap,
        DiscoveryNode node,
        Integer fileCacheActiveUsageThreshold
    ) {
        if (fileCacheStatsMap == null) return false;
        AggregateFileCacheStats stats = fileCacheStatsMap.get(node.getId());
        return stats != null && stats.getActivePercent() > fileCacheActiveUsageThreshold;
    }

    /**
     * Validates the capacity of eligible nodes in the cluster to accommodate the specified indices.
     *
     * @param clusterInfo the current nodes usage info for the cluster
     * @param clusterState the current cluster state
     * @param tieringIndices set of indices undergoing tiering
     * @param index index to be validated
     * @throws IllegalArgumentException if there isn't enough capacity in the target tier
     */
    protected static void validateEligibleHotNodesCapacity(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Set<Index> tieringIndices,
        Index index
    ) {
        int indexSizeMultiplier = getReplicaCount(clusterState, index) + 1;

        // 1. Get available space in target tier
        long totalAvailableBytesInTargetTier = getTotalAvailableBytesInTargetTier(
            clusterState,
            clusterInfo.getNodeLeastAvailableDiskUsages(),
            HOT
        );

        // Calculate space for existing tiering entries
        long totalTieringIndicesBytes = tieringIndices.stream()
            .mapToLong(
                tieringIndex -> (getReplicaCount(clusterState, tieringIndex) + 1) * getIndexPrimaryStoreSize(
                    clusterState,
                    clusterInfo,
                    tieringIndex.getName(),
                    HOT
                )
            )
            .sum();

        // Add space for incoming tiering request index
        long requestIndexBytes = indexSizeMultiplier * getIndexPrimaryStoreSize(clusterState, clusterInfo, index.getName(), HOT);

        // 3. Validate capacity
        if (requestIndexBytes + totalTieringIndicesBytes >= totalAvailableBytesInTargetTier) {
            final StringBuilder errorMsgBuilder = new StringBuilder("Rejecting migration request for index [").append(index)
                .append("] since we don't have enough [")
                .append(HOT)
                .append("] capacity. Please add more ")
                .append(HOT)
                .append(" nodes");
            logger.warn(
                errorMsgBuilder.toString()
                    + "Size of index to be migrated - "
                    + requestIndexBytes
                    + "Total "
                    + HOT
                    + " available bytes - "
                    + totalAvailableBytesInTargetTier
                    + ", "
                    + HOT
                    + " tieringQueue capacity to be utilized - "
                    + totalTieringIndicesBytes
            );
            throw new TieringRejectionException(
                RejectionReason.INSUFFICIENT_SPACE_HOT,
                new IllegalArgumentException(errorMsgBuilder.toString())
            );
        }
    }

    /**
     * Validates if warm nodes have sufficient disk space after considering watermark settings
     * and ongoing tiering operations. Only runs when remote data ratio is non-zero.
     *
     * @param clusterState current cluster state
     * @param clusterInfo cluster disk usage and shard locations
     * @param tieringIndices ongoing/pending tiering operations
     * @param index index to validate
     * @param diskThresholdEvaluator warm node disk evaluator
     * @throws IllegalArgumentException if warm nodes lack sufficient space
     */
    protected static void validateWarmNodeDiskThresholdWaterMarkLow(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        final Set<Index> tieringIndices,
        Index index,
        DiskThresholdEvaluator diskThresholdEvaluator
    ) {
        final Set<DiscoveryNode> eligibleNodes = getTargetTierNodes(clusterState, WARM);
        Map<String, DiskUsage> usages = clusterInfo.getNodeMostAvailableDiskUsages();
        long totalAddressableSpaceExcludingLowWatermark = 0L;

        for (DiscoveryNode node : eligibleNodes) {
            DiskUsage nodeDiskUsage = usages.get(node.getId());
            if (nodeDiskUsage == null) {
                continue;
            }
            totalAddressableSpaceExcludingLowWatermark += Math.max(
                0,
                nodeDiskUsage.getFreeBytes() - diskThresholdEvaluator.getFreeSpaceLowThreshold(nodeDiskUsage.getTotalBytes())
            );
        }

        long tieringShardSizes = calculateShardSizesForTieringIndices(clusterState, clusterInfo, tieringIndices);
        long indexPrimaryStoreSize = getIndexPrimaryStoreSize(clusterState, clusterInfo, index.getName(), WARM);
        if (totalAddressableSpaceExcludingLowWatermark - tieringShardSizes - indexPrimaryStoreSize <= 0) {
            String errorMsg = "Rejecting migration request for index [" + index + "] since we don't have enough space on all warm nodes";
            throw new TieringRejectionException(RejectionReason.INSUFFICIENT_SPACE_WARM, new IllegalArgumentException(errorMsg));
        }
    }

    /**
     * Validates whether there is sufficient space in the target tier nodes to accommodate the largest shard of the index.
     * This validation ensures that both primary and replica shards can be placed on different nodes with adequate space.
     *
     * @param clusterInfo Information about the cluster's current state including disk usage
     * @param clusterState Current state of the cluster
     * @param requestIndex Name of the index being validated
     * @param targetTier Target tier where the index will be moved
     * @param bufferSpace Minimum buffer space to maintain on nodes (in bytes)
     * @throws IllegalArgumentException if there isn't enough space on at least two nodes to accommodate
     *                              the largest shard (for primary and replica placement)
     */
    protected static void validateSpaceForLargestShard(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Index requestIndex,
        IndexModule.TieringState targetTier,
        long bufferSpace
    ) {
        long maxNodeCapacity = 0L;

        // find the size of largest shard of this index
        long largestShardBytes = getLargestShardBytes(clusterState, clusterInfo, requestIndex.getName());

        // if replica count is not 1 we need to account for shard size of primary only
        if (getReplicaCount(clusterState, requestIndex) == 0) {
            maxNodeCapacity = getHighestNodeCapacity(clusterState, clusterInfo, targetTier, bufferSpace);
        } else {
            maxNodeCapacity = getSecondHighestNodeCapacity(clusterState, clusterInfo, targetTier, bufferSpace);
        }

        // We have required nodes to place replicas of largest shard
        if (maxNodeCapacity < largestShardBytes) {
            logger.warn(
                "Details of node capacity and shard size: LargestShardSize: {}, MaxAvailableBytesOnNode: {}",
                largestShardBytes,
                maxNodeCapacity
            );
            final StringBuilder errorMsgBuilder = new StringBuilder("Rejecting migration request for index [").append(requestIndex)
                .append("] since we don't have [")
                .append(targetTier)
                .append("] node with free space to fit it's largest shard");
            throw new TieringRejectionException(
                RejectionReason.LARGEST_SHARD_SPACE_INSUFFICIENT,
                new IllegalArgumentException(errorMsgBuilder.toString())
            );
        }
    }

    /**
     * Calculates effective available bytes on a node after accounting for buffer space and write-block threshold.
     */
    private static long calculateEffectiveAvailableBytes(DiskUsage diskUsage, long bufferSpace) {
        long totalBytes = Math.max(0, diskUsage.getTotalBytes());
        long freeBytes = Math.max(0, diskUsage.getFreeBytes());
        // Accounting the risk of cluster-write-block if the below threshold is breached.
        long bufferBytes = Math.round(Math.min(bufferSpace, totalBytes * CLUSTER_WRITE_BLOCK_THRESHOLD_MULTIPLIER));
        return Math.max(0, freeBytes - bufferBytes);
    }

    /**
     * Calculates the largest and second-largest available disk space among nodes in the target tier.
     *
     * @param clusterState current cluster state
     * @param clusterInfo cluster disk usage information
     * @param targetTier target tier to analyze
     * @param bufferSpace required buffer space
     * @return array where [0] is largest and [1] is second-largest available space
     */
    private static long[] calculateTopTwoAvailableSpaces(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        IndexModule.TieringState targetTier,
        long bufferSpace
    ) {

        Set<String> eligibleNodeIds = getTargetTierNodes(clusterState, targetTier).stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());

        long maxAvailableBytes = Long.MIN_VALUE;
        long nextMaxAvailableBytes = Long.MIN_VALUE;

        for (String nodeId : eligibleNodeIds) {
            DiskUsage diskUsage = clusterInfo.getNodeLeastAvailableDiskUsages().get(nodeId);
            if (diskUsage == null) {
                logger.warn("Disk usage information not available for node [{}]", nodeId);
                continue;
            }
            long availableBytes = calculateEffectiveAvailableBytes(diskUsage, bufferSpace);

            if (maxAvailableBytes <= availableBytes) {
                nextMaxAvailableBytes = maxAvailableBytes;
                maxAvailableBytes = availableBytes;
            } else if (availableBytes > nextMaxAvailableBytes) {
                nextMaxAvailableBytes = availableBytes;
            }
        }

        return new long[] { maxAvailableBytes, nextMaxAvailableBytes };
    }

    /** Returns the highest available disk space among nodes in the target tier. */
    private static long getHighestNodeCapacity(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        IndexModule.TieringState targetTier,
        long bufferSpace
    ) {
        return calculateTopTwoAvailableSpaces(clusterState, clusterInfo, targetTier, bufferSpace)[0];
    }

    /** Returns the second-highest available disk space among nodes in the target tier. */
    private static long getSecondHighestNodeCapacity(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        IndexModule.TieringState targetTier,
        long bufferSpace
    ) {
        return calculateTopTwoAvailableSpaces(clusterState, clusterInfo, targetTier, bufferSpace)[1];
    }

    /** Calculates total primary shard sizes for all indices currently undergoing tiering. */
    private static long calculateShardSizesForTieringIndices(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        Set<Index> tieringIndices
    ) {
        var totalShardSizes = 0L;
        if (tieringIndices.isEmpty() == false) {
            for (Index index : tieringIndices) {
                totalShardSizes += getIndexPrimaryStoreSize(clusterState, clusterInfo, index.getName(), WARM);
            }
        }
        return totalShardSizes;
    }

    /**
     * Calculates the total size of the specified index in the cluster
     * Note: This function only accounts for the following conditions :
     * 1. should be a primary shard
     * 2. If in STARTED state then should not be assigned to the target tier node
     * 3. If UNASSIGNED/RELOCATING, should be considered
     *
     * @param clusterState the current state of the cluster
     * @param clusterInfo  the current nodes usage info for the cluster
     * @param index        the name of the index for which the total size is to be calculated
     * @return the total size of the specified index in the cluster
     */
    static long getIndexPrimaryStoreSize(
        ClusterState clusterState,
        ClusterInfo clusterInfo,
        String index,
        IndexModule.TieringState targetTier
    ) {
        return clusterState.routingTable()
            .allShards(index)
            .stream()
            .filter(ShardRouting::primary)
            .filter(shard -> shouldIncludeShard(clusterState, shard, targetTier))
            .mapToLong(shard -> clusterInfo.getShardSize(shard, DEFAULT_FALLBACK_SHARD_SIZE))
            .sum();
    }

    /** Returns the size in bytes of the largest primary shard for the given index. */
    private static long getLargestShardBytes(ClusterState clusterState, ClusterInfo clusterInfo, String requestIndex) {
        long largestShardBytes = Long.MIN_VALUE;

        List<ShardRouting> shardRoutings = clusterState.routingTable().allShards(requestIndex);
        for (ShardRouting shardRouting : shardRoutings) {
            if (shardRouting.primary()) {
                long currentShardBytes = Math.max(0, clusterInfo.getShardSize(shardRouting, DEFAULT_FALLBACK_SHARD_SIZE));
                largestShardBytes = Math.max(largestShardBytes, currentShardBytes);
            }
        }
        return largestShardBytes;
    }

    /**
     * Returns totalAvailableBytes across all nodes in the target tier
     */
    static long getTotalAvailableBytesInTargetTier(
        final ClusterState clusterState,
        final Map<String, DiskUsage> usages,
        IndexModule.TieringState targetTier
    ) {
        Set<String> targetNodeIds = getTargetTierNodes(clusterState, targetTier).stream()
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());

        long totalAvailableBytes = 0;
        for (String node : targetNodeIds) {
            totalAvailableBytes += usages.get(node).getFreeBytes();
        }
        return totalAvailableBytes;
    }

    /**
     * Determines whether a shard should be included in the tier calculation based on its state and current location.
     * The following shards are always included:
     * - RELOCATING shards
     * - UNASSIGNED shards
     * - Any shard not in STARTED state
     * For STARTED shards:
     * - Include if the shard is currently on a warm node but target tier is not warm
     * - Include if the shard is currently on a hot node but target tier is warm
     *
     * @param clusterState Current state of the cluster
     * @param shard The shard routing to evaluate
     * @param targetTier The target tier (HOT/WARM) we're calculating for
     * @return true if the shard should be included in calculations, false otherwise
     */
    private static boolean shouldIncludeShard(ClusterState clusterState, ShardRouting shard, IndexModule.TieringState targetTier) {
        if (!shard.state().equals(ShardRoutingState.STARTED)) {
            return true;
        }
        boolean isTargetTierWarm = WARM.equals(targetTier);
        return clusterState.getNodes().get(shard.currentNodeId()).isWarmNode() != isTargetTierWarm;
    }

    /** Returns the configured replica count for the given index. */
    private static int getReplicaCount(ClusterState clusterState, Index index) {
        final IndexMetadata indexMetadata = clusterState.metadata().index(index);
        return Integer.parseInt(indexMetadata.getSettings().get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()));
    }

    /**
     * Returns set of nodes eligible for target tier operations.
     */
    private static Set<DiscoveryNode> getTargetTierNodes(final ClusterState clusterState, IndexModule.TieringState targetTier) {
        // TODO: We need to make this validation strict by excluding nodes from the excluded DI
        final Map<String, DiscoveryNode> nodes = clusterState.getNodes().getDataNodes();
        return nodes.values()
            .stream()
            .filter(WARM.equals(targetTier) ? DiscoveryNode::isWarmNode : node -> !node.isWarmNode())
            .collect(Collectors.toSet());
    }
}
