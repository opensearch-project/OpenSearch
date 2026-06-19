/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.storage.action.tiering.CancelTieringRequest;
import org.opensearch.storage.action.tiering.IndexTieringRequest;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.storage.common.tiering.TieringRejectionException;
import org.opensearch.storage.common.tiering.TieringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_WRITE_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.storage.common.tiering.TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT;
import static org.opensearch.storage.common.tiering.TieringUtils.TIERING_CUSTOM_KEY;
import static org.opensearch.storage.common.tiering.TieringUtils.getTierPairForTargetTier;
import static org.opensearch.storage.common.tiering.TieringUtils.getTieringSourceType;
import static org.opensearch.storage.common.tiering.TieringUtils.getTieringStartTime;
import static org.opensearch.storage.common.tiering.TieringUtils.isShardStateValidForTier;
import static org.opensearch.storage.common.tiering.TieringUtils.resolveRequestIndex;

/**
 * Abstract base class for managing tiering operations in OpenSearch.
 * This service handles the movement of indices between different storage tiers
 * and manages the cluster state updates during tiering operations.
 */
public abstract class TieringService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TieringService.class);
    /**
     * Status constant indicating that a tiering operation is currently executing shard relocation.
     */
    private static final String TIERING_IN_PROGRESS_STATUS = "RUNNING_SHARD_RELOCATION";

    /** The cluster service. */
    protected final ClusterService clusterService;
    /** The cluster info service. */
    protected final ClusterInfoService clusterInfoService;
    /** The index name expression resolver. */
    protected final IndexNameExpressionResolver indexNameExpressionResolver;
    /** The allocation service. */
    protected final AllocationService allocationService;
    /** The set of indices currently being tiered. */
    protected final Set<Index> tieringIndices;
    /** The disk threshold settings. */
    protected final DiskThresholdSettings diskThresholdSettings;
    /** The file cache settings. */
    protected final FileCacheSettings fileCacheSettings;
    /** The shard limit validator. */
    protected final ShardLimitValidator shardLimitValidator;
    /**
     * Controls maximum number of in-flight migrations.
     */
    private Integer maxConcurrentTieringRequests;
    /**
     * Setting to avoid fileCacheUsage overload while accepting tiering requests .
     */
    private Integer jvmActiveUsageThresholdPercent;
    // TODO: Add TierActionMetrics field and its settings/initialization in the implementation PR
    /** The node ID. */
    protected final String nodeId;

    /**
     * Constructs a new TieringService.
     * @param settings the settings
     * @param clusterService the cluster service
     * @param clusterInfoService the cluster info service
     * @param indexNameExpressionResolver the index name expression resolver
     * @param allocationService the allocation service
     * @param nodeEnvironment the node environment
     * @param shardLimitValidator the shard limit validator
     */
    protected TieringService(
        final Settings settings,
        final ClusterService clusterService,
        final ClusterInfoService clusterInfoService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AllocationService allocationService,
        final NodeEnvironment nodeEnvironment,
        final ShardLimitValidator shardLimitValidator
    ) {
        this.clusterService = clusterService;
        this.clusterInfoService = clusterInfoService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.allocationService = allocationService;
        this.nodeId = nodeEnvironment.nodeId();
        this.tieringIndices = ConcurrentHashMap.newKeySet();
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterService.getClusterSettings());
        this.fileCacheSettings = new FileCacheSettings(settings, clusterService.getClusterSettings());
        this.shardLimitValidator = shardLimitValidator;

        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(getMaxConcurrentTieringRequestsSetting(), this::setMaxConcurrentTieringRequests);
        setMaxConcurrentTieringRequests(clusterSettings.get(getMaxConcurrentTieringRequestsSetting()));
        clusterSettings.addSettingsUpdateConsumer(JVM_USAGE_TIERING_THRESHOLD_PERCENT, this::setJvmActiveUsageThresholdPercent);
        setJvmActiveUsageThresholdPercent(clusterSettings.get(JVM_USAGE_TIERING_THRESHOLD_PERCENT));

        if (DiscoveryNode.isClusterManagerNode(settings)) {
            clusterService.addListener(this);
        }
    }

    /** Returns the settings to add when tiering starts. @return the tiering start settings */
    protected abstract Settings getTieringStartSettingsToAdd(IndexMetadata indexMetadata);

    /** Returns the index tier settings to restore after cancellation. @return the settings to restore */
    protected abstract Settings getIndexTierSettingsToRestoreAfterCancellation(IndexMetadata indexMetadata);

    /** Returns the ClusterBlocks.Builder with tier-specific block changes for tier start. Only called for DFA indices. */
    protected abstract ClusterBlocks.Builder getTieringStartClusterBlocksToAdd(
        ClusterBlocks.Builder blocksBuilder,
        String indexName,
        IndexMetadata indexMetadata
    );

    /** Returns the ClusterBlocks.Builder with tier-specific block changes for a cancel. Default is a no-op. */
    protected ClusterBlocks.Builder getIndexTierClusterBlocksToRestoreAfterCancellation(
        ClusterBlocks.Builder blocksBuilder,
        String indexName,
        IndexMetadata indexMetadata
    ) {
        return blocksBuilder;
    }

    /** Returns the key for tiering start time. @return the tiering start time key */
    protected abstract String getTieringStartTimeKey();

    /** Returns the setting for max concurrent tiering requests. @return the max concurrent tiering requests setting */
    protected abstract Setting<Integer> getMaxConcurrentTieringRequestsSetting();

    /** Returns the target tiering state. @return the target tiering state */
    protected abstract IndexModule.TieringState getTargetTieringState();

    /** Returns the tiering type (in-progress state). @return the tiering type */
    protected abstract IndexModule.TieringState getTieringType();

    /** Validates the tiering request.
     * @param clusterState the cluster state
     * @param service the cluster info service
     * @param tieringEntries the tiering entries
     * @param maxConcurrentTieringRequests the max concurrent tiering requests
     * @param jvmActiveUsageThresholdPercent the JVM active usage threshold percent
     * @param index the index
     */
    protected abstract void validateTieringRequest(
        ClusterState clusterState,
        ClusterInfoService service,
        Set<Index> tieringEntries,
        Integer maxConcurrentTieringRequests,
        Integer jvmActiveUsageThresholdPercent,
        Index index
    );

    /**
     * Set maximum number of in-flight tiering indices
     */
    private void setMaxConcurrentTieringRequests(Integer maxConcurrentTieringRequests) {
        this.maxConcurrentTieringRequests = maxConcurrentTieringRequests;
    }

    /**
     * Set threshold of maximum JVM Usage Threshold over target tier nodes
     */
    private void setJvmActiveUsageThresholdPercent(Integer jvmActiveUsageThresholdPercent) {
        this.jvmActiveUsageThresholdPercent = jvmActiveUsageThresholdPercent;
    }

    /**
     * Handles cluster changed events only on current master node to prevent duplicate updates
     *
     * @param event cluster state change event
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        String source = getTieringSourceType(getTargetTieringState());
        if (event.localNodeClusterManager()) {
            if (event.previousState().nodes().isLocalNodeElectedClusterManager() == false
                || (event.blocksChanged()
                    && event.previousState().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)
                    && !event.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK))) {
                reconstructInProgressTieringRequests(event.state(), getTieringType(), source);
            }
            if (event.routingTableChanged() && tieringIndices.isEmpty() == false) {
                logger.debug(
                    () -> String.format(
                        Locale.ROOT,
                        "[%s] processing %d in-progress requests after routing table update",
                        source,
                        tieringIndices.size()
                    )
                );
                processTieringInProgress(event.state(), source);
            }
            if (event.routingTableChanged()) {
                removeWriteBlockForCancelledDfaIndices(event.state());
            }
        }
    }

    void reconstructInProgressTieringRequests(
        final ClusterState clusterState,
        final IndexModule.TieringState tieringState,
        final String source
    ) {
        logger.info(() -> String.format(Locale.ROOT, "[%s] initiating reconstruction of in-progress migration requests", source));
        clusterState.metadata().indices().values().forEach(indexMetadata -> {
            try {
                String indexTieringState = indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey());
                if (tieringState.toString().equals(indexTieringState)) {
                    Index index = indexMetadata.getIndex();
                    tieringIndices.add(index);
                    logger.info(
                        () -> String.format(Locale.ROOT, "[%s] added index [%s] to in-progress migration tracking", source, index.getName())
                    );
                }
            } catch (Exception e) {
                logger.error("Failed to process index [{}] during reconstruction. Error: {}", indexMetadata.getIndex().getName(), e);
            }
        });
    }

    /**
     * Determines if a shard is located in its target tier.
     */
    protected boolean isShardInTargetTier(final ShardRouting shard, final ClusterState clusterState) {
        return isShardStateValidForTier(shard, clusterState, getTargetTieringState());
    }

    /**
     * Processes pending tiering requests by checking the status of shard relocations
     * and updating the cluster state accordingly.
     *
     * @param clusterState current state of the cluster
     * @param source identifier for the tiering operation
     */
    void processTieringInProgress(final ClusterState clusterState, final String source) {
        Set<Index> completedInBatch = new HashSet<>();
        Set<Index> deletedIndices = new HashSet<>();

        for (Index index : tieringIndices) {
            logger.debug(() -> String.format(Locale.ROOT, "Checking if [%s] is complete for index: %s", source, index.getName()));
            List<ShardRouting> shardRoutings;
            if (clusterState.routingTable().hasIndex(index)) {
                shardRoutings = clusterState.routingTable().allShards(index.getName());
            } else {
                logger.info(() -> String.format(Locale.ROOT, "Index [%s] deleted before %s completion", index.getName(), source));
                deletedIndices.add(index);
                continue;
            }

            boolean allShardRelocationComplete = true;
            for (ShardRouting shard : shardRoutings) {
                if (isShardInTargetTier(shard, clusterState) == false) {
                    allShardRelocationComplete = false;
                    break;
                }
            }

            if (allShardRelocationComplete) {
                logger.info(
                    () -> String.format(
                        Locale.ROOT,
                        "Shard relocation for [%s] completed successfully for index [%s]",
                        source,
                        index.getName()
                    )
                );
                completedInBatch.add(index);
            }
        }

        tieringIndices.removeAll(deletedIndices);

        // Only update cluster state once for all completed indices
        if (completedInBatch.isEmpty() == false) {
            logger.debug(() -> String.format(Locale.ROOT, "Updating cluster state for indices: [%s]", completedInBatch));
            updateClusterStateForTieredIndices(completedInBatch, source);
        }
    }

    /**
     * Updates the cluster state for indices that have completed tiering.
     * This method submits a cluster state update task to mark the tiering operation as complete.
     *
     * @param completedIndices set of indices that have completed tiering
     * @param source identifier for the tiering operation
     */
    private void updateClusterStateForTieredIndices(final Set<Index> completedIndices, final String source) {
        // Update cluster state for completed indices, we use NORMAL priority here.
        clusterService.submitStateUpdateTask(
            "complete " + source + " for indices: " + completedIndices,
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    for (Index index : completedIndices) {
                        if (tieringIndices.contains(index)) {
                            final IndexMetadata indexMetadata = currentState.metadata().index(index);
                            if (indexMetadata == null) {
                                logger.info(
                                    () -> String.format(Locale.ROOT, "Index [%s] not found while completing %s", index.getName(), source)
                                );
                                tieringIndices.remove(index);
                                continue;
                            }
                            updateIndexMetadataPostTiering(metadataBuilder, indexMetadata);
                        }
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage("Failed to complete task {} for indices [{}]", source, completedIndices),
                        e
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info(() -> String.format(Locale.ROOT, "Cluster state updated successfully for task %s", source));
                    for (Index index : completedIndices) {
                        tieringIndices.remove(index);
                    }
                }
            }
        );
    }

    /**
     * Cancels an ongoing tiering operation for an index.
     * This method reverts the index settings and triggers reroute to move shards back to their original state.
     *
     * @param request the cancel tiering request
     * @param listener callback listener for the cancel operation
     * @param state current cluster state
     */
    public void cancelTiering(
        final CancelTieringRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener,
        final ClusterState state
    ) {
        try {
            final Index index = resolveRequestIndex(indexNameExpressionResolver, request.getIndex(), state);
            final String source = "cancel " + getTieringSourceType(getTargetTieringState());

            clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(Priority.IMMEDIATE) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final IndexMetadata indexMetadata = currentState.metadata().index(index);
                    validateTieringCancelRequest(index, indexMetadata, currentState);

                    final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

                    updateIndexMetadataForTieringCancel(metadataBuilder, indexMetadata);

                    ClusterState.Builder stateBuilder = ClusterState.builder(currentState)
                        .metadata(metadataBuilder)
                        .routingTable(routingTableBuilder.build())
                        .blocks(
                            getIndexTierClusterBlocksToRestoreAfterCancellation(
                                ClusterBlocks.builder().blocks(currentState.blocks()),
                                index.getName(),
                                indexMetadata
                            )
                        );

                    ClusterState updatedState = stateBuilder.build();

                    // Trigger reroute to move shards back to original state
                    return allocationService.reroute(updatedState, source);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> new ParameterizedMessage("Failed to cancel tiering for index [{}]", request.getIndex()), e);
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info(
                        () -> String.format(
                            Locale.ROOT,
                            "Successfully cancelled [%s] tiering for index [%s]",
                            getTieringSourceType(getTargetTieringState()),
                            index.getName()
                        )
                    );
                    tieringIndices.remove(index);
                    listener.onResponse(new ClusterStateUpdateResponse(true));
                }

                @Override
                public TimeValue timeout() {
                    return request.timeout();
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Checks if the given index is currently being tiered.
     * @param index the index to check
     * @return true if the index is being tiered
     */
    public boolean isIndexBeingTiered(Index index) {
        return tieringIndices.contains(index);
    }

    /**
     * Initiates a tiering operation for an index.
     * This method handles the cluster state updates required to start the tiering process.
     *
     * @param request the tiering request containing index and tier information
     * @param listener callback listener for the tiering operation
     * @param state current cluster state
     */
    public void tier(
        final IndexTieringRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener,
        final ClusterState state
    ) {
        try {
            final Index index = resolveRequestIndex(indexNameExpressionResolver, request.getIndex(), state);
            final String source = getTieringSourceType(request);

            clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {

                    if (tieringIndices.contains(index)) {
                        return currentState;
                    }
                    try {
                        validateTieringRequest(
                            currentState,
                            clusterInfoService,
                            tieringIndices,
                            maxConcurrentTieringRequests,
                            jvmActiveUsageThresholdPercent,
                            index
                        );
                    } catch (TieringRejectionException e) {
                        throw (RuntimeException) e.getCause();
                    }

                    final RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
                    final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    final IndexMetadata indexMetadata = currentState.metadata().index(index);

                    updateIndexMetadataForTieringStart(metadataBuilder, routingTableBuilder, indexMetadata, index);
                    ClusterState.Builder stateBuilder = ClusterState.builder(currentState)
                        .metadata(metadataBuilder)
                        .routingTable(routingTableBuilder.build())
                        .blocks(
                            getTieringStartClusterBlocksToAdd(
                                ClusterBlocks.builder().blocks(currentState.blocks()),
                                index.getName(),
                                indexMetadata
                            )
                        );

                    ClusterState updatedState = stateBuilder.build();

                    // now, reroute to trigger shard relocation
                    return allocationService.reroute(updatedState, source);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Failed to update cluster state update for {} for index [{}]",
                            source,
                            request.getIndex()
                        ),
                        e
                    );
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info(() -> String.format(Locale.ROOT, "Successfully initiated %s for index [%s]", source, index.getName()));
                    tieringIndices.add(index);
                    listener.onResponse(new ClusterStateUpdateResponse(true));
                }

                @Override
                public TimeValue timeout() {
                    return request.clusterManagerNodeTimeout();
                }
            });

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates index metadata and routing table at the start of a tiering operation.
     * Implementation should handle tier-specific initialization.
     *
     * @param metadataBuilder builder for updating cluster metadata
     * @param routingTableBuilder builder for updating routing table
     * @param indexMetadata current metadata of the index
     * @param index index to be tiered
     */
    void updateIndexMetadataForTieringStart(
        final Metadata.Builder metadataBuilder,
        final RoutingTable.Builder routingTableBuilder,
        final IndexMetadata indexMetadata,
        final Index index
    ) {
        try {
            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(indexMetadata.getSettings())
                .put(getTieringStartSettingsToAdd(indexMetadata));

            // 2. Handle replica updates if needed
            int currentReplicas = Integer.parseInt(indexMetadata.getSettings().get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()));
            if (currentReplicas != 1) {
                indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1);
            }

            // 3. Create tiering custom data
            Map<String, String> tieringCustomData = new HashMap<>();
            tieringCustomData.put(getTieringStartTimeKey(), String.valueOf(System.currentTimeMillis()));

            // 4. Build and update metadata
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                .settings(indexSettingsBuilder)
                .putCustom(TIERING_CUSTOM_KEY, tieringCustomData)
                .settingsVersion(1 + indexMetadata.getSettingsVersion());

            metadataBuilder.put(indexMetadataBuilder);

            // 5. Update routing table if replicas were changed
            if (currentReplicas != 1) {
                final String[] indices = new String[] { index.getName() };
                routingTableBuilder.updateNumberOfReplicas(1, indices);
                metadataBuilder.updateNumberOfReplicas(1, indices);
            }
        } catch (Exception e) {
            throw new OpenSearchException("Failed to update index metadata for tiering start", e);
        }
    }

    /**
     * Updates the metadata for an index after tiering operation completion.
     * Handles common metadata updates while allowing specific validations per implementation.
     *
     * @param metadataBuilder builder for updating cluster metadata
     * @param indexMetadata current metadata of the index
     */
    void updateIndexMetadataPostTiering(final Metadata.Builder metadataBuilder, final IndexMetadata indexMetadata) {
        try {
            // Build settings
            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(indexMetadata.getSettings())
                .put(INDEX_TIERING_STATE.getKey(), getTargetTieringState());

            // Update index metadata
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata).settings(indexSettingsBuilder);
            indexMetadataBuilder.removeCustom(TIERING_CUSTOM_KEY);

            // Update index settings version
            indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
            metadataBuilder.put(indexMetadataBuilder);
        } catch (Exception e) {
            throw new OpenSearchException("Failed to update index metadata post tiering", e);
        }
    }

    /**
     * Updates index metadata when cancelling a tiering operation.
     * This method reverts the changes made during tiering start to restore the index to its original state.
     *
     * @param metadataBuilder builder for updating cluster metadata
     * @param indexMetadata current metadata of the index
     */
    void updateIndexMetadataForTieringCancel(final Metadata.Builder metadataBuilder, final IndexMetadata indexMetadata) {
        try {
            // 1. Build settings - remove tiering-specific settings and disable auto-expand.
            // write-block settings only for DFA indices.
            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(indexMetadata.getSettings())
                .put(getIndexTierSettingsToRestoreAfterCancellation(indexMetadata));

            // 2. Build and update metadata
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                .settings(indexSettingsBuilder)
                .settingsVersion(1 + indexMetadata.getSettingsVersion());

            // 3. Remove tiering custom metadata
            indexMetadataBuilder.removeCustom(TIERING_CUSTOM_KEY);
            metadataBuilder.put(indexMetadataBuilder);
        } catch (Exception e) {
            throw new OpenSearchException("Failed to update index metadata for tiering cancellation", e);
        }
    }

    /**
     * Runs tiering validation synchronously before any cluster state mutation.
     *
     * <p>Used by the DFA (pluggable dataformat) tiering path to validate BEFORE adding the
     * read-only block or running pre-tiering sync. This prevents expensive and side-effecting
     * operations (flush, remote store sync) from running when validation would reject the request.
     *
     * <p>Note: validation also runs inside the cluster state task (in {@link #tier}) for
     * double-safety against TOCTOU races. This preflight call is an additional early gate.
     *
     * @param state current cluster state at the time of the request
     * @param index index to be tiered
     * @throws RuntimeException if validation fails — same exceptions as thrown by {@link #validateTieringRequest}
     */
    public void preflightValidate(final ClusterState state, final Index index) {
        validateTieringRequest(
            state,
            clusterInfoService,
            tieringIndices,
            maxConcurrentTieringRequests,
            jvmActiveUsageThresholdPercent,
            index
        );
    }

    /**
     * Performs common validations for tiering cancel request.
     *
     * @param index index for tiering cancel request
     * @param indexMetadata current metadata of the index
     * @param currentState current cluster state
     */
    void validateTieringCancelRequest(final Index index, final IndexMetadata indexMetadata, final ClusterState currentState) {
        // Accept the index as cancellable if it is tracked in the in-memory set OR its persisted tiering
        // state shows a migration in progress. The in-memory set is per-cluster-manager and starts empty
        // on a newly elected cluster-manager; until it is rebuilt from cluster state
        // (reconstructInProgressTieringRequests), the persisted INDEX_TIERING_STATE is what lets cancel
        // reach a mid-migration index. (The cluster state itself is durable and not lost across failover.)
        if (!tieringIndices.contains(index) && !isMigrationInProgress(indexMetadata)) {
            throw new IllegalArgumentException("Index [" + index + "] is not currently undergoing tiering operation");
        }
        if (indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey(), "").equals(getTargetTieringState().toString())) {
            throw new IllegalArgumentException(
                "Index [" + index + "] already reached its target tier: " + getTargetTieringState() + ". Cannot cancel tiering"
            );
        }
        if (currentState.routingTable().hasIndex(index) == false) {
            throw new IllegalArgumentException("Index [" + index + "] deleted before tiering cancellation");
        }
    }

    /**
     * Returns true if the index's persisted tiering state indicates a migration is in progress
     * (HOT_TO_WARM or WARM_TO_HOT), as opposed to a terminal HOT/WARM state.
     */
    private static boolean isMigrationInProgress(final IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            return false;
        }
        final String state = indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT.name());
        return IndexModule.TieringState.HOT_TO_WARM.name().equals(state) || IndexModule.TieringState.WARM_TO_HOT.name().equals(state);
    }

    /**
     * Retrieves the tiering status for a specific index.
     * This method provides detailed information about an ongoing tiering operation
     * including source and target tiers, start time, and shard-level status.
     *
     * @param requestIndex The name of the index to check
     * @param isDetailedFlagEnabled If true, includes detailed shard-level status information
     * @return TieringStatus object containing detailed status information
     * @throws IllegalArgumentException if the specified index has no active migrations
     */
    public TieringStatus getTieringStatus(String requestIndex, Boolean isDetailedFlagEnabled) {
        final Index index = resolveRequestIndex(indexNameExpressionResolver, requestIndex, clusterService.state());

        if (tieringIndices.contains(index)) {
            try {
                return constructTieringStatus(index, true, isDetailedFlagEnabled);
            } catch (IllegalStateException e) {
                throw new IllegalArgumentException("Index [" + requestIndex + "] has no active migrations");
            }

        }
        throw new IllegalArgumentException("Index [" + requestIndex + "] has no active migrations");
    }

    /**
     * Retrieves a list of tiering status for all indices currently undergoing tiering operations.
     * This method provides a summary view of all active tiering operations in the cluster.
     *
     * @return List of TieringStatus objects, each representing the status of an ongoing tiering operation
     * Returns an empty list if no tiering operations are active
     */
    public List<TieringStatus> listTieringStatus() {
        final List<TieringStatus> tieringStatusList = new ArrayList<>();

        for (Index tieringIndex : tieringIndices) {
            try {
                tieringStatusList.add(constructTieringStatus(tieringIndex, false, false));
            } catch (IllegalStateException e) {
                continue;
            }
        }
        return tieringStatusList;
    }

    /**
     * Lifts the write block from DFA indices whose H2W cancel completed but the block was intentionally
     * kept to prevent writes reaching warm-node shards that still run a read-only engine.
     *
     * <p>Called on every routing-table or metadata change. It removes the block only when:
     * <ol>
     *   <li>The index is NOT in {@code tieringIndices} (cancel completed, no active tiering)</li>
     *   <li>The index is a DFA index</li>
     *   <li>{@code INDEX_TIERING_STATE=HOT} (cancel reverted the tier state)</li>
     *   <li>{@code INDEX_BLOCKS_WRITE=true} (block still set from H2W preparation)</li>
     *   <li>All shards are {@code started} on HOT nodes (writable engine is live)</li>
     * </ol>
     */
    private void removeWriteBlockForCancelledDfaIndices(final ClusterState clusterState) {
        Set<Index> indicesToUnblock = new HashSet<>();
        for (IndexMetadata indexMetadata : clusterState.metadata()) {
            // Only act on indices that are NOT currently tiering
            if (tieringIndices.contains(indexMetadata.getIndex())) {
                continue;
            }
            if (!TieringUtils.isDfaIndex(indexMetadata)) {
                continue;
            }
            // Must be in HOT state (cancel succeeded) with write block still present
            String tieringState = indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey(), "");
            boolean hasWriteBlock = INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings());
            if (!IndexModule.TieringState.HOT.toString().equals(tieringState) || !hasWriteBlock) {
                continue;
            }
            // All shards must be started on hot nodes before we re-enable writes
            if (!clusterState.routingTable().hasIndex(indexMetadata.getIndex())) {
                continue;
            }
            List<ShardRouting> shards = clusterState.routingTable().allShards(indexMetadata.getIndex().getName());
            boolean allOnHot = shards.stream()
                .allMatch(
                    s -> (s.unassigned() && !s.primary())
                        || (s.started() && isShardStateValidForTier(s, clusterState, IndexModule.TieringState.HOT))
                );
            if (allOnHot) {
                indicesToUnblock.add(indexMetadata.getIndex());
            }
        }
        if (indicesToUnblock.isEmpty()) {
            return;
        }
        clusterService.submitStateUpdateTask(
            "remove-write-block-after-h2w-cancel for " + indicesToUnblock,
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
                    ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
                    for (Index index : indicesToUnblock) {
                        IndexMetadata indexMetadata = currentState.metadata().index(index);
                        if (indexMetadata == null) continue;
                        // Re-check conditions inside the task to guard against TOCTOU
                        String tieringState = indexMetadata.getSettings().get(INDEX_TIERING_STATE.getKey(), "");
                        boolean hasWriteBlock = INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings());
                        if (!IndexModule.TieringState.HOT.toString().equals(tieringState) || !hasWriteBlock) {
                            continue;
                        }
                        Settings.Builder settingsBuilder = Settings.builder()
                            .put(indexMetadata.getSettings())
                            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false);
                        metadataBuilder.put(
                            IndexMetadata.builder(indexMetadata)
                                .settings(settingsBuilder)
                                .settingsVersion(1 + indexMetadata.getSettingsVersion())
                        );
                        blocksBuilder.removeIndexBlock(index.getName(), IndexMetadata.INDEX_WRITE_BLOCK);
                        logger.info("Removed write block for DFA index [{}] after H2W cancel — all shards on hot", index.getName());
                    }
                    return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocksBuilder).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(
                        () -> new ParameterizedMessage("Failed to remove write block after H2W cancel for indices {}", indicesToUnblock),
                        e
                    );
                }
            }
        );
    }

    private TieringStatus constructTieringStatus(Index index, boolean shardLevelStatus, boolean isDetailedFlagEnabled) {
        TieringStatus tieringStatus;
        long tieringStartTime = getTieringStartTime(clusterService.state(), index, getTieringStartTimeKey());

        String[] tiers = getTierPairForTargetTier(getTargetTieringState());
        String sourceTier = tiers[0];
        String destinationTier = tiers[1];
        tieringStatus = new TieringStatus(index.getName(), TIERING_IN_PROGRESS_STATUS, sourceTier, destinationTier, tieringStartTime);

        if (shardLevelStatus) {
            tieringStatus.setShardLevelStatus(
                TieringStatus.ShardLevelStatus.fromRoutingTable(
                    clusterService.state(),
                    index.getName(),
                    isDetailedFlagEnabled,
                    destinationTier
                )
            );
        }
        return tieringStatus;
    }
}
