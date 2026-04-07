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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.indices.ShardLimitValidator;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract base class for managing tiering operations in OpenSearch.
 * This service handles the movement of indices between different storage tiers
 * and manages the cluster state updates during tiering operations.
 *
 * Constructor wiring, ClusterStateListener implementation, tier/cancelTiering methods,
 * cluster state update tasks, and metadata update logic
 * will be added in the implementation PR.
 */
public abstract class TieringService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TieringService.class);
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
    private Integer maxConcurrentTieringRequests;
    private Integer jvmActiveUsageThresholdPercent;
    // TierActionMetrics field will be added in the implementation PR
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
    // Placeholder constructor. Real constructor with settings update consumers will be added in the implementation PR.
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
        this.tieringIndices = new HashSet<>();
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterService.getClusterSettings());
        this.fileCacheSettings = new FileCacheSettings(settings, clusterService.getClusterSettings());
        this.shardLimitValidator = shardLimitValidator;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /** Returns the settings to add when tiering starts. @return the tiering start settings */
    protected abstract Settings getTieringStartSettingsToAdd();

    /** Returns the index tier settings to restore after cancellation. @return the settings to restore */
    protected abstract Settings getIndexTierSettingsToRestoreAfterCancellation();

    /** Returns the key for tiering start time. @return the tiering start time key */
    protected abstract String getTieringStartTimeKey();

    /** Returns the setting for max concurrent tiering requests. @return the max concurrent tiering requests setting */
    protected abstract Setting<Integer> getMaxConcurrentTieringRequestsSetting();

    /** Returns the target tiering state. @return the target tiering state */
    protected abstract IndexModule.TieringState getTargetTieringState();

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

    /** Returns the tiering type. @return the tiering type */
    protected abstract IndexModule.TieringState getTieringType();
}
