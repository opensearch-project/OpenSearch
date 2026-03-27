/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.storage.metrics.TierActionMetrics;

import java.util.Set;

/**
 * Service responsible for tiering indices from hot to warm.
 * validateTieringRequest, getTieringStartSettingsToAdd, getIndexTierSettingsToRestoreAfterCancellation,
 * and fileCacheActiveUsageThreshold management will be added in the implementation PR.
 */
public class HotToWarmTieringService extends TieringService {

    /**
     * The threshold percentage for file cache active usage when tiering from hot to warm.
     */
    private Integer fileCacheActiveUsageThresholdPercent;

    /**
     * Constructs a new HotToWarmTieringService.
     * @param settings the settings
     * @param clusterService the cluster service
     * @param clusterInfoService the cluster info service
     * @param indexNameExpressionResolver the index name expression resolver
     * @param allocationService the allocation service
     * @param tierActionMetrics the tier action metrics
     * @param nodeEnvironment the node environment
     * @param shardLimitValidator the shard limit validator
     */
    @Inject
    public HotToWarmTieringService(
        final Settings settings,
        final ClusterService clusterService,
        final ClusterInfoService clusterInfoService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AllocationService allocationService,
        final TierActionMetrics tierActionMetrics,
        final NodeEnvironment nodeEnvironment,
        final ShardLimitValidator shardLimitValidator
    ) {
        super(
            settings,
            clusterService,
            clusterInfoService,
            indexNameExpressionResolver,
            allocationService,
            tierActionMetrics,
            nodeEnvironment,
            shardLimitValidator
        );
    }

    @Override
    protected void validateTieringRequest(
        ClusterState clusterState,
        ClusterInfoService clusterInfoService,
        Set<Index> tieringEntries,
        Integer maxConcurrentTieringRequests,
        Integer jvmActiveUsageThresholdPercent,
        Index index
    ) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected Settings getTieringStartSettingsToAdd() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected Settings getIndexTierSettingsToRestoreAfterCancellation() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected Setting<Integer> getMaxConcurrentTieringRequestsSetting() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected String getTieringStartTimeKey() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected IndexModule.TieringState getTargetTieringState() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected IndexModule.TieringState getTieringType() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
