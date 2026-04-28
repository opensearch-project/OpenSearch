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

import java.util.Set;

import static org.opensearch.index.IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.IndexModule.TieringState.HOT;
import static org.opensearch.index.IndexModule.TieringState.WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM_TO_HOT;
import static org.opensearch.storage.common.tiering.TieringServiceValidator.validateWarmToHotTiering;
import static org.opensearch.storage.common.tiering.TieringUtils.TIERED_COMPOSITE_INDEX_TYPE;
import static org.opensearch.storage.common.tiering.TieringUtils.W2H_MAX_CONCURRENT_TIERING_REQUESTS;
import static org.opensearch.storage.common.tiering.TieringUtils.W2H_TIERING_START_TIME_KEY;

/**
 * Service responsible for tiering indices from warm to hot.
 */
public class WarmToHotTieringService extends TieringService {

    /**
     * Constructs a new WarmToHotTieringService.
     * @param settings the settings
     * @param clusterService the cluster service
     * @param clusterInfoService the cluster info service
     * @param indexNameExpressionResolver the index name expression resolver
     * @param allocationService the allocation service
     * @param nodeEnvironment the node environment
     * @param shardLimitValidator the shard limit validator
     */
    @Inject
    public WarmToHotTieringService(
        final Settings settings,
        final ClusterService clusterService,
        final ClusterInfoService clusterInfoService,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final AllocationService allocationService,
        final NodeEnvironment nodeEnvironment,
        final ShardLimitValidator shardLimitValidator
    ) {
        super(
            settings,
            clusterService,
            clusterInfoService,
            indexNameExpressionResolver,
            allocationService,
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
        validateWarmToHotTiering(
            clusterState,
            clusterInfoService.getClusterInfo(),
            tieringEntries,
            maxConcurrentTieringRequests,
            jvmActiveUsageThresholdPercent,
            index,
            shardLimitValidator
        );
    }

    @Override
    protected Settings getTieringStartSettingsToAdd() {
        return Settings.builder()
            .put(IS_WARM_INDEX_SETTING.getKey(), false)
            .put(INDEX_TIERING_STATE.getKey(), WARM_TO_HOT)
            .put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "default")
            .build();
    }

    @Override
    protected Settings getIndexTierSettingsToRestoreAfterCancellation() {
        return Settings.builder()
            .put(IS_WARM_INDEX_SETTING.getKey(), true)
            .put(INDEX_TIERING_STATE.getKey(), WARM)
            .put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), TIERED_COMPOSITE_INDEX_TYPE)
            .build();
    }

    @Override
    protected String getTieringStartTimeKey() {
        return W2H_TIERING_START_TIME_KEY;
    }

    @Override
    protected Setting<Integer> getMaxConcurrentTieringRequestsSetting() {
        return W2H_MAX_CONCURRENT_TIERING_REQUESTS;
    }

    @Override
    protected IndexModule.TieringState getTargetTieringState() {
        return HOT;
    }

    @Override
    protected IndexModule.TieringState getTieringType() {
        return WARM_TO_HOT;
    }
}
