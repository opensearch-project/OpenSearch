/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Provider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class PublishMergedSegmentActionProvider implements Provider<MergedSegmentPublisher.PublishAction> {

    private final Settings settings;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private final ShardStateAction shardStateAction;
    private final ActionFilters actionFilters;
    private final SegmentReplicationTargetService targetService;

    @Inject
    public PublishMergedSegmentActionProvider(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationTargetService targetService
    ) {
        this.settings = settings;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.threadPool = threadPool;
        this.shardStateAction = shardStateAction;
        this.actionFilters = actionFilters;
        this.targetService = targetService;
    }

    @Override
    public MergedSegmentPublisher.PublishAction get() {
        if (FeatureFlags.isEnabled(FeatureFlags.MERGED_SEGMENT_WARMER_EXPERIMENTAL_SETTING) == false) {
            return null;
        }
        // TODO: FIX THIS
        if (false) {// || clusterService.localNode().isRemoteStoreNode() == false) {
            return new PublishMergedSegmentAction(
                settings,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                targetService
            );
        } else {
            return new RemoteStorePublishMergedSegmentAction(
                settings,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                targetService
            );
        }
    }
}
