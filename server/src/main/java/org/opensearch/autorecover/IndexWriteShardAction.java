/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autorecover;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.client.Client;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestDeduplicator;

import java.io.IOException;

public class IndexWriteShardAction implements IndexingOperationListener {
    private static final Logger logger = LogManager.getLogger(IndexWriteShardAction.class);

    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;


    /**
     * Setting that specifies if auto recover of red indices from past snapshots is enabled or not
     */
    public static final Setting<Boolean> AUTO_RESTORE_RED_INDICES_ENABLE =
        Setting.boolSetting("snapshot.auto_restore_red_indices_enable",false,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Setting that specifies if auto recover of red indices on data loss or not
     */
    public static final Setting<Boolean> AUTO_RESTORE_RED_INDICES_IF_NO_DATA_LOSS =
        Setting.boolSetting("snapshot.auto_restore_red_indices_if_no_data_loss",true,
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    /**
     * Setting that specifies from which repository to restore red indices
     */
    public static final Setting<String> AUTO_RESTORE_RED_INDICES_REPOSITORY =
        Setting.simpleString("snapshot.auto_restore_red_indices_repository",
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final TransportRequestDeduplicator<UpdateIndexCleanStatusRequest> remoteFailedRequestDeduplicator =
        new TransportRequestDeduplicator<>();

    public IndexWriteShardAction(ClusterService clusterService, Client client, ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) throws IOException{
        boolean autoRestoreRedIndexEnabled = clusterService.getClusterSettings().get(AUTO_RESTORE_RED_INDICES_ENABLE);
        if (!autoRestoreRedIndexEnabled) {
            // No Action Needed
            return;
        }

        ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest().local(true);
        ClusterState state = client.admin().cluster().state(clusterStateRequest).actionGet().getState();
        if (!state.routingTable().shardRoutingTable(shardId).primaryShard().currentNodeId().equals(clusterService.localNode().getId())) {
            // No Action Needed, Index setting update happens only on primary shard
            return;
        }

        SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);

        if (snapshotsInProgress.equals(SnapshotsInProgress.EMPTY)) {
            logger.trace("No Snapshot in progress during index write");
            updateIndexSetting(shardId);

        } else {
            logger.trace(" Snapshot in progress during index write:[{}]", snapshotsInProgress);
            updateIndexSettingWithSnapshotInProgress(shardId, snapshotsInProgress);
        }
    }

    public void updateIndexSetting(ShardId shardId) {
        Settings indexSettings = client.admin().cluster().prepareState().get().getState().metadata().index(
            shardId.getIndex().getName()).getSettings();
        boolean index_clean_to_restore_from_snapshot = indexSettings.getAsBoolean(
            IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(), Boolean.FALSE);
        long index_clean_to_restore_from_snapshot_update_time = indexSettings.getAsLong(
            IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.getKey(), -1L);

        if(index_clean_to_restore_from_snapshot || index_clean_to_restore_from_snapshot_update_time == -1L) {
            // TODO: Implement Batching
            client.admin().indices().prepareUpdateSettings(shardId.getIndexName())
                .setSettings(Settings.builder()
                    .put(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(), false)
                    .put(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.getKey(), threadPool.absoluteTimeInMillis())
                ).get();
            logger.info("[{}] updated index_clean_to_restore_from_snapshot to [{}] & index_clean_to_restore_from_snapshot_update_time" +
                    " to [{}]", shardId, index_clean_to_restore_from_snapshot, index_clean_to_restore_from_snapshot_update_time);
        }
    }


    public void updateIndexSettingWithSnapshotInProgress(ShardId shardId, SnapshotsInProgress snapshotsInProgress) {
        Settings indexSettings = client.admin().cluster().prepareState().get().getState().metadata().index(
            shardId.getIndex().getName()).getSettings();
        boolean index_clean_to_restore_from_snapshot = indexSettings.getAsBoolean(
            IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(), Boolean.FALSE);
        long index_clean_to_restore_from_snapshot_update_time = indexSettings.getAsLong(
            IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.getKey(), -1L);
        String autoRestoreRedIndexRepositoryName = clusterService.getClusterSettings().get(AUTO_RESTORE_RED_INDICES_REPOSITORY);

        if(index_clean_to_restore_from_snapshot || (index_clean_to_restore_from_snapshot_update_time != -1 &&
            index_clean_to_restore_from_snapshot_update_time < getLatestSnapshotStartTime(snapshotsInProgress,
                autoRestoreRedIndexRepositoryName))) {
            // TODO: Implement Batching
            client.admin().indices().prepareUpdateSettings(shardId.getIndexName())
                .setSettings(Settings.builder()
                    .put(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT.getKey(), false)
                    .put(IndexSettings.INDEX_CLEAN_TO_RESTORE_FROM_SNAPSHOT_UPDATE_TIME.getKey(), threadPool.absoluteTimeInMillis())
                ).get();
            logger.info("[{}] Found [{}], updated index_clean_to_restore_from_snapshot to [{}] & " +
                    "index_clean_to_restore_from_snapshot_update_time to [{}]", shardId, snapshotsInProgress,
                index_clean_to_restore_from_snapshot, index_clean_to_restore_from_snapshot_update_time);
        }
    }

    private long getLatestSnapshotStartTime(SnapshotsInProgress snapshotsInProgress, String repositoryName) {
        long latestSnapshotStartTime = -1;

        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.repository().equals(repositoryName) && entry.startTime() > latestSnapshotStartTime) {
                latestSnapshotStartTime = entry.startTime();
            }
        }
        return  latestSnapshotStartTime;
    }
}
