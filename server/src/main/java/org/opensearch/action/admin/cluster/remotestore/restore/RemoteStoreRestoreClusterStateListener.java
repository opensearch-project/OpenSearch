/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.index.shard.ShardId;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.snapshots.RestoreService;

import static org.opensearch.snapshots.RestoreService.restoreInProgress;

/**
 * Transport listener for cluster state updates
 *
 * @opensearch.internal
 */
public class RemoteStoreRestoreClusterStateListener implements ClusterStateListener {


    private static final Logger logger = LogManager.getLogger(RemoteStoreRestoreClusterStateListener.class);

    private final ClusterService clusterService;
    private final String uuid;
    private final ActionListener<RestoreRemoteStoreResponse> listener;

    private RemoteStoreRestoreClusterStateListener(
        ClusterService clusterService,
        RestoreService.RestoreCompletionResponse response,
        ActionListener<RestoreRemoteStoreResponse> listener
    ) {
        this.clusterService = clusterService;
        this.uuid = response.getUuid();
        this.listener = listener;

    }

    @Override
    public void clusterChanged(ClusterChangedEvent changedEvent) {
        final RestoreInProgress.Entry prevEntry = restoreInProgress(changedEvent.previousState(), uuid);
        final RestoreInProgress.Entry newEntry = restoreInProgress(changedEvent.state(), uuid);
        if (prevEntry == null) {
            // When there is a cluster-manager failure after a restore has been started, this listener might not be registered
            // on the current cluster-manager and as such it might miss some intermediary cluster states due to batching.
            // Clean up listener in that case and acknowledge completion of restore operation to client.
            clusterService.removeListener(this);
            listener.onResponse(new RestoreRemoteStoreResponse((RestoreInfo) null));
        } else if (newEntry == null) {
            clusterService.removeListener(this);
            ImmutableOpenMap < ShardId, RestoreInProgress.ShardRestoreStatus > shards = prevEntry.shards();
            assert prevEntry.state().completed() : "expected completed remote store restore state but was " + prevEntry.state();
            assert RestoreService.completed(shards) : "expected all restore entries to be completed";
            RestoreInfo ri = new RestoreInfo(
                "remote_store",
                prevEntry.indices(),
                shards.size(),
                +shards.size() - RestoreService.failedShards(shards)
            );
            RestoreRemoteStoreResponse response = new RestoreRemoteStoreResponse(ri);
            logger.debug("restore from remote store completed");
            listener.onResponse(response);
        } else {
            // restore not completed yet, wait for next cluster state update
        }
    }

    /**
     * Creates a cluster state listener and registers it with the cluster service. The listener passed as a
     * parameter will be called when the restore is complete.
     */
    public static void createAndRegisterListener(
        ClusterService clusterService,
        RestoreService.RestoreCompletionResponse response,
        ActionListener<RestoreRemoteStoreResponse> listener
    ) {
        clusterService.addListener(new RemoteStoreRestoreClusterStateListener(clusterService, response, listener));
    }
}
