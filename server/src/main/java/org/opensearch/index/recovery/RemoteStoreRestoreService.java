/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.snapshots.RestoreService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;

/**
 * Service responsible for restoring index data from remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRestoreService {
    private static final Logger logger = LogManager.getLogger(RemoteStoreRestoreService.class);

    private final ClusterService clusterService;

    private final AllocationService allocationService;

    public RemoteStoreRestoreService(ClusterService clusterService, AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
    }

    public void restore(RestoreRemoteStoreRequest request, final ActionListener<RestoreService.RestoreCompletionResponse> listener) {
        clusterService.submitStateUpdateTask("restore[remote_store]", new ClusterStateUpdateTask() {
            final String restoreUUID = UUIDs.randomBase64UUID();
            RestoreInfo restoreInfo = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                // Updating cluster state
                ClusterState.Builder builder = ClusterState.builder(currentState);
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());

                List<String> indicesToBeRestored = new ArrayList<>();
                int totalShards = 0;
                for (String index : request.indices()) {
                    IndexMetadata currentIndexMetadata = currentState.metadata().index(index);
                    if (currentIndexMetadata == null) {
                        // ToDo: Handle index metadata does not exist case. (GitHub #3457)
                        logger.warn("Remote store restore is not supported for non-existent index. Skipping: {}", index);
                        continue;
                    }
                    if (currentIndexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false)) {
                        IndexMetadata updatedIndexMetadata = currentIndexMetadata;
                        if (request.restoreAllShards()) {
                            if (currentIndexMetadata.getState() != IndexMetadata.State.CLOSE) {
                                throw new IllegalStateException(
                                    "cannot restore index ["
                                        + index
                                        + "] because an open index "
                                        + "with same name already exists in the cluster. Close the existing index"
                                );
                            }
                            updatedIndexMetadata = IndexMetadata.builder(currentIndexMetadata)
                                .state(IndexMetadata.State.OPEN)
                                .version(1 + currentIndexMetadata.getVersion())
                                .mappingVersion(1 + currentIndexMetadata.getMappingVersion())
                                .settingsVersion(1 + currentIndexMetadata.getSettingsVersion())
                                .aliasesVersion(1 + currentIndexMetadata.getAliasesVersion())
                                .build();
                        }

                        Map<ShardId, IndexShardRoutingTable> indexShardRoutingTableMap = currentState.routingTable()
                            .index(index)
                            .shards()
                            .values()
                            .stream()
                            .collect(Collectors.toMap(IndexShardRoutingTable::shardId, Function.identity()));

                        IndexId indexId = new IndexId(index, updatedIndexMetadata.getIndexUUID());

                        RecoverySource.RemoteStoreRecoverySource recoverySource = new RecoverySource.RemoteStoreRecoverySource(
                            restoreUUID,
                            updatedIndexMetadata.getCreationVersion(),
                            indexId
                        );
                        rtBuilder.addAsRemoteStoreRestore(
                            updatedIndexMetadata,
                            recoverySource,
                            indexShardRoutingTableMap,
                            request.restoreAllShards()
                        );
                        blocks.updateBlocks(updatedIndexMetadata);
                        mdBuilder.put(updatedIndexMetadata, true);
                        indicesToBeRestored.add(index);
                        totalShards += updatedIndexMetadata.getNumberOfShards();
                    } else {
                        logger.warn("Remote store is not enabled for index: {}", index);
                    }
                }

                restoreInfo = new RestoreInfo("remote_store", indicesToBeRestored, totalShards, totalShards);

                RoutingTable rt = rtBuilder.build();
                ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
                return allocationService.reroute(updatedState, "restored from remote store");
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to restore from remote store", e);
                listener.onFailure(e);
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new RestoreService.RestoreCompletionResponse(restoreUUID, null, restoreInfo));
            }
        });

    }
}
