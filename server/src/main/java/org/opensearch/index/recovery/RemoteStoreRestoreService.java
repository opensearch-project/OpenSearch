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
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.snapshots.RestoreService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

    private final MetadataCreateIndexService createIndexService;

    private final MetadataIndexUpgradeService metadataIndexUpgradeService;

    private final ShardLimitValidator shardLimitValidator;

    private final RemoteClusterStateService remoteClusterStateService;

    public RemoteStoreRestoreService(
        ClusterService clusterService,
        AllocationService allocationService,
        MetadataCreateIndexService createIndexService,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        ShardLimitValidator shardLimitValidator,
        RemoteClusterStateService remoteClusterStateService
    ) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.createIndexService = createIndexService;
        this.metadataIndexUpgradeService = metadataIndexUpgradeService;
        this.shardLimitValidator = shardLimitValidator;
        this.remoteClusterStateService = remoteClusterStateService;
    }

    /**
     * Restores data from remote store for indices specified in the restore request.
     *
     * @param request  restore request
     * @param listener restore listener
     */
    public void restore(RestoreRemoteStoreRequest request, final ActionListener<RestoreService.RestoreCompletionResponse> listener) {
        clusterService.submitStateUpdateTask("restore[remote_store]", new ClusterStateUpdateTask() {
            String restoreUUID;
            RestoreInfo restoreInfo = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                RemoteRestoreResult remoteRestoreResult = restore(currentState, null, request.restoreAllShards(), request.indices());
                restoreUUID = remoteRestoreResult.getRestoreUUID();
                restoreInfo = remoteRestoreResult.getRestoreInfo();
                return remoteRestoreResult.getClusterState();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to restore from remote store", e);
                listener.onFailure(e);
            }

            @Override
            public TimeValue timeout() {
                return request.clusterManagerNodeTimeout();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(new RestoreService.RestoreCompletionResponse(restoreUUID, null, restoreInfo));
            }
        });
    }

    /**
     * Executes remote restore
     * @param currentState current cluster state
     * @param restoreClusterUUID cluster UUID used to restore IndexMetadata
     * @param restoreAllShards indicates if all shards of the index needs to be restored. This flag is ignored if remoteClusterUUID is provided
     * @param indexNames list of indices to restore. This list is ignored if remoteClusterUUID is provided
     * @return remote restore result
     */
    public RemoteRestoreResult restore(
        ClusterState currentState,
        @Nullable String restoreClusterUUID,
        boolean restoreAllShards,
        String[] indexNames
    ) {
        Map<String, Tuple<Boolean, IndexMetadata>> indexMetadataMap = new HashMap<>();
        boolean metadataFromRemoteStore = (restoreClusterUUID == null
            || restoreClusterUUID.isEmpty()
            || restoreClusterUUID.isBlank()) == false;
        if (metadataFromRemoteStore) {
            try {
                remoteClusterStateService.getLatestIndexMetadata(currentState.getClusterName().value(), restoreClusterUUID)
                    .values()
                    .forEach(indexMetadata -> {
                        indexMetadataMap.put(indexMetadata.getIndex().getName(), new Tuple<>(true, indexMetadata));
                    });
            } catch (Exception e) {
                throw new IllegalStateException("Unable to restore remote index metadata", e);
            }
        } else {
            for (String indexName : indexNames) {
                IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                if (indexMetadata == null) {
                    logger.warn("Index restore is not supported for non-existent index. Skipping: {}", indexName);
                } else {
                    indexMetadataMap.put(indexName, new Tuple<>(false, indexMetadata));
                }
            }
        }
        validate(currentState, indexMetadataMap, restoreClusterUUID, restoreAllShards);
        return executeRestore(currentState, indexMetadataMap, restoreAllShards);
    }

    /**
     * Executes remote restore
     * @param currentState current cluster state
     * @param indexMetadataMap map of index metadata to restore
     * @param restoreAllShards indicates if all shards of the index needs to be restored
     * @return remote restore result
     */
    private RemoteRestoreResult executeRestore(
        ClusterState currentState,
        Map<String, Tuple<Boolean, IndexMetadata>> indexMetadataMap,
        boolean restoreAllShards
    ) {
        final String restoreUUID = UUIDs.randomBase64UUID();
        List<String> indicesToBeRestored = new ArrayList<>();
        int totalShards = 0;
        ClusterState.Builder builder = ClusterState.builder(currentState);
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        RoutingTable.Builder rtBuilder = RoutingTable.builder(currentState.routingTable());
        for (Map.Entry<String, Tuple<Boolean, IndexMetadata>> indexMetadataEntry : indexMetadataMap.entrySet()) {
            String indexName = indexMetadataEntry.getKey();
            IndexMetadata indexMetadata = indexMetadataEntry.getValue().v2();
            boolean metadataFromRemoteStore = indexMetadataEntry.getValue().v1();
            IndexMetadata updatedIndexMetadata = indexMetadata;
            if (metadataFromRemoteStore == false && restoreAllShards) {
                updatedIndexMetadata = IndexMetadata.builder(indexMetadata)
                    .state(IndexMetadata.State.OPEN)
                    .version(1 + indexMetadata.getVersion())
                    .mappingVersion(1 + indexMetadata.getMappingVersion())
                    .settingsVersion(1 + indexMetadata.getSettingsVersion())
                    .aliasesVersion(1 + indexMetadata.getAliasesVersion())
                    .build();
            }

            IndexId indexId = new IndexId(indexName, updatedIndexMetadata.getIndexUUID());

            Map<ShardId, IndexShardRoutingTable> indexShardRoutingTableMap = new HashMap<>();
            if (metadataFromRemoteStore == false) {
                indexShardRoutingTableMap = currentState.routingTable()
                    .index(indexName)
                    .shards()
                    .values()
                    .stream()
                    .collect(Collectors.toMap(IndexShardRoutingTable::shardId, Function.identity()));
            }

            RecoverySource.RemoteStoreRecoverySource recoverySource = new RecoverySource.RemoteStoreRecoverySource(
                restoreUUID,
                updatedIndexMetadata.getCreationVersion(),
                indexId
            );
            rtBuilder.addAsRemoteStoreRestore(
                updatedIndexMetadata,
                recoverySource,
                indexShardRoutingTableMap,
                restoreAllShards || metadataFromRemoteStore
            );
            blocks.updateBlocks(updatedIndexMetadata);
            mdBuilder.put(updatedIndexMetadata, true);
            indicesToBeRestored.add(indexName);
            totalShards += updatedIndexMetadata.getNumberOfShards();
        }

        RestoreInfo restoreInfo = new RestoreInfo("remote_store", indicesToBeRestored, totalShards, totalShards);

        RoutingTable rt = rtBuilder.build();
        ClusterState updatedState = builder.metadata(mdBuilder).blocks(blocks).routingTable(rt).build();
        return RemoteRestoreResult.build(restoreUUID, restoreInfo, allocationService.reroute(updatedState, "restored from remote store"));
    }

    /**
     * Performs various validations needed before executing restore
     * @param currentState current cluster state
     * @param indexMetadataMap map of index metadata to restore
     * @param restoreClusterUUID cluster UUID used to restore IndexMetadata
     * @param restoreAllShards indicates if all shards of the index needs to be restored. This flat is ignored if remoteClusterUUID is provided
     */
    private void validate(
        ClusterState currentState,
        Map<String, Tuple<Boolean, IndexMetadata>> indexMetadataMap,
        @Nullable String restoreClusterUUID,
        boolean restoreAllShards
    ) throws IllegalStateException, IllegalArgumentException {
        String errorMsg = "cannot restore index [%s] because an open index with same name/uuid already exists in the cluster.";

        // Restore with current cluster UUID will fail as same indices would be present in the cluster which we are trying to
        // restore
        if (currentState.metadata().clusterUUID().equals(restoreClusterUUID)) {
            throw new IllegalArgumentException("clusterUUID to restore from should be different from current cluster UUID");
        }
        for (Map.Entry<String, Tuple<Boolean, IndexMetadata>> indexMetadataEntry : indexMetadataMap.entrySet()) {
            String indexName = indexMetadataEntry.getKey();
            IndexMetadata indexMetadata = indexMetadataEntry.getValue().v2();
            String indexUUID = indexMetadata.getIndexUUID();
            boolean metadataFromRemoteStore = indexMetadataEntry.getValue().v1();
            if (indexMetadata.getSettings().getAsBoolean(SETTING_REMOTE_STORE_ENABLED, false)) {
                if (metadataFromRemoteStore) {
                    Set<String> graveyardIndexNames = new HashSet<>();
                    Set<String> graveyardIndexUUID = new HashSet<>();
                    Set<String> liveClusterIndexUUIDs = currentState.metadata()
                        .indices()
                        .values()
                        .stream()
                        .map(IndexMetadata::getIndexUUID)
                        .collect(Collectors.toSet());

                    currentState.metadata().indexGraveyard().getTombstones().forEach(tombstone -> {
                        graveyardIndexNames.add(tombstone.getIndex().getName());
                        graveyardIndexUUID.add(tombstone.getIndex().getUUID());
                    });

                    // Since updates to graveyard are synced to remote we should neven land in a situation where remote contain index
                    // metadata for graveyard index.
                    assert graveyardIndexNames.contains(indexName) == false : String.format(
                        Locale.ROOT,
                        "Index name [%s] exists in graveyard!",
                        indexName
                    );
                    assert graveyardIndexUUID.contains(indexUUID) == false : String.format(
                        Locale.ROOT,
                        "Index UUID [%s] exists in graveyard!",
                        indexUUID
                    );

                    // Any indices being restored from remote cluster state should not already be part of the cluster as this causes
                    // conflict
                    boolean sameNameIndexExists = currentState.metadata().hasIndex(indexName);
                    boolean sameUUIDIndexExists = liveClusterIndexUUIDs.contains(indexUUID);
                    if (sameNameIndexExists || sameUUIDIndexExists) {
                        String finalErrorMsg = String.format(Locale.ROOT, errorMsg, indexName);
                        logger.info(finalErrorMsg);
                        throw new IllegalStateException(finalErrorMsg);
                    }

                    Version minIndexCompatibilityVersion = currentState.getNodes().getMaxNodeVersion().minimumIndexCompatibilityVersion();
                    metadataIndexUpgradeService.upgradeIndexMetadata(indexMetadata, minIndexCompatibilityVersion);
                    boolean isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.getSettings());
                    createIndexService.validateIndexName(indexName, currentState);
                    createIndexService.validateDotIndex(indexName, isHidden);
                    shardLimitValidator.validateShardLimit(indexName, indexMetadata.getSettings(), currentState);
                } else if (restoreAllShards && IndexMetadata.State.CLOSE.equals(indexMetadata.getState()) == false) {
                    throw new IllegalStateException(String.format(Locale.ROOT, errorMsg, indexName) + " Close the existing index.");
                }
            } else {
                logger.warn("Remote store is not enabled for index: {}", indexName);
            }
        }
    }

    /**
     * Result of a remote restore operation.
     */
    public static class RemoteRestoreResult {
        private final ClusterState clusterState;
        private final RestoreInfo restoreInfo;
        private final String restoreUUID;

        private RemoteRestoreResult(String restoreUUID, RestoreInfo restoreInfo, ClusterState clusterState) {
            this.clusterState = clusterState;
            this.restoreInfo = restoreInfo;
            this.restoreUUID = restoreUUID;
        }

        public static RemoteRestoreResult build(String restoreUUID, RestoreInfo restoreInfo, ClusterState clusterState) {
            return new RemoteRestoreResult(restoreUUID, restoreInfo, clusterState);
        }

        public ClusterState getClusterState() {
            return clusterState;
        }

        public RestoreInfo getRestoreInfo() {
            return restoreInfo;
        }

        public String getRestoreUUID() {
            return restoreUUID;
        }
    }
}
