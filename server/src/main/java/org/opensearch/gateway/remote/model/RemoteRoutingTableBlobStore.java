/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.opensearch.common.remote.RemoteWriteableBlobEntity;
import org.opensearch.common.remote.RemoteWriteableEntity;
import org.opensearch.common.remote.RemoteWriteableEntityBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;

/**
 * Extends the RemoteClusterStateBlobStore to support {@link RemoteIndexRoutingTable}
 *
 * @param <IndexRoutingTable> which can be uploaded to / downloaded from blob store
 * @param <U> The concrete class implementing {@link RemoteWriteableEntity} which is used as a wrapper for IndexRoutingTable entity.
 */
public class RemoteRoutingTableBlobStore<IndexRoutingTable, U extends AbstractClusterMetadataWriteableBlobEntity<IndexRoutingTable>> extends
    RemoteWriteableEntityBlobStore<IndexRoutingTable, U> {

    /**
     * This setting is used to set the remote routing table store blob store path type strategy.
     */
    public static final Setting<RemoteStoreEnums.PathType> REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING = new Setting<>(
        "cluster.remote_store.routing_table.path_type",
        RemoteStoreEnums.PathType.HASHED_PREFIX.toString(),
        RemoteStoreEnums.PathType::parseString,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting is used to set the remote routing table store blob store path hash algorithm strategy.
     * This setting will come to effect if the {@link #REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING}
     * is either {@code HASHED_PREFIX} or {@code HASHED_INFIX}.
     */
    public static final Setting<RemoteStoreEnums.PathHashAlgorithm> REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING = new Setting<>(
        "cluster.remote_store.routing_table.path_hash_algo",
        RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.toString(),
        RemoteStoreEnums.PathHashAlgorithm::parseString,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private RemoteStoreEnums.PathType pathType;
    private RemoteStoreEnums.PathHashAlgorithm pathHashAlgo;

    public RemoteRoutingTableBlobStore(
        BlobStoreTransferService blobStoreTransferService,
        BlobStoreRepository blobStoreRepository,
        String clusterName,
        ThreadPool threadPool,
        String executor,
        ClusterSettings clusterSettings
    ) {
        super(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            threadPool,
            executor,
            RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN
        );
        this.pathType = clusterSettings.get(REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING);
        this.pathHashAlgo = clusterSettings.get(REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING, this::setPathTypeSetting);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_ROUTING_TABLE_PATH_HASH_ALGO_SETTING, this::setPathHashAlgoSetting);
    }

    @Override
    public BlobPath getBlobPathForUpload(final RemoteWriteableBlobEntity<IndexRoutingTable> obj) {
        assert obj.getBlobPathParameters().getPathTokens().size() == 1 : "Unexpected tokens in RemoteRoutingTableObject";
        BlobPath indexRoutingPath = getBlobPathPrefix(obj.clusterUUID()).add(INDEX_ROUTING_TABLE);

        BlobPath path = pathType.path(
            RemoteStorePathStrategy.BasePathInput.builder()
                .basePath(indexRoutingPath)
                .indexUUID(String.join("", obj.getBlobPathParameters().getPathTokens()))
                .build(),
            pathHashAlgo
        );
        return path;
    }

    private void setPathTypeSetting(RemoteStoreEnums.PathType pathType) {
        this.pathType = pathType;
    }

    private void setPathHashAlgoSetting(RemoteStoreEnums.PathHashAlgorithm pathHashAlgo) {
        this.pathHashAlgo = pathHashAlgo;
    }

    // For testing only
    protected RemoteStoreEnums.PathType getPathTypeSetting() {
        return pathType;
    }

    // For testing only
    protected RemoteStoreEnums.PathHashAlgorithm getPathHashAlgoSetting() {
        return pathHashAlgo;
    }
}
