/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.remote.AbstractRemoteWritableBlobEntity;
import org.opensearch.common.remote.AbstractRemoteWritableEntityManager;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Locale;

/**
 * A Manager which provides APIs to write and read Index Metadata to remote store
 *
 * @opensearch.internal
 */
public class RemoteIndexMetadataManager extends AbstractRemoteWritableEntityManager {

    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;

    private volatile TimeValue indexMetadataUploadTimeout;

    public RemoteIndexMetadataManager(
        ClusterSettings clusterSettings,
        String clusterName,
        BlobStoreRepository blobStoreRepository,
        BlobStoreTransferService blobStoreTransferService,
        ThreadPool threadpool
    ) {
        this.remoteWritableEntityStores.put(
            RemoteIndexMetadata.INDEX,
            new RemoteClusterStateBlobStore<>(
                blobStoreTransferService,
                blobStoreRepository,
                clusterName,
                threadpool,
                ThreadPool.Names.REMOTE_STATE_READ
            )
        );
        this.namedXContentRegistry = blobStoreRepository.getNamedXContentRegistry();
        this.compressor = blobStoreRepository.getCompressor();
        this.indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
    }

    /**
     * Fetch index metadata from remote cluster state
     *
     * @param uploadedIndexMetadata {@link ClusterMetadataManifest.UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    IndexMetadata getIndexMetadata(ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata, String clusterUUID) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(
            RemoteClusterStateUtils.getFormattedIndexFileName(uploadedIndexMetadata.getUploadedFilename()),
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        try {
            return (IndexMetadata) getStore(remoteIndexMetadata).read(remoteIndexMetadata);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    public TimeValue getIndexMetadataUploadTimeout() {
        return this.indexMetadataUploadTimeout;
    }

    private void setIndexMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = newIndexMetadataUploadTimeout;
    }

    @Override
    protected ActionListener<Void> getWrappedWriteListener(
        String component,
        AbstractRemoteWritableBlobEntity remoteEntity,
        ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
    ) {
        return ActionListener.wrap(
            resp -> listener.onResponse(remoteEntity.getUploadedMetadata()),
            ex -> listener.onFailure(new RemoteStateTransferException("Upload failed for " + component, remoteEntity, ex))
        );
    }

    @Override
    protected ActionListener<Object> getWrappedReadListener(
        String component,
        AbstractRemoteWritableBlobEntity remoteEntity,
        ActionListener<RemoteReadResult> listener
    ) {
        return ActionListener.wrap(
            response -> listener.onResponse(new RemoteReadResult(response, RemoteIndexMetadata.INDEX, component)),
            ex -> listener.onFailure(new RemoteStateTransferException("Download failed for " + component, remoteEntity, ex))
        );
    }
}
