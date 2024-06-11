/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.remote.RemoteWritableEntityStore;
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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A Manager which provides APIs to write and read Index Metadata to remote store
 *
 * @opensearch.internal
 */
public class RemoteIndexMetadataManager {

    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private final RemoteWritableEntityStore<IndexMetadata, RemoteIndexMetadata> indexMetadataBlobStore;
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
        this.indexMetadataBlobStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            threadpool,
            ThreadPool.Names.REMOTE_STATE_READ
        );
        this.namedXContentRegistry = blobStoreRepository.getNamedXContentRegistry();
        this.compressor = blobStoreRepository.getCompressor();
        this.indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
    }

    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param indexMetadata {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    CheckedRunnable<IOException> getAsyncIndexMetadataWriteAction(
        IndexMetadata indexMetadata,
        String clusterUUID,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(indexMetadata, clusterUUID, compressor, namedXContentRegistry);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(remoteIndexMetadata.getUploadedMetadata()),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(indexMetadata.getIndex().getName(), ex))
        );
        return () -> indexMetadataBlobStore.writeAsync(remoteIndexMetadata, completionListener);
    }

    CheckedRunnable<IOException> getAsyncIndexMetadataReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<RemoteReadResult> latchedActionListener
    ) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(
            RemoteClusterStateUtils.getFormattedIndexFileName(uploadedFilename),
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        ActionListener<IndexMetadata> actionListener = ActionListener.wrap(
            response -> latchedActionListener.onResponse(
                new RemoteReadResult(response, RemoteIndexMetadata.INDEX, response.getIndex().getName())
            ),
            latchedActionListener::onFailure
        );
        return () -> indexMetadataBlobStore.readAsync(remoteIndexMetadata, actionListener);
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
            return indexMetadataBlobStore.read(remoteIndexMetadata);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    /**
     * Fetch latest index metadata from remote cluster state
     *
     * @param clusterMetadataManifest manifest file of cluster
     * @param clusterUUID             uuid of cluster state to refer to in remote
     * @return {@code Map<String, IndexMetadata>} latest IndexUUID to IndexMetadata map
     */
    Map<String, IndexMetadata> getIndexMetadataMap(String clusterUUID, ClusterMetadataManifest clusterMetadataManifest) {
        assert Objects.equals(clusterUUID, clusterMetadataManifest.getClusterUUID())
            : "Corrupt ClusterMetadataManifest found. Cluster UUID mismatch.";
        Map<String, IndexMetadata> remoteIndexMetadata = new HashMap<>();
        for (ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata : clusterMetadataManifest.getIndices()) {
            IndexMetadata indexMetadata = getIndexMetadata(uploadedIndexMetadata, clusterUUID);
            remoteIndexMetadata.put(uploadedIndexMetadata.getIndexUUID(), indexMetadata);
        }
        return remoteIndexMetadata;
    }

    public TimeValue getIndexMetadataUploadTimeout() {
        return this.indexMetadataUploadTimeout;
    }

    private void setIndexMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = newIndexMetadataUploadTimeout;
    }

}
