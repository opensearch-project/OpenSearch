/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.model.RemoteIndexMetadata.INDEX_PATH_TOKEN;

import java.io.IOException;
import java.util.Locale;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.model.RemoteIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteIndexMetadataBlobStore;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

public class RemoteIndexMetadataManager {

    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;

    private final BlobStoreTransferService blobStoreTransferService;
    private final RemoteIndexMetadataBlobStore indexMetadataBlobStore;

    private volatile TimeValue indexMetadataUploadTimeout;
    private final String clusterName;

    public RemoteIndexMetadataManager(BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings, ThreadPool threadPool, String clusterName,
        BlobStoreTransferService blobStoreTransferService) {
        indexMetadataBlobStore = new RemoteIndexMetadataBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.blobStoreRepository = blobStoreRepository;
        this.indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.threadPool = threadPool;
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
        this.blobStoreTransferService = blobStoreTransferService;
        this.clusterName = clusterName;
    }

    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param indexMetadata {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    CheckedRunnable<IOException> getIndexMetadataAsyncAction(IndexMetadata indexMetadata, String clusterUUID,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreRepository);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteIndexMetadata.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(indexMetadata.getIndex().getName(), ex))
        );
        return () -> indexMetadataBlobStore.writeAsync(remoteIndexMetadata, completionListener);
    }

    CheckedRunnable<IOException> getAsyncIndexMetadataReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<RemoteReadResult> latchedActionListener
    ) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(uploadedFilename, clusterUUID, blobStoreRepository);
        ActionListener<IndexMetadata> actionListener = ActionListener.wrap(
            response -> latchedActionListener.onResponse(new RemoteReadResult(response, INDEX_PATH_TOKEN, response.getIndexName())),
            latchedActionListener::onFailure);
        return () -> indexMetadataBlobStore.readAsync(remoteIndexMetadata, actionListener);
    }

    /**
     * Fetch index metadata from remote cluster state
     *
     * @param uploadedIndexMetadata {@link ClusterMetadataManifest.UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    IndexMetadata getIndexMetadata(
        ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata, String clusterUUID, int manifestCodecVersion
    ) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(RemoteClusterStateUtils.getFormattedFileName(
            uploadedIndexMetadata.getUploadedFilename(), manifestCodecVersion), clusterUUID, blobStoreRepository);
        try {
            return indexMetadataBlobStore.read(remoteIndexMetadata);
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

}
