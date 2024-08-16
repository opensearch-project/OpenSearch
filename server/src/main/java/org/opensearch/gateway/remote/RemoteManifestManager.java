/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteClusterStateBlobStore;
import org.opensearch.gateway.remote.model.RemoteClusterStateManifestInfo;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * A Manager which provides APIs to write and read {@link ClusterMetadataManifest} to remote store
 *
 * @opensearch.internal
 */
public class RemoteManifestManager {

    public static final TimeValue METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.metadata_manifest.upload_timeout",
        METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(RemoteManifestManager.class);

    private volatile TimeValue metadataManifestUploadTimeout;
    private final String nodeId;
    private final RemoteClusterStateBlobStore<ClusterMetadataManifest, RemoteClusterMetadataManifest> manifestBlobStore;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;
    // todo remove blobStorerepo from here
    private final BlobStoreRepository blobStoreRepository;

    RemoteManifestManager(
        ClusterSettings clusterSettings,
        String clusterName,
        String nodeId,
        BlobStoreRepository blobStoreRepository,
        BlobStoreTransferService blobStoreTransferService,
        ThreadPool threadpool
    ) {
        this.metadataManifestUploadTimeout = clusterSettings.get(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING);
        this.nodeId = nodeId;
        this.manifestBlobStore = new RemoteClusterStateBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            threadpool,
            ThreadPool.Names.REMOTE_STATE_READ
        );
        ;
        clusterSettings.addSettingsUpdateConsumer(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING, this::setMetadataManifestUploadTimeout);
        this.compressor = blobStoreRepository.getCompressor();
        this.namedXContentRegistry = blobStoreRepository.getNamedXContentRegistry();
        this.blobStoreRepository = blobStoreRepository;
    }

    RemoteClusterStateManifestInfo uploadManifest(
        ClusterState clusterState,
        RemoteClusterStateUtils.UploadedMetadataResults uploadedMetadataResult,
        String previousClusterUUID,
        ClusterStateDiffManifest clusterDiffManifest,
        ClusterStateChecksum clusterStateChecksum,
        boolean committed
    ) {
        synchronized (this) {
            ClusterMetadataManifest.Builder manifestBuilder = ClusterMetadataManifest.builder();
            manifestBuilder.clusterTerm(clusterState.term())
                .stateVersion(clusterState.getVersion())
                .clusterUUID(clusterState.metadata().clusterUUID())
                .stateUUID(clusterState.stateUUID())
                .opensearchVersion(Version.CURRENT)
                .nodeId(nodeId)
                .committed(committed)
                .codecVersion(RemoteClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION)
                .indices(uploadedMetadataResult.uploadedIndexMetadata)
                .previousClusterUUID(previousClusterUUID)
                .clusterUUIDCommitted(clusterState.metadata().clusterUUIDCommitted())
                .coordinationMetadata(uploadedMetadataResult.uploadedCoordinationMetadata)
                .settingMetadata(uploadedMetadataResult.uploadedSettingsMetadata)
                .templatesMetadata(uploadedMetadataResult.uploadedTemplatesMetadata)
                .customMetadataMap(uploadedMetadataResult.uploadedCustomMetadataMap)
                .routingTableVersion(clusterState.getRoutingTable().version())
                .indicesRouting(uploadedMetadataResult.uploadedIndicesRoutingMetadata)
                .discoveryNodesMetadata(uploadedMetadataResult.uploadedDiscoveryNodes)
                .clusterBlocksMetadata(uploadedMetadataResult.uploadedClusterBlocks)
                .diffManifest(clusterDiffManifest)
                .metadataVersion(clusterState.metadata().version())
                .transientSettingsMetadata(uploadedMetadataResult.uploadedTransientSettingsMetadata)
                .clusterStateCustomMetadataMap(uploadedMetadataResult.uploadedClusterStateCustomMetadataMap)
                .hashesOfConsistentSettings(uploadedMetadataResult.uploadedHashesOfConsistentSettings)
                .checksum(clusterStateChecksum);
            final ClusterMetadataManifest manifest = manifestBuilder.build();
            logger.trace("uploading manifest [{}]", manifest);
            logger.trace(() -> new ParameterizedMessage("[{}] uploading manifest", manifest));
            String manifestFileName = writeMetadataManifest(clusterState.metadata().clusterUUID(), manifest);
            return new RemoteClusterStateManifestInfo(manifest, manifestFileName);
        }
    }

    private String writeMetadataManifest(String clusterUUID, ClusterMetadataManifest uploadManifest) {
        AtomicReference<String> result = new AtomicReference<String>();
        AtomicReference<Exception> exceptionReference = new AtomicReference<Exception>();

        // latch to wait until upload is not finished
        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener completionListener = new LatchedActionListener<>(ActionListener.wrap(resp -> {
            logger.trace(String.format(Locale.ROOT, "Manifest file uploaded successfully."));
        }, ex -> { exceptionReference.set(ex); }), latch);

        RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(
            uploadManifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        manifestBlobStore.writeAsync(remoteClusterMetadataManifest, completionListener);

        try {
            if (latch.await(getMetadataManifestUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete")
                );
                throw ex;
            }
        } catch (InterruptedException ex) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete - %s"),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (exceptionReference.get() != null) {
            throw new RemoteStateTransferException(exceptionReference.get().getMessage(), exceptionReference.get());
        }
        logger.debug(
            "Metadata manifest file [{}] written during [{}] phase. ",
            remoteClusterMetadataManifest.getBlobFileName(),
            uploadManifest.isCommitted() ? "commit" : "publish"
        );
        return remoteClusterMetadataManifest.getUploadedMetadata().getUploadedFilename();
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        return latestManifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    public ClusterMetadataManifest getRemoteClusterMetadataManifestByFileName(String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(
                filename,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            return manifestBlobStore.read(remoteClusterMetadataManifest);
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            String fullBlobName = getManifestFolderPath(clusterName, clusterUUID).buildAsString() + filename;
            RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(
                fullBlobName,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            return manifestBlobStore.read(remoteClusterMetadataManifest);
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID),
                    e
                );
            }
        }
        return manifestsByClusterUUID;
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore().blobContainer(getManifestFolderPath(clusterName, clusterUUID));
    }

    BlobPath getManifestFolderPath(String clusterName, String clusterUUID) {
        return RemoteClusterStateUtils.getClusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID)
            .add(RemoteClusterMetadataManifest.MANIFEST);
    }

    public TimeValue getMetadataManifestUploadTimeout() {
        return this.metadataManifestUploadTimeout;
    }

    private void setMetadataManifestUploadTimeout(TimeValue newMetadataManifestUploadTimeout) {
        this.metadataManifestUploadTimeout = newMetadataManifestUploadTimeout;
    }

    /**
     * Fetch ClusterMetadataManifest files from remote state store in order
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param limit max no of files to fetch
     * @return all manifest file names
     */
    private List<BlobMetadata> getManifestFileNames(String clusterName, String clusterUUID, String filePrefix, int limit)
        throws IllegalStateException {
        try {

            /*
              {@link BlobContainer#listBlobsByPrefixInSortedOrder} will list the latest manifest file first
              as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
              when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            return manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                filePrefix,
                limit,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }
    }

    static String getManifestFilePrefixForTermVersion(long term, long version) {
        return String.join(
            DELIMITER,
            RemoteClusterMetadataManifest.MANIFEST,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version)
        ) + DELIMITER;
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private Optional<String> getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(
            clusterName,
            clusterUUID,
            RemoteClusterMetadataManifest.MANIFEST + DELIMITER,
            1
        );
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }
}
