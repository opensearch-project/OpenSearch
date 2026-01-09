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
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.remote.RemoteWriteableEntityBlobStore;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteIndexMetadataManifest;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;

/**
 * Manager for IndexMetadataManifest operations
 *
 * @opensearch.internal
 */
public class IndexMetadataManifestManager {

    private static final Logger logger = LogManager.getLogger(IndexMetadataManifestManager.class);

    private final String nodeId;
    private final RemoteWriteableEntityBlobStore<IndexMetadataManifest, RemoteIndexMetadataManifest> indexManifestBlobStore;
    private final Compressor compressor;
    private final NamedXContentRegistry namedXContentRegistry;
    private final BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadpool;
    private volatile String lastUploadedIndexManifestVersion;

    public IndexMetadataManifestManager(
        String clusterName,
        String nodeId,
        BlobStoreRepository blobStoreRepository,
        BlobStoreTransferService blobStoreTransferService,
        ThreadPool threadpool
    ) {
        this.nodeId = nodeId;
        this.indexManifestBlobStore = new RemoteWriteableEntityBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            threadpool,
            ThreadPool.Names.REMOTE_STATE_READ,
            RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN
        );
        this.compressor = blobStoreRepository.getCompressor();
        this.namedXContentRegistry = blobStoreRepository.getNamedXContentRegistry();
        this.blobStoreRepository = blobStoreRepository;
        this.threadpool = threadpool;
    }

    /**
     * Upload IndexMetadataManifest independently when index metadata changes
     */
    public String uploadIndexMetadataManifest(
        ClusterState clusterState,
        ClusterState previousClusterState,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        int indexMetadataVersion
    ) throws IOException {

        IndexStateDiffManifest indexDiffManifest = null;
        if (previousClusterState != null) {
            List<String> indicesUpdated = new ArrayList<>();
            List<String> indicesDeleted = new ArrayList<>();

            for (UploadedIndexMetadata indexMetadata : uploadedIndexMetadata) {
                indicesUpdated.add(indexMetadata.getIndexName());
            }

            for (String indexName : previousClusterState.metadata().indices().keySet()) {
                if (!clusterState.metadata().indices().containsKey(indexName)) {
                    indicesDeleted.add(indexName);
                }
            }

            indexDiffManifest = new IndexStateDiffManifest(
                previousClusterState.getVersion(),
                clusterState.getVersion(),
                indicesUpdated,
                indicesDeleted
            );
        }

        IndexMetadataManifest indexManifest = IndexMetadataManifest.builder()
            .opensearchVersion(Version.CURRENT)
            .codecVersion(IndexMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION)
            .indices(uploadedIndexMetadata)
            .manifestVersion(indexMetadataVersion)
            .indexDiffManifest(indexDiffManifest)
            .build();

        return writeIndexMetadataManifest(clusterState.metadata().clusterUUID(), indexManifest);
    }

    private String writeIndexMetadataManifest(String clusterUUID, IndexMetadataManifest indexManifest) throws IOException {
        RemoteIndexMetadataManifest remoteIndexManifest = new RemoteIndexMetadataManifest(
            indexManifest,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );

        String newManifestVersion;
        try {
            if (lastUploadedIndexManifestVersion != null) {
                newManifestVersion = indexManifestBlobStore.conditionallyUpdateVersionedBlob(
                    remoteIndexManifest,
                    lastUploadedIndexManifestVersion
                );
            } else {
                newManifestVersion = indexManifestBlobStore.writeVersionedBlob(remoteIndexManifest);
            }
        } catch (IOException e) {
            if (e.getMessage() != null && e.getMessage().contains("Version conflict")) {
                throw new RemoteStateVersionConflictException(
                    String.format(
                        Locale.ROOT,
                        "Version conflict while uploading index metadata manifest. Expected version: %s",
                        lastUploadedIndexManifestVersion
                    ),
                    e
                );
            }
            throw new RemoteStateTransferException("Failed to upload index metadata manifest", e);
        }

        lastUploadedIndexManifestVersion = newManifestVersion;
        logger.debug("Updated index metadata manifest version: {}", newManifestVersion);

        return newManifestVersion;
    }

    /**
     * Get latest IndexMetadataManifest from remote store
     */
    public Optional<IndexMetadataManifest> getLatestIndexMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestIndexManifestFileName(clusterName, clusterUUID);
        return latestManifestFileName.map(s -> fetchRemoteIndexMetadataManifest(clusterName, clusterUUID, s));
    }

    /**
     * Fetch IndexMetadataManifest by filename
     */
    public IndexMetadataManifest getRemoteIndexMetadataManifestByFileName(String clusterUUID, String filename) {
        try {
            RemoteIndexMetadataManifest remoteIndexManifest = new RemoteIndexMetadataManifest(
                filename,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            Tuple<IndexMetadataManifest, String> manifestByVersion = indexManifestBlobStore.readWithVersion(remoteIndexManifest);
            IndexMetadataManifest manifest = manifestByVersion.v1();
            lastUploadedIndexManifestVersion = manifestByVersion.v2();
            return manifest;
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading index metadata manifest - %s", filename),
                e
            );
        }
    }

    private IndexMetadataManifest fetchRemoteIndexMetadataManifest(String clusterName, String clusterUUID, String filename) {
        try {
            RemoteIndexMetadataManifest remoteIndexManifest = new RemoteIndexMetadataManifest(
                filename,
                clusterUUID,
                compressor,
                namedXContentRegistry
            );
            Tuple<IndexMetadataManifest, String> manifestByVersion = indexManifestBlobStore.readWithVersion(remoteIndexManifest);
            IndexMetadataManifest manifest = manifestByVersion.v1();
            lastUploadedIndexManifestVersion = manifestByVersion.v2();
            return manifest;
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading index metadata manifest - %s", filename),
                e
            );
        }
    }

    private Optional<String> getLatestIndexManifestFileName(String clusterName, String clusterUUID) {
        try {
            BlobContainer manifestContainer = blobStoreRepository.blobStore().blobContainer(
                getIndexManifestFolderPath(clusterName, clusterUUID)
            );

            var allBlobs = manifestContainer.listBlobs().keySet();
            logger.debug("All blob files found: {}", allBlobs);

            var filteredBlobs = allBlobs.stream()
                .filter(fileName -> fileName.contains("index-metadata-manifest"))
                .collect(java.util.stream.Collectors.toList());
            logger.debug("Filtered index_metadata_manifest files: {}", filteredBlobs);

            return filteredBlobs.stream().max(String::compareTo);
        } catch (IOException e) {
            logger.error("Error while fetching latest index manifest file for cluster {}", clusterName, e);
            return Optional.empty();
        }
    }

    private BlobPath getIndexManifestFolderPath(String clusterName, String clusterUUID) {
        return blobStoreRepository.basePath()
            .add(RemoteClusterStateUtils.encodeString(clusterName))
            .add("cluster-state").add("index-metadata-manifest");

    }

    public void setLastUploadedIndexManifestVersion(String version) {
        this.lastUploadedIndexManifestVersion = version;
    }

    public String getLastUploadedIndexManifestVersion() {
        return lastUploadedIndexManifestVersion;
    }


    public BlobPath getManifestPath() {
        BlobPath blobPath = indexManifestBlobStore.getBlobPathPrefix(null, true);
        blobPath = blobPath.add(RemoteClusterMetadataManifest.MANIFEST);
        return blobPath;
    }

    private BlobContainer manifestContainerV2() {
        return blobStoreRepository.blobStore().blobContainer(getManifestPath());
    }

    public String getLatestManifestFileName() throws IOException {

        List<BlobMetadata> manifests = manifestContainerV2().listBlobsByPrefixInSortedOrder(
            RemoteIndexMetadataManifest.INDEX_METADATA_MANIFEST,
            1,
            BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
        );
        return manifests.isEmpty() ? null : manifests.getFirst().name();
    }

    public IndexMetadataManifest getLatestIndexMetadataManifest() throws IOException {
        String latestManifestFileName = getLatestManifestFileName();
        if (Objects.isNull(latestManifestFileName)) {
            return null;
        }
        return fetchRemoteIndexMetadataManifest(null, null, latestManifestFileName);
    }
}
