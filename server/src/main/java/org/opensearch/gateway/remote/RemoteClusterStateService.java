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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {

    public static final String METADATA_NAME_FORMAT = "%s.dat";

    public static final String METADATA_MANIFEST_NAME_FORMAT = "%s";

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-manifest",
        METADATA_MANIFEST_NAME_FORMAT,
        ClusterMetadataManifest::fromXContent
    );
    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    // TODO The remote state enabled and repository settings should be read from node attributes.
    // Dependent on https://github.com/opensearch-project/OpenSearch/pull/9105/
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );
    /**
     * Used to specify default repo to use for cluster state metadata upload
     */
    public static final Setting<String> REMOTE_CLUSTER_STATE_REPOSITORY_SETTING = Setting.simpleString(
        "cluster.remote_store.state.repository",
        "",
        Property.NodeScope,
        Property.Final
    );
    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    public static final String DELIMITER = "__";

    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private BlobStoreRepository blobStoreRepository;
    private volatile TimeValue slowWriteLoggingThreshold;

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeNanosSupplier
    ) {
        assert REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) == true : "Remote cluster state is not enabled";
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public ClusterMetadataManifest writeFullMetadata(ClusterState clusterState) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        ensureRepositorySet();

        final List<ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = new ArrayList<>();
        // todo parallel upload
        // any validations before/after upload ?
        for (IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX/metadata_4_1690947200
            final String indexMetadataKey = writeIndexMetadata(
                clusterState.getClusterName().value(),
                clusterState.getMetadata().clusterUUID(),
                indexMetadata,
                indexMetadataFileName(indexMetadata)
            );
            final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(
                indexMetadata.getIndex().getName(),
                indexMetadata.getIndexUUID(),
                indexMetadataKey
            );
            allUploadedIndexMetadata.add(uploadedIndexMetadata);
        }
        final ClusterMetadataManifest manifest = uploadManifest(clusterState, allUploadedIndexMetadata, false);
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; " + "wrote full state with [{}] indices",
                durationMillis,
                slowWriteLoggingThreshold,
                allUploadedIndexMetadata.size()
            );
        } else {
            // todo change to debug
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices",
                durationMillis,
                allUploadedIndexMetadata.size()
            );
        }
        return manifest;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current cluster
     * state.
     *
     * @return The uploaded ClusterMetadataManifest file
     */
    @Nullable
    public ClusterMetadataManifest writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();
        final Map<String, Long> previousStateIndexMetadataVersionByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : previousClusterState.metadata().indices().values()) {
            previousStateIndexMetadataVersionByName.put(indexMetadata.getIndex().getName(), indexMetadata.getVersion());
        }

        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));
        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            final Long previousVersion = previousStateIndexMetadataVersionByName.get(indexMetadata.getIndex().getName());
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.trace(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                final String indexMetadataKey = writeIndexMetadata(
                    clusterState.getClusterName().value(),
                    clusterState.getMetadata().clusterUUID(),
                    indexMetadata,
                    indexMetadataFileName(indexMetadata)
                );
                final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getIndexUUID(),
                    indexMetadataKey
                );
                allUploadedIndexMetadata.put(indexMetadata.getIndex().getName(), uploadedIndexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            previousStateIndexMetadataVersionByName.remove(indexMetadata.getIndex().getName());
        }

        for (String removedIndexName : previousStateIndexMetadataVersionByName.keySet()) {
            allUploadedIndexMetadata.remove(removedIndexName);
        }
        final ClusterMetadataManifest manifest = uploadManifest(
            clusterState,
            allUploadedIndexMetadata.values().stream().collect(Collectors.toList()),
            false
        );
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged
            );
        } else {
            logger.trace(
                "writing cluster state took [{}ms]; " + "wrote metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged
            );
        }
        return manifest;
    }

    @Nullable
    public ClusterMetadataManifest markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataManifest previousManifest)
        throws IOException {
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert clusterState != null : "Last accepted cluster state is not set";
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        return uploadManifest(clusterState, previousManifest.getIndices(), true);
    }

    public ClusterState getLatestClusterState(String clusterUUID) {
        // todo
        return null;
    }

    @Override
    public void close() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    // Visible for testing
    void ensureRepositorySet() {
        if (blobStoreRepository != null) {
            return;
        }
        assert REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = REMOTE_CLUSTER_STATE_REPOSITORY_SETTING.get(settings);
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    private ClusterMetadataManifest uploadManifest(
        ClusterState clusterState,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String manifestFileName = getManifestFileName(clusterState.term(), clusterState.version());
            final ClusterMetadataManifest manifest = new ClusterMetadataManifest(
                clusterState.term(),
                clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID(),
                Version.CURRENT,
                nodeId,
                committed,
                uploadedIndexMetadata
            );
            writeMetadataManifest(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), manifest, manifestFileName);
            return manifest;
        }
    }

    private String writeIndexMetadata(String clusterName, String clusterUUID, IndexMetadata uploadIndexMetadata, String fileName)
        throws IOException {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(clusterName, clusterUUID, uploadIndexMetadata.getIndexUUID());
        INDEX_METADATA_FORMAT.write(uploadIndexMetadata, indexMetadataContainer, fileName, blobStoreRepository.getCompressor());
        // returning full path
        return indexMetadataContainer.path().buildAsString() + fileName;
    }

    private void writeMetadataManifest(String clusterName, String clusterUUID, ClusterMetadataManifest uploadManifest, String fileName)
        throws IOException {
        final BlobContainer metadataManifestContainer = manifestContainer(clusterName, clusterUUID);
        CLUSTER_METADATA_MANIFEST_FORMAT.write(uploadManifest, metadataManifestContainer, fileName, blobStoreRepository.getCompressor());
    }

    private BlobContainer indexMetadataContainer(String clusterName, String clusterUUID, String indexUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add("cluster-state")
                    .add(clusterUUID)
                    .add("index")
                    .add(indexUUID)
            );
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add("cluster-state")
                    .add(clusterUUID)
                    .add("manifest")
            );
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    private static String getManifestFileName(long term, long version) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest_2147483642_2147483637_456536447
        return String.join(
            DELIMITER,
            "manifest",
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version),
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );
    }

    private static String indexMetadataFileName(IndexMetadata indexMetadata) {
        return String.join(DELIMITER, "metadata", String.valueOf(indexMetadata.getVersion()), String.valueOf(System.currentTimeMillis()));
    }

    /**
     * Fetch latest index metadata from remote cluster state
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@code Map<String, IndexMetadata>} latest IndexUUID to IndexMetadata map
     */
    public Map<String, IndexMetadata> getLatestIndexMetadata(String clusterName, String clusterUUID) throws IOException {
        Map<String, IndexMetadata> remoteIndexMetadata = new HashMap<>();
        ClusterMetadataManifest clusterMetadataManifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
        assert Objects.equals(clusterUUID, clusterMetadataManifest.getClusterUUID())
            : "Corrupt ClusterMetadataManifest found. Cluster UUID mismatch.";
        for (UploadedIndexMetadata uploadedIndexMetadata : clusterMetadataManifest.getIndices()) {
            IndexMetadata indexMetadata = getIndexMetadata(clusterName, clusterUUID, uploadedIndexMetadata);
            remoteIndexMetadata.put(uploadedIndexMetadata.getIndexUUID(), indexMetadata);
        }
        return remoteIndexMetadata;
    }

    /**
     * Fetch index metadata from remote cluster state
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param uploadedIndexMetadata {@link UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    private IndexMetadata getIndexMetadata(String clusterName, String clusterUUID, UploadedIndexMetadata uploadedIndexMetadata) {
        try {
            return INDEX_METADATA_FORMAT.read(
                indexMetadataContainer(clusterName, clusterUUID, uploadedIndexMetadata.getIndexUUID()),
                uploadedIndexMetadata.getUploadedFilename(),
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public ClusterMetadataManifest getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        String latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        return fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, latestManifestFileName);
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private String getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        try {
            /**
             * {@link BlobContainer#listBlobsByPrefixInSortedOrder} will get the latest manifest file
             * as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
             * when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            List<BlobMetadata> manifestFilesMetadata = manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                "manifest" + DELIMITER,
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
            if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
                return manifestFilesMetadata.get(0).name();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }

        throw new IllegalStateException(String.format(Locale.ROOT, "Remote Cluster State not found - %s", clusterUUID));
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    private ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            return RemoteClusterStateService.CLUSTER_METADATA_MANIFEST_FORMAT.read(
                manifestContainer(clusterName, clusterUUID),
                filename,
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }
}
