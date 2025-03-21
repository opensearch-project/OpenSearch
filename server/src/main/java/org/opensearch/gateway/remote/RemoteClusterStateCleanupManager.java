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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST;
import static org.opensearch.gateway.remote.model.RemoteGlobalMetadata.GLOBAL_METADATA_FORMAT;

/**
 * A Manager which provides APIs to clean up stale cluster state files and runs an async stale cleanup task
 *
 * @opensearch.internal
 */
public class RemoteClusterStateCleanupManager implements Closeable {

    public static final int RETAINED_MANIFESTS = 10;
    public static final int SKIP_CLEANUP_STATE_CHANGES = 10;
    public static final TimeValue CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT = TimeValue.timeValueMinutes(5);
    public static final TimeValue CLUSTER_STATE_CLEANUP_INTERVAL_MINIMUM = TimeValue.MINUS_ONE;

    /**
     * Setting to specify the interval to do run stale file cleanup job
     * Min value -1 indicates that the stale file cleanup job should be disabled
     */
    public static final Setting<TimeValue> REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.cleanup_interval",
        CLUSTER_STATE_CLEANUP_INTERVAL_DEFAULT,
        CLUSTER_STATE_CLEANUP_INTERVAL_MINIMUM,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final Logger logger = LogManager.getLogger(RemoteClusterStateCleanupManager.class);
    private final RemoteClusterStateService remoteClusterStateService;
    private final RemotePersistenceStats remoteStateStats;
    private BlobStoreTransferService blobStoreTransferService;
    private TimeValue staleFileCleanupInterval;
    private final AtomicBoolean deleteStaleMetadataRunning = new AtomicBoolean(false);
    private volatile AsyncStaleFileDeletion staleFileDeletionTask;
    private long lastCleanupAttemptStateVersion;
    private final ThreadPool threadpool;
    private final ClusterApplierService clusterApplierService;
    private RemoteManifestManager remoteManifestManager;
    private final RemoteRoutingTableService remoteRoutingTableService;

    public RemoteClusterStateCleanupManager(
        RemoteClusterStateService remoteClusterStateService,
        ClusterService clusterService,
        RemoteRoutingTableService remoteRoutingTableService
    ) {
        this.remoteClusterStateService = remoteClusterStateService;
        this.remoteStateStats = remoteClusterStateService.getRemoteStateStats();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.clusterApplierService = clusterService.getClusterApplierService();
        this.staleFileCleanupInterval = clusterSettings.get(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING);
        this.threadpool = remoteClusterStateService.getThreadpool();
        // initialize with 0, a cleanup will be done when this node is elected master node and version is incremented more than threshold
        this.lastCleanupAttemptStateVersion = 0;
        clusterSettings.addSettingsUpdateConsumer(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING, this::updateCleanupInterval);
        this.remoteRoutingTableService = remoteRoutingTableService;
    }

    void start() {
        staleFileDeletionTask = new AsyncStaleFileDeletion(this);
        remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
    }

    @Override
    public void close() throws IOException {
        if (staleFileDeletionTask != null) {
            staleFileDeletionTask.close();
        }
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(remoteClusterStateService.getBlobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    private void updateCleanupInterval(TimeValue updatedInterval) {
        this.staleFileCleanupInterval = updatedInterval;
        logger.info("updated remote state cleanup interval to {}", updatedInterval);
        // After updating the interval, we need to close the current task and create a new one which will run with updated interval
        if (staleFileDeletionTask != null && !staleFileDeletionTask.getInterval().equals(updatedInterval)) {
            staleFileDeletionTask.setInterval(updatedInterval);
        }
    }

    // visible for testing
    void cleanUpStaleFiles() {
        ClusterState currentAppliedState = clusterApplierService.state();
        if (currentAppliedState.nodes().isLocalNodeElectedClusterManager()) {
            long cleanUpAttemptStateVersion = currentAppliedState.version();
            assert !Strings.isNullOrEmpty(currentAppliedState.getClusterName().value()) : "cluster name is not set";
            assert !Strings.isNullOrEmpty(currentAppliedState.metadata().clusterUUID()) : "cluster uuid is not set";
            if (cleanUpAttemptStateVersion - lastCleanupAttemptStateVersion > SKIP_CLEANUP_STATE_CHANGES) {
                logger.info(
                    "Cleaning up stale remote state files for cluster [{}] with uuid [{}]. Last clean was done before {} updates",
                    currentAppliedState.getClusterName().value(),
                    currentAppliedState.metadata().clusterUUID(),
                    cleanUpAttemptStateVersion - lastCleanupAttemptStateVersion
                );
                this.deleteStaleClusterMetadata(
                    currentAppliedState.getClusterName().value(),
                    currentAppliedState.metadata().clusterUUID(),
                    RETAINED_MANIFESTS
                );
                lastCleanupAttemptStateVersion = cleanUpAttemptStateVersion;
            } else {
                logger.debug(
                    "Skipping cleanup of stale remote state files for cluster [{}] with uuid [{}]. Last clean was done before {} updates, which is less than threshold {}",
                    currentAppliedState.getClusterName().value(),
                    currentAppliedState.metadata().clusterUUID(),
                    cleanUpAttemptStateVersion - lastCleanupAttemptStateVersion,
                    SKIP_CLEANUP_STATE_CHANGES
                );
            }
        } else {
            logger.debug("Skipping cleanup task as local node is not elected Cluster Manager");
        }
    }

    private void addStaleGlobalMetadataPath(String fileName, Set<String> filesToKeep, Set<String> staleGlobalMetadataPaths) {
        if (!filesToKeep.contains(fileName)) {
            String[] splitPath = fileName.split("/");
            staleGlobalMetadataPaths.add(
                new BlobPath().add(GLOBAL_METADATA_PATH_TOKEN).buildAsString() + GLOBAL_METADATA_FORMAT.blobName(
                    splitPath[splitPath.length - 1]
                )
            );
        }
    }

    // visible for testing
    void deleteClusterMetadata(
        String clusterName,
        String clusterUUID,
        List<BlobMetadata> activeManifestBlobMetadata,
        List<BlobMetadata> staleManifestBlobMetadata
    ) {
        try {
            Set<String> filesToKeep = new HashSet<>();
            Set<String> staleManifestPaths = new HashSet<>();
            Set<String> staleIndexMetadataPaths = new HashSet<>();
            Set<String> staleGlobalMetadataPaths = new HashSet<>();
            Set<String> staleEphemeralAttributePaths = new HashSet<>();
            Set<String> staleIndexRoutingPaths = new HashSet<>();
            Set<String> staleIndexRoutingDiffPaths = new HashSet<>();
            activeManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = remoteManifestManager.fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                clusterMetadataManifest.getIndices()
                    .forEach(
                        uploadedIndexMetadata -> filesToKeep.add(
                            RemoteClusterStateUtils.getFormattedIndexFileName(uploadedIndexMetadata.getUploadedFilename())
                        )
                    );
                if (clusterMetadataManifest.getCodecVersion() == ClusterMetadataManifest.CODEC_V1) {
                    filesToKeep.add(clusterMetadataManifest.getGlobalMetadataFileName());
                } else if (clusterMetadataManifest.getCodecVersion() >= ClusterMetadataManifest.CODEC_V2) {
                    filesToKeep.add(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename());
                    filesToKeep.add(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename());
                    filesToKeep.add(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename());
                    clusterMetadataManifest.getCustomMetadataMap()
                        .values()
                        .forEach(attribute -> filesToKeep.add(attribute.getUploadedFilename()));
                }
                if (clusterMetadataManifest.getTransientSettingsMetadata() != null) {
                    filesToKeep.add(clusterMetadataManifest.getTransientSettingsMetadata().getUploadedFilename());
                }
                if (clusterMetadataManifest.getHashesOfConsistentSettings() != null) {
                    filesToKeep.add(clusterMetadataManifest.getHashesOfConsistentSettings().getUploadedFilename());
                }
                if (clusterMetadataManifest.getDiscoveryNodesMetadata() != null) {
                    filesToKeep.add(clusterMetadataManifest.getDiscoveryNodesMetadata().getUploadedFilename());
                }
                if (clusterMetadataManifest.getClusterBlocksMetadata() != null) {
                    filesToKeep.add(clusterMetadataManifest.getClusterBlocksMetadata().getUploadedFilename());
                }
                if (clusterMetadataManifest.getClusterStateCustomMap() != null) {
                    clusterMetadataManifest.getClusterStateCustomMap()
                        .values()
                        .forEach(attribute -> filesToKeep.add(attribute.getUploadedFilename()));
                }
                if (clusterMetadataManifest.getIndicesRouting() != null) {
                    clusterMetadataManifest.getIndicesRouting()
                        .forEach(uploadedIndicesRouting -> filesToKeep.add(uploadedIndicesRouting.getUploadedFilename()));
                }
                if (clusterMetadataManifest.getDiffManifest() != null
                    && clusterMetadataManifest.getDiffManifest().getIndicesRoutingDiffPath() != null) {
                    filesToKeep.add(clusterMetadataManifest.getDiffManifest().getIndicesRoutingDiffPath());
                }
            });
            staleManifestBlobMetadata.forEach(blobMetadata -> {
                ClusterMetadataManifest clusterMetadataManifest = remoteManifestManager.fetchRemoteClusterMetadataManifest(
                    clusterName,
                    clusterUUID,
                    blobMetadata.name()
                );
                staleManifestPaths.add(
                    remoteManifestManager.getManifestFolderPath(clusterName, clusterUUID).buildAsString() + blobMetadata.name()
                );
                if (clusterMetadataManifest.getCodecVersion() == ClusterMetadataManifest.CODEC_V1) {
                    addStaleGlobalMetadataPath(clusterMetadataManifest.getGlobalMetadataFileName(), filesToKeep, staleGlobalMetadataPaths);
                } else if (clusterMetadataManifest.getCodecVersion() >= ClusterMetadataManifest.CODEC_V2) {
                    if (filesToKeep.contains(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename()) == false) {
                        staleGlobalMetadataPaths.add(clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename());
                    }
                    if (filesToKeep.contains(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename()) == false) {
                        staleGlobalMetadataPaths.add(clusterMetadataManifest.getSettingsMetadata().getUploadedFilename());
                    }
                    if (filesToKeep.contains(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename()) == false) {
                        staleGlobalMetadataPaths.add(clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename());
                    }
                    clusterMetadataManifest.getCustomMetadataMap()
                        .values()
                        .stream()
                        .map(ClusterMetadataManifest.UploadedMetadataAttribute::getUploadedFilename)
                        .filter(file -> filesToKeep.contains(file) == false)
                        .forEach(staleGlobalMetadataPaths::add);
                }
                if (clusterMetadataManifest.getIndicesRouting() != null) {
                    clusterMetadataManifest.getIndicesRouting().forEach(uploadedIndicesRouting -> {
                        if (!filesToKeep.contains(uploadedIndicesRouting.getUploadedFilename())) {
                            staleIndexRoutingPaths.add(uploadedIndicesRouting.getUploadedFilename());
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "Indices routing paths in stale manifest: {}",
                                    uploadedIndicesRouting.getUploadedFilename()
                                )
                            );
                        }
                    });
                }
                if (clusterMetadataManifest.getDiffManifest() != null
                    && clusterMetadataManifest.getDiffManifest().getIndicesRoutingDiffPath() != null) {
                    if (!filesToKeep.contains(clusterMetadataManifest.getDiffManifest().getIndicesRoutingDiffPath())) {
                        staleIndexRoutingDiffPaths.add(clusterMetadataManifest.getDiffManifest().getIndicesRoutingDiffPath());
                        logger.debug(
                            () -> new ParameterizedMessage(
                                "Indices routing diff paths in stale manifest: {}",
                                clusterMetadataManifest.getDiffManifest().getIndicesRoutingDiffPath()
                            )
                        );
                    }
                }

                clusterMetadataManifest.getIndices().forEach(uploadedIndexMetadata -> {
                    String fileName = RemoteClusterStateUtils.getFormattedIndexFileName(uploadedIndexMetadata.getUploadedFilename());
                    if (filesToKeep.contains(fileName) == false) {
                        staleIndexMetadataPaths.add(fileName);
                    }
                });

                if (clusterMetadataManifest.getClusterBlocksMetadata() != null
                    && !filesToKeep.contains(clusterMetadataManifest.getClusterBlocksMetadata().getUploadedFilename())) {
                    staleEphemeralAttributePaths.add(clusterMetadataManifest.getClusterBlocksMetadata().getUploadedFilename());
                }
                if (clusterMetadataManifest.getDiscoveryNodesMetadata() != null
                    && !filesToKeep.contains(clusterMetadataManifest.getDiscoveryNodesMetadata().getUploadedFilename())) {
                    staleEphemeralAttributePaths.add(clusterMetadataManifest.getDiscoveryNodesMetadata().getUploadedFilename());
                }
                if (clusterMetadataManifest.getTransientSettingsMetadata() != null
                    && !filesToKeep.contains(clusterMetadataManifest.getTransientSettingsMetadata().getUploadedFilename())) {
                    staleEphemeralAttributePaths.add(clusterMetadataManifest.getTransientSettingsMetadata().getUploadedFilename());
                }
                if (clusterMetadataManifest.getHashesOfConsistentSettings() != null
                    && !filesToKeep.contains(clusterMetadataManifest.getHashesOfConsistentSettings().getUploadedFilename())) {
                    staleEphemeralAttributePaths.add(clusterMetadataManifest.getHashesOfConsistentSettings().getUploadedFilename());
                }
                if (clusterMetadataManifest.getClusterStateCustomMap() != null) {
                    clusterMetadataManifest.getClusterStateCustomMap()
                        .values()
                        .stream()
                        .filter(u -> !filesToKeep.contains(u.getUploadedFilename()))
                        .forEach(attribute -> staleEphemeralAttributePaths.add(attribute.getUploadedFilename()));
                }

            });

            if (staleManifestPaths.isEmpty()) {
                logger.debug("No stale Remote Cluster Metadata files found");
                return;
            }

            deleteStalePaths(new ArrayList<>(staleGlobalMetadataPaths));
            deleteStalePaths(new ArrayList<>(staleIndexMetadataPaths));
            deleteStalePaths(new ArrayList<>(staleEphemeralAttributePaths));
            deleteStalePaths(new ArrayList<>(staleManifestPaths));
            try {
                remoteRoutingTableService.deleteStaleIndexRoutingPaths(new ArrayList<>(staleIndexRoutingPaths));
            } catch (IOException e) {
                logger.error(
                    () -> new ParameterizedMessage("Error while deleting stale index routing files {}", staleIndexRoutingPaths),
                    e
                );
                remoteStateStats.indexRoutingFilesCleanupAttemptFailed();
            }
            try {
                remoteRoutingTableService.deleteStaleIndexRoutingDiffPaths(new ArrayList<>(staleIndexRoutingDiffPaths));
            } catch (IOException e) {
                logger.error(
                    () -> new ParameterizedMessage("Error while deleting stale index routing diff files {}", staleIndexRoutingDiffPaths),
                    e
                );
                remoteStateStats.indicesRoutingDiffFileCleanupAttemptFailed();
            }
        } catch (IllegalStateException e) {
            logger.error("Error while fetching Remote Cluster Metadata manifests", e);
        } catch (IOException e) {
            logger.error("Error while deleting stale Remote Cluster Metadata files", e);
            remoteStateStats.cleanUpAttemptFailed();
        } catch (Exception e) {
            logger.error("Unexpected error while deleting stale Remote Cluster Metadata files", e);
            remoteStateStats.cleanUpAttemptFailed();
        }
    }

    /**
     * Deletes older than last {@code versionsToRetain} manifests. Also cleans up unreferenced IndexMetadata associated with older manifests
     *
     * @param clusterName name of the cluster
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param manifestsToRetain no of latest manifest files to keep in remote
     */
    // package private for testing
    void deleteStaleClusterMetadata(String clusterName, String clusterUUID, int manifestsToRetain) {
        if (deleteStaleMetadataRunning.compareAndSet(false, true) == false) {
            logger.info("Delete stale cluster metadata task is already in progress.");
            return;
        }
        try {
            getBlobStoreTransferService().listAllInSortedOrderAsync(
                ThreadPool.Names.REMOTE_PURGE,
                remoteManifestManager.getManifestFolderPath(clusterName, clusterUUID),
                MANIFEST,
                Integer.MAX_VALUE,
                new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {
                        if (blobMetadata.size() > manifestsToRetain) {
                            deleteClusterMetadata(
                                clusterName,
                                clusterUUID,
                                blobMetadata.subList(0, manifestsToRetain),
                                blobMetadata.subList(manifestsToRetain, blobMetadata.size())
                            );
                        }
                        deleteStaleMetadataRunning.set(false);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting Remote Cluster Metadata for clusterUUIDs {}",
                                clusterUUID
                            )
                        );
                        deleteStaleMetadataRunning.set(false);
                    }
                }
            );
        } catch (Exception e) {
            deleteStaleMetadataRunning.set(false);
            throw e;
        }
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     *
     * @param clusterName name of the cluster
     * @param clusterUUIDs clusteUUIDs for which the remote state needs to be purged
     */
    void deleteStaleUUIDsClusterMetadata(String clusterName, List<String> clusterUUIDs) {
        clusterUUIDs.forEach(
            clusterUUID -> getBlobStoreTransferService().deleteAsync(
                ThreadPool.Names.REMOTE_PURGE,
                RemoteClusterStateUtils.getClusterMetadataBasePath(
                    remoteClusterStateService.getBlobStoreRepository(),
                    clusterName,
                    clusterUUID
                ),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        logger.info("Deleted all remote cluster metadata for cluster UUID - {}", clusterUUID);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error(
                            new ParameterizedMessage(
                                "Exception occurred while deleting all remote cluster metadata for cluster UUID {}",
                                clusterUUID
                            ),
                            e
                        );
                        remoteStateStats.cleanUpAttemptFailed();
                    }
                }
            )
        );
    }

    // package private for testing
    void deleteStalePaths(List<String> stalePaths) throws IOException {
        logger.debug(String.format(Locale.ROOT, "Deleting stale files from remote - %s", stalePaths));
        getBlobStoreTransferService().deleteBlobs(BlobPath.cleanPath(), stalePaths);
    }

    /**
     * Purges all remote cluster state against provided cluster UUIDs
     * @param clusterState current state of the cluster
     * @param committedManifest last committed ClusterMetadataManifest
     */
    public void deleteStaleClusterUUIDs(ClusterState clusterState, ClusterMetadataManifest committedManifest) {
        threadpool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
            String clusterName = clusterState.getClusterName().value();
            logger.debug("Deleting stale cluster UUIDs data from remote [{}]", clusterName);
            Set<String> allClustersUUIDsInRemote;
            try {
                allClustersUUIDsInRemote = new HashSet<>(
                    remoteClusterStateService.getAllClusterUUIDs(clusterState.getClusterName().value())
                );
            } catch (IOException e) {
                logger.info(String.format(Locale.ROOT, "Error while fetching all cluster UUIDs for [%s]", clusterName));
                return;
            }
            // Retain last 2 cluster uuids data
            allClustersUUIDsInRemote.remove(committedManifest.getClusterUUID());
            allClustersUUIDsInRemote.remove(committedManifest.getPreviousClusterUUID());
            deleteStaleUUIDsClusterMetadata(clusterName, new ArrayList<>(allClustersUUIDsInRemote));
        });
    }

    public TimeValue getStaleFileCleanupInterval() {
        return this.staleFileCleanupInterval;
    }

    AsyncStaleFileDeletion getStaleFileDeletionTask() { // for testing
        return this.staleFileDeletionTask;
    }

    RemotePersistenceStats getStats() {
        return this.remoteStateStats;
    }

    static final class AsyncStaleFileDeletion extends AbstractAsyncTask {
        private final RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;

        AsyncStaleFileDeletion(RemoteClusterStateCleanupManager remoteClusterStateCleanupManager) {
            super(
                logger,
                remoteClusterStateCleanupManager.threadpool,
                remoteClusterStateCleanupManager.getStaleFileCleanupInterval(),
                true
            );
            this.remoteClusterStateCleanupManager = remoteClusterStateCleanupManager;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            remoteClusterStateCleanupManager.cleanUpStaleFiles();
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.REMOTE_PURGE;
        }
    }
}
