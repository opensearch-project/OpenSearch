/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.translog.transfer.TranslogTransferManager;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;

/**
 * A Translog implementation which syncs local FS with a remote store
 * The current impl uploads translog , ckp and metadata to remote store
 * for every sync, post syncing to disk. Post that, a new generation is
 * created. This implementation is also aware of pinned timestamp and makes
 * sure data against pinned timestamp is retained.
 *
 * @opensearch.internal
 */
public class RemoteFsTimestampAwareTranslog extends RemoteFsTranslog {

    private static Logger staticLogger = LogManager.getLogger(RemoteFsTimestampAwareTranslog.class);
    private final Logger logger;
    private final Map<Long, String> metadataFilePinnedTimestampMap;
    // For metadata files, with no min generation in the name, we cache generation data to avoid multiple reads.
    private final Map<String, Tuple<Long, Long>> oldFormatMetadataFileGenerationMap;
    private final Map<String, Tuple<Long, Long>> oldFormatMetadataFilePrimaryTermMap;
    private final AtomicLong minPrimaryTermInRemote = new AtomicLong(Long.MAX_VALUE);
    private final AtomicBoolean triggerTrimOnMinRemoteGenReferencedChange = new AtomicBoolean(false);

    public RemoteFsTimestampAwareTranslog(
        TranslogConfig config,
        String translogUUID,
        TranslogDeletionPolicy deletionPolicy,
        LongSupplier globalCheckpointSupplier,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSequenceNumberConsumer,
        BlobStoreRepository blobStoreRepository,
        ThreadPool threadPool,
        BooleanSupplier startedPrimarySupplier,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings
    ) throws IOException {
        super(
            config,
            translogUUID,
            deletionPolicy,
            globalCheckpointSupplier,
            primaryTermSupplier,
            persistedSequenceNumberConsumer,
            blobStoreRepository,
            threadPool,
            startedPrimarySupplier,
            remoteTranslogTransferTracker,
            remoteStoreSettings
        );
        logger = Loggers.getLogger(getClass(), shardId);
        this.metadataFilePinnedTimestampMap = new HashMap<>();
        this.oldFormatMetadataFileGenerationMap = new HashMap<>();
        this.oldFormatMetadataFilePrimaryTermMap = new HashMap<>();
    }

    @Override
    protected void onDelete() {
        ClusterService.assertClusterOrClusterManagerStateThread();
        // clean up all remote translog files
        try {
            trimUnreferencedReaders(true, false);
        } catch (IOException e) {
            logger.error("Exception while deleting translog files from remote store", e);
        }
    }

    @Override
    protected void onMinRemoteGenReferencedChange() {
        triggerTrimOnMinRemoteGenReferencedChange.set(true);
    }

    @Override
    public void trimUnreferencedReaders() throws IOException {
        trimUnreferencedReaders(false, true);
    }

    // Visible for testing
    protected void trimUnreferencedReaders(boolean indexDeleted, boolean trimLocal) throws IOException {
        if (trimLocal) {
            // clean up local translog files and updates readers
            super.trimUnreferencedReaders(true);
        }

        // Update file tracker to reflect local translog state
        Optional<Long> minLiveGeneration = readers.stream().map(BaseTranslogReader::getGeneration).min(Long::compareTo);
        if (minLiveGeneration.isPresent()) {
            List<String> staleFilesInTracker = new ArrayList<>();
            for (String file : fileTransferTracker.allUploaded()) {
                if (file.endsWith(TRANSLOG_FILE_SUFFIX)) {
                    long generation = Translog.parseIdFromFileName(file);
                    if (generation < minLiveGeneration.get()) {
                        staleFilesInTracker.add(file);
                        staleFilesInTracker.add(Translog.getCommitCheckpointFileName(generation));
                    }
                }
                fileTransferTracker.delete(staleFilesInTracker);
            }
        }

        // This is to ensure that after the permits are acquired during primary relocation, there are no further modification on remote
        // store.
        if (indexDeleted == false && (startedPrimarySupplier.getAsBoolean() == false || pauseSync.get())) {
            return;
        }

        // This is to fail fast and avoid listing md files un-necessarily.
        if (indexDeleted == false && RemoteStoreUtils.isPinnedTimestampStateStale()) {
            logger.warn("Skipping remote translog garbage collection as last fetch of pinned timestamp is stale");
            return;
        }

        // This code block ensures parity with RemoteFsTranslog. Without this, we will end up making list translog metadata
        // call in each invocation of trimUnreferencedReaders
        if (indexDeleted == false && triggerTrimOnMinRemoteGenReferencedChange.get() == false) {
            return;
        } else if (triggerTrimOnMinRemoteGenReferencedChange.get()) {
            triggerTrimOnMinRemoteGenReferencedChange.set(false);
        }

        // Since remote generation deletion is async, this ensures that only one generation deletion happens at a time.
        // Remote generations involves 2 async operations - 1) Delete translog generation files 2) Delete metadata files
        // We try to acquire 2 permits and if we can not, we return from here itself.
        if (remoteGenerationDeletionPermits.tryAcquire(REMOTE_DELETION_PERMITS) == false) {
            return;
        }

        ActionListener<List<BlobMetadata>> listMetadataFilesListener = new ActionListener<>() {
            @Override
            public void onResponse(List<BlobMetadata> blobMetadata) {
                List<String> metadataFiles = blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList());

                try {
                    if (indexDeleted == false && metadataFiles.size() <= 1) {
                        logger.debug("No stale translog metadata files found");
                        remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
                        return;
                    }

                    // Check last fetch status of pinned timestamps. If stale, return.
                    if (indexDeleted == false && RemoteStoreUtils.isPinnedTimestampStateStale()) {
                        logger.warn("Skipping remote translog garbage collection as last fetch of pinned timestamp is stale");
                        remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
                        return;
                    }

                    List<String> metadataFilesToBeDeleted = getMetadataFilesToBeDeleted(metadataFiles, indexDeleted);

                    // If index is not deleted, make sure to keep latest metadata file
                    if (indexDeleted == false) {
                        metadataFilesToBeDeleted.remove(metadataFiles.get(0));
                    }

                    if (metadataFilesToBeDeleted.isEmpty()) {
                        logger.debug("No metadata files to delete");
                        remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
                        return;
                    }

                    logger.debug(() -> "metadataFilesToBeDeleted = " + metadataFilesToBeDeleted);
                    // For all the files that we are keeping, fetch min and max generations
                    List<String> metadataFilesNotToBeDeleted = new ArrayList<>(metadataFiles);
                    metadataFilesNotToBeDeleted.removeAll(metadataFilesToBeDeleted);

                    logger.debug(() -> "metadataFilesNotToBeDeleted = " + metadataFilesNotToBeDeleted);

                    Set<Long> generationsToBeDeleted = getGenerationsToBeDeleted(
                        metadataFilesNotToBeDeleted,
                        metadataFilesToBeDeleted,
                        indexDeleted ? Long.MAX_VALUE : getMinGenerationToKeepInRemote()
                    );

                    logger.debug(() -> "generationsToBeDeleted = " + generationsToBeDeleted);
                    if (generationsToBeDeleted.isEmpty() == false) {
                        // Delete stale generations
                        translogTransferManager.deleteGenerationAsync(
                            primaryTermSupplier.getAsLong(),
                            generationsToBeDeleted,
                            remoteGenerationDeletionPermits::release
                        );
                    } else {
                        remoteGenerationDeletionPermits.release();
                    }

                    if (metadataFilesToBeDeleted.isEmpty() == false) {
                        // Delete stale metadata files
                        translogTransferManager.deleteMetadataFilesAsync(
                            metadataFilesToBeDeleted,
                            remoteGenerationDeletionPermits::release
                        );

                        // Update cache to keep only those metadata files that are not getting deleted
                        oldFormatMetadataFileGenerationMap.keySet().retainAll(metadataFilesNotToBeDeleted);
                        oldFormatMetadataFilePrimaryTermMap.keySet().retainAll(metadataFilesNotToBeDeleted);
                        // Delete stale primary terms
                        deleteStaleRemotePrimaryTerms(metadataFilesNotToBeDeleted);
                    } else {
                        remoteGenerationDeletionPermits.release();
                    }
                } catch (Exception e) {
                    remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
                }
            }

            @Override
            public void onFailure(Exception e) {
                remoteGenerationDeletionPermits.release(REMOTE_DELETION_PERMITS);
                logger.error("Exception while listing translog metadata files", e);
            }
        };
        translogTransferManager.listTranslogMetadataFilesAsync(listMetadataFilesListener);
    }

    private long getMinGenerationToKeepInRemote() {
        return minRemoteGenReferenced - indexSettings().getRemoteTranslogExtraKeep();
    }

    // Visible for testing
    protected Set<Long> getGenerationsToBeDeleted(
        List<String> metadataFilesNotToBeDeleted,
        List<String> metadataFilesToBeDeleted,
        long minGenerationToKeepInRemote
    ) throws IOException {
        Set<Long> generationsFromMetadataFilesToBeDeleted = new HashSet<>();
        for (String mdFile : metadataFilesToBeDeleted) {
            Tuple<Long, Long> minMaxGen = getMinMaxTranslogGenerationFromMetadataFile(mdFile, translogTransferManager);
            generationsFromMetadataFilesToBeDeleted.addAll(
                LongStream.rangeClosed(minMaxGen.v1(), minMaxGen.v2()).boxed().collect(Collectors.toList())
            );
        }

        Map<String, Tuple<Long, Long>> metadataFileNotToBeDeletedGenerationMap = getGenerationForMetadataFiles(metadataFilesNotToBeDeleted);
        TreeSet<Tuple<Long, Long>> pinnedGenerations = getOrderedPinnedMetadataGenerations(metadataFileNotToBeDeletedGenerationMap);
        Set<Long> generationsToBeDeleted = new HashSet<>();
        for (long generation : generationsFromMetadataFilesToBeDeleted) {
            // Check if the generation is not referred by metadata file matching pinned timestamps
            // The check with minGenerationToKeep is redundant but kept as to make sure we don't delete generations
            // that are not persisted in remote segment store yet.
            if (generation < minGenerationToKeepInRemote && isGenerationPinned(generation, pinnedGenerations) == false) {
                generationsToBeDeleted.add(generation);
            }
        }
        return generationsToBeDeleted;
    }

    protected List<String> getMetadataFilesToBeDeleted(List<String> metadataFiles, boolean indexDeleted) {
        return getMetadataFilesToBeDeleted(
            metadataFiles,
            metadataFilePinnedTimestampMap,
            getMinGenerationToKeepInRemote(),
            indexDeleted,
            logger
        );
    }

    // Visible for testing
    protected static List<String> getMetadataFilesToBeDeleted(
        List<String> metadataFiles,
        Map<Long, String> metadataFilePinnedTimestampMap,
        long minGenerationToKeepInRemote,
        boolean indexDeleted,
        Logger logger
    ) {
        Tuple<Long, Set<Long>> pinnedTimestampsState = RemoteStorePinnedTimestampService.getPinnedTimestamps();

        // Keep files since last successful run of scheduler
        List<String> metadataFilesToBeDeleted = RemoteStoreUtils.filterOutMetadataFilesBasedOnAge(
            metadataFiles,
            file -> RemoteStoreUtils.invertLong(file.split(METADATA_SEPARATOR)[3]),
            pinnedTimestampsState.v1()
        );

        logger.trace(
            "metadataFiles.size = {}, metadataFilesToBeDeleted based on age based filtering = {}",
            metadataFiles.size(),
            metadataFilesToBeDeleted.size()
        );

        // Get md files matching pinned timestamps
        Set<String> implicitLockedFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFilesToBeDeleted,
            pinnedTimestampsState.v2(),
            metadataFilePinnedTimestampMap,
            file -> RemoteStoreUtils.invertLong(file.split(METADATA_SEPARATOR)[3]),
            TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
        );

        // Filter out metadata files matching pinned timestamps
        metadataFilesToBeDeleted.removeAll(implicitLockedFiles);

        logger.trace(
            "implicitLockedFiles.size = {}, metadataFilesToBeDeleted based on pinned timestamp filtering = {}",
            implicitLockedFiles.size(),
            metadataFilesToBeDeleted.size()
        );

        if (indexDeleted == false) {
            // Filter out metadata files based on minGenerationToKeep
            List<String> metadataFilesContainingMinGenerationToKeep = metadataFilesToBeDeleted.stream().filter(md -> {
                long maxGeneration = TranslogTransferMetadata.getMaxGenerationFromFileName(md);
                return maxGeneration == -1 || maxGeneration >= minGenerationToKeepInRemote;
            }).collect(Collectors.toList());
            metadataFilesToBeDeleted.removeAll(metadataFilesContainingMinGenerationToKeep);

            logger.trace(
                "metadataFilesContainingMinGenerationToKeep.size = {}, metadataFilesToBeDeleted based on minGenerationToKeep filtering = {}, minGenerationToKeep = {}",
                metadataFilesContainingMinGenerationToKeep.size(),
                metadataFilesToBeDeleted.size(),
                minGenerationToKeepInRemote
            );
        }

        return metadataFilesToBeDeleted;
    }

    // Visible for testing
    protected boolean isGenerationPinned(long generation, TreeSet<Tuple<Long, Long>> pinnedGenerations) {
        Tuple<Long, Long> ceilingGenerationRange = pinnedGenerations.ceiling(new Tuple<>(generation, generation));
        if (ceilingGenerationRange != null && generation >= ceilingGenerationRange.v1() && generation <= ceilingGenerationRange.v2()) {
            return true;
        }
        Tuple<Long, Long> floorGenerationRange = pinnedGenerations.floor(new Tuple<>(generation, generation));
        if (floorGenerationRange != null && generation >= floorGenerationRange.v1() && generation <= floorGenerationRange.v2()) {
            return true;
        }
        return false;
    }

    private TreeSet<Tuple<Long, Long>> getOrderedPinnedMetadataGenerations(Map<String, Tuple<Long, Long>> metadataFileGenerationMap) {
        TreeSet<Tuple<Long, Long>> pinnedGenerations = new TreeSet<>((o1, o2) -> {
            if (Objects.equals(o1.v1(), o2.v1()) == false) {
                return o1.v1().compareTo(o2.v1());
            } else {
                return o1.v2().compareTo(o2.v2());
            }
        });
        pinnedGenerations.addAll(metadataFileGenerationMap.values());
        return pinnedGenerations;
    }

    // Visible for testing
    protected Map<String, Tuple<Long, Long>> getGenerationForMetadataFiles(List<String> metadataFiles) throws IOException {
        Map<String, Tuple<Long, Long>> metadataFileGenerationMap = new HashMap<>();
        for (String metadataFile : metadataFiles) {
            metadataFileGenerationMap.put(metadataFile, getMinMaxTranslogGenerationFromMetadataFile(metadataFile, translogTransferManager));
        }
        return metadataFileGenerationMap;
    }

    // Visible for testing
    protected Tuple<Long, Long> getMinMaxTranslogGenerationFromMetadataFile(
        String metadataFile,
        TranslogTransferManager translogTransferManager
    ) throws IOException {
        Tuple<Long, Long> minMaxGenerationFromFileName = TranslogTransferMetadata.getMinMaxTranslogGenerationFromFilename(metadataFile);
        if (minMaxGenerationFromFileName != null) {
            return minMaxGenerationFromFileName;
        } else {
            if (oldFormatMetadataFileGenerationMap.containsKey(metadataFile)) {
                return oldFormatMetadataFileGenerationMap.get(metadataFile);
            } else {
                TranslogTransferMetadata metadata = translogTransferManager.readMetadata(metadataFile);
                Tuple<Long, Long> minMaxGenTuple = new Tuple<>(metadata.getMinTranslogGeneration(), metadata.getGeneration());
                oldFormatMetadataFileGenerationMap.put(metadataFile, minMaxGenTuple);
                return minMaxGenTuple;
            }
        }
    }

    private void deleteStaleRemotePrimaryTerms(List<String> metadataFilesNotToBeDeleted) {
        deleteStaleRemotePrimaryTerms(
            metadataFilesNotToBeDeleted,
            translogTransferManager,
            oldFormatMetadataFilePrimaryTermMap,
            minPrimaryTermInRemote,
            logger
        );
    }

    /**
     * This method must be called only after there are valid generations to delete in trimUnreferencedReaders as it ensures
     * implicitly that minimum primary term in latest translog metadata in remote store is the current primary term.
     * <br>
     * This will also delete all stale translog metadata files from remote except the latest basis the metadata file comparator.
     */
    protected static void deleteStaleRemotePrimaryTerms(
        List<String> metadataFilesNotToBeDeleted,
        TranslogTransferManager translogTransferManager,
        Map<String, Tuple<Long, Long>> oldFormatMetadataFilePrimaryTermMap,
        AtomicLong minPrimaryTermInRemoteAtomicLong,
        Logger logger
    ) {
        // The deletion of older translog files in remote store is on best-effort basis, there is a possibility that there
        // are older files that are no longer needed and should be cleaned up. In here, we delete all files that are part
        // of older primary term.
        if (metadataFilesNotToBeDeleted.isEmpty()) {
            logger.trace("No metadata is uploaded yet, returning from deleteStaleRemotePrimaryTerms");
            return;
        }
        Optional<Long> minPrimaryTermFromMetadataFiles = metadataFilesNotToBeDeleted.stream().map(file -> {
            try {
                return getMinMaxPrimaryTermFromMetadataFile(file, translogTransferManager, oldFormatMetadataFilePrimaryTermMap).v1();
            } catch (IOException e) {
                return Long.MIN_VALUE;
            }
        }).min(Long::compareTo);
        // First we delete all stale primary terms folders from remote store
        Long minPrimaryTermInRemote = getMinPrimaryTermInRemote(minPrimaryTermInRemoteAtomicLong, translogTransferManager, logger);
        if (minPrimaryTermFromMetadataFiles.get() > minPrimaryTermInRemote) {
            translogTransferManager.deletePrimaryTermsAsync(minPrimaryTermFromMetadataFiles.get());
            minPrimaryTermInRemoteAtomicLong.set(minPrimaryTermFromMetadataFiles.get());
        } else {
            logger.debug(
                "Skipping primary term cleanup. minimumReferencedPrimaryTerm = {}, minPrimaryTermInRemote = {}",
                minPrimaryTermFromMetadataFiles.get(),
                minPrimaryTermInRemote
            );
        }
    }

    protected static Long getMinPrimaryTermInRemote(
        AtomicLong minPrimaryTermInRemote,
        TranslogTransferManager translogTransferManager,
        Logger logger
    ) {
        if (minPrimaryTermInRemote.get() == Long.MAX_VALUE) {
            try {
                Set<Long> primaryTermsInRemote = translogTransferManager.listPrimaryTermsInRemote();
                if (primaryTermsInRemote.isEmpty() == false) {
                    Optional<Long> minPrimaryTerm = primaryTermsInRemote.stream().min(Long::compareTo);
                    minPrimaryTerm.ifPresent(minPrimaryTermInRemote::set);
                }
            } catch (IOException e) {
                logger.error("Exception while listing primary terms in remote translog", e);
            }
        }
        return minPrimaryTermInRemote.get();
    }

    protected static Tuple<Long, Long> getMinMaxPrimaryTermFromMetadataFile(
        String metadataFile,
        TranslogTransferManager translogTransferManager,
        Map<String, Tuple<Long, Long>> oldFormatMetadataFilePrimaryTermMap
    ) throws IOException {
        Tuple<Long, Long> minMaxPrimaryTermFromFileName = TranslogTransferMetadata.getMinMaxPrimaryTermFromFilename(metadataFile);
        if (minMaxPrimaryTermFromFileName != null) {
            return minMaxPrimaryTermFromFileName;
        } else {
            if (oldFormatMetadataFilePrimaryTermMap.containsKey(metadataFile)) {
                return oldFormatMetadataFilePrimaryTermMap.get(metadataFile);
            } else {
                TranslogTransferMetadata metadata = translogTransferManager.readMetadata(metadataFile);
                long maxPrimaryTem = TranslogTransferMetadata.getPrimaryTermFromFileName(metadataFile);
                long minPrimaryTem = -1;
                if (metadata.getGenerationToPrimaryTermMapper() != null
                    && metadata.getGenerationToPrimaryTermMapper().values().isEmpty() == false) {
                    Optional<Long> primaryTerm = metadata.getGenerationToPrimaryTermMapper()
                        .values()
                        .stream()
                        .map(s -> Long.parseLong(s))
                        .min(Long::compareTo);
                    if (primaryTerm.isPresent()) {
                        minPrimaryTem = primaryTerm.get();
                    }
                }
                Tuple<Long, Long> minMaxPrimaryTermTuple = new Tuple<>(minPrimaryTem, maxPrimaryTem);
                oldFormatMetadataFilePrimaryTermMap.put(metadataFile, minMaxPrimaryTermTuple);
                return minMaxPrimaryTermTuple;
            }
        }
    }

    public static void cleanupOfDeletedIndex(TranslogTransferManager translogTransferManager, boolean forceClean) throws IOException {
        if (forceClean) {
            translogTransferManager.delete();
        } else {
            ActionListener<List<BlobMetadata>> listMetadataFilesListener = new ActionListener<>() {
                @Override
                public void onResponse(List<BlobMetadata> blobMetadata) {
                    List<String> metadataFiles = blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList());

                    try {
                        if (metadataFiles.isEmpty()) {
                            staticLogger.debug("No stale translog metadata files found");
                            return;
                        }
                        List<String> metadataFilesToBeDeleted = getMetadataFilesToBeDeleted(
                            metadataFiles,
                            new HashMap<>(),
                            Long.MAX_VALUE,
                            true,  // This method gets called when the index is no longer present
                            staticLogger
                        );
                        if (metadataFilesToBeDeleted.isEmpty()) {
                            staticLogger.debug("No metadata files to delete");
                            return;
                        }
                        staticLogger.debug(() -> "metadataFilesToBeDeleted = " + metadataFilesToBeDeleted);

                        // For all the files that we are keeping, fetch min and max generations
                        List<String> metadataFilesNotToBeDeleted = new ArrayList<>(metadataFiles);
                        metadataFilesNotToBeDeleted.removeAll(metadataFilesToBeDeleted);
                        staticLogger.debug(() -> "metadataFilesNotToBeDeleted = " + metadataFilesNotToBeDeleted);

                        // Delete stale metadata files
                        translogTransferManager.deleteMetadataFilesAsync(metadataFilesToBeDeleted, () -> {});

                        // Delete stale primary terms
                        deleteStaleRemotePrimaryTerms(
                            metadataFilesNotToBeDeleted,
                            translogTransferManager,
                            new HashMap<>(),
                            new AtomicLong(Long.MAX_VALUE),
                            staticLogger
                        );
                    } catch (Exception e) {
                        staticLogger.error("Exception while cleaning up metadata and primary terms", e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    staticLogger.error("Exception while cleaning up metadata and primary terms", e);
                }
            };
            translogTransferManager.listTranslogMetadataFilesAsync(listMetadataFilesListener);
        }
    }
}
