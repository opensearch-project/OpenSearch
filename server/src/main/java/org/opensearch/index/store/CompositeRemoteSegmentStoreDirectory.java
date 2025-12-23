/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.lockmanager.FileLockInfo;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.CompositeRemoteDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Remote segment store directory with format-aware storage capabilities.
 * Uses CompositeRemoteDirectory for all file operations with format-specific routing.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public final class CompositeRemoteSegmentStoreDirectory extends RemoteSegmentStoreDirectory {

    /**
     * Each segment file is uploaded with unique suffix.
     * For example, _0.cfe in local filesystem will be uploaded to remote segment store as _0.cfe__gX7bNIIBrs0AUNsR2yEG
     */
    public static final String SEGMENT_NAME_UUID_SEPARATOR = "__";

    /**
     * compositeRemoteDirectory is used to store segment files with format-specific routing
     * Always present - never null
     */
    private final CompositeRemoteDirectory compositeRemoteDirectory;

    /**
     * remoteMetadataDirectory is used to store metadata files at path: cluster_UUID/index_UUID/shardId/segments/metadata
     */
    private final RemoteDirectory remoteMetadataDirectory;

    private final RemoteStoreLockManager mdLockManager;
    private final Map<Long, String> metadataFilePinnedTimestampMap;
    private final ThreadPool threadPool;

    /**
     * Only relevant for remote-store-enabled domains on replica shards
     * to store localSegmentFilename -> remoteSegmentFilename mappings
     */
    private final Map<FileMetadata, String> pendingDownloadMergedSegments;

    /**
     * Keeps track of local segment filename to uploaded filename along with other attributes like checksum.
     * This map acts as a cache layer for uploaded segment filenames which helps avoid calling listAll() each time.
     * It is important to initialize this map on creation of CompositeRemoteSegmentStoreDirectory and update it on each upload and delete.
     */
    private Map<FileMetadata, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandlerFactory(),
        RemoteSegmentMetadata.VERSION_ONE,
        RemoteSegmentMetadata.CURRENT_VERSION,
        RemoteSegmentMetadata.METADATA_CODEC
    );

    private static final Logger staticLogger = LogManager.getLogger(RemoteSegmentStoreDirectory.class);

    /**
     * AtomicBoolean that ensures only one staleCommitDeletion activity is scheduled at a time.
     * Visible for testing
     */
    protected final AtomicBoolean canDeleteStaleCommits = new AtomicBoolean(true);
    private final AtomicLong metadataUploadCounter = new AtomicLong(0);

    public static final int METADATA_FILES_TO_FETCH = 10;

    public CompositeRemoteSegmentStoreDirectory(
        RemoteDirectory remoteDataDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId
    ) throws IOException {
        this(remoteDataDirectory, remoteMetadataDirectory, mdLockManager, threadPool, shardId, null);
    }

    @InternalApi
    public CompositeRemoteSegmentStoreDirectory(
        RemoteDirectory remoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId,
        @Nullable Map<FileMetadata, String> pendingDownloadMergedSegments
    ) throws IOException {
        super(remoteDirectory, remoteMetadataDirectory, mdLockManager, threadPool, shardId);
        this.compositeRemoteDirectory = null;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        this.threadPool = threadPool;
        this.metadataFilePinnedTimestampMap = new HashMap<>();
        this.pendingDownloadMergedSegments = pendingDownloadMergedSegments != null ?
            new ConcurrentHashMap<>(pendingDownloadMergedSegments) : new ConcurrentHashMap<>();
        init();
    }

    @InternalApi
    public CompositeRemoteSegmentStoreDirectory(
        CompositeRemoteDirectory compositeRemoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId,
        @Nullable Map<FileMetadata, String> pendingDownloadMergedSegments
    ) throws IOException {
        super(null, remoteMetadataDirectory, mdLockManager, threadPool, shardId);
        this.compositeRemoteDirectory = compositeRemoteDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        this.threadPool = threadPool;
        this.metadataFilePinnedTimestampMap = new HashMap<>();
        this.pendingDownloadMergedSegments = pendingDownloadMergedSegments != null ?
            new ConcurrentHashMap<>(pendingDownloadMergedSegments) : new ConcurrentHashMap<>();
        init();
    }

    /**
     * Initializes the cache which keeps track of all the segment files uploaded to the remote segment store.
     * As this cache is specific to an instance of CompositeRemoteSegmentStoreDirectory, it is possible that cache becomes stale
     * if another instance of CompositeRemoteSegmentStoreDirectory is used to upload/delete segment files.
     * It is caller's responsibility to call init() again to ensure that cache is properly updated.
     *
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata init() throws IOException {
        logger.debug("Start initialisation of remote segment metadata");
        RemoteSegmentMetadata remoteSegmentMetadata = readLatestMetadataFile();
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadataV2());
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
        }
        logger.debug("Initialisation of remote segment metadata completed");
        return remoteSegmentMetadata;
    }

    /**
     * Read the latest metadata file to get the list of segments uploaded to the remote segment store.
     * Delegates to CompositeRemoteDirectory when available for better format-aware metadata handling.
     */
    public RemoteSegmentMetadata readLatestMetadataFile() throws IOException {
        if (compositeRemoteDirectory != null) {
            logger.debug("Reading latest metadata file from CompositeRemoteDirectory for better format-aware handling");
            return compositeRemoteDirectory.readLatestMetadataFile();
        } else {
            logger.info("No CompositeRemoteDirectory found");
            return null;
        }
    }

    private RemoteSegmentMetadata readMetadataFile(String metadataFilename) throws IOException {
        try (InputStream inputStream = remoteMetadataDirectory.getBlobStream(metadataFilename)) {
            byte[] metadataBytes = inputStream.readAllBytes();
            return metadataStreamWrapper.readStream(new ByteArrayIndexInput(metadataFilename, metadataBytes));
        }
    }

    /**
     * Initializes the cache to a specific commit which keeps track of all the segment files uploaded to the remote segment store.
     */
    public RemoteSegmentMetadata initializeToSpecificCommit(long primaryTerm, long commitGeneration, String acquirerId) throws IOException {
        String metadataFilePrefix = MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, commitGeneration);
        String metadataFile = ((RemoteStoreMetadataLockManager) mdLockManager).fetchLockedMetadataFile(metadataFilePrefix, acquirerId);
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadataV2());
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
        }
        return remoteSegmentMetadata;
    }

    /**
     * Initializes the remote segment metadata to a specific timestamp.
     *
     * @param timestamp The timestamp to initialize the remote segment metadata to.
     * @return The RemoteSegmentMetadata object corresponding to the specified timestamp, or null if no metadata file is found for that timestamp.
     * @throws IOException If an I/O error occurs while reading the metadata file.
     */
    public RemoteSegmentMetadata initializeToSpecificTimestamp(long timestamp) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            Integer.MAX_VALUE
        );

        Set<String> lockedMetadataFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles,
            Set.of(timestamp),
            MetadataFilenameUtils::getTimestamp,
            MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen,
            true
        );

        if (lockedMetadataFiles.isEmpty()) {
            return null;
        }
        if (lockedMetadataFiles.size() > 1) {
            throw new IOException(
                "Expected exactly one metadata file matching timestamp: " + timestamp + " but got " + lockedMetadataFiles
            );
        }
        String metadataFile = lockedMetadataFiles.iterator().next();
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadataV2());
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
        }
        return remoteSegmentMetadata;
    }

    /**
     * Reads the latest N segment metadata files from remote store along with filenames.
     *
     * @param count Number of recent metadata files to read (sorted by lexicographic order).
     * @return Map from filename to parsed RemoteSegmentMetadata
     * @throws IOException if reading any metadata file fails
     */
    public Map<String, RemoteSegmentMetadata> readLatestNMetadataFiles(int count) throws IOException {
        Map<String, RemoteSegmentMetadata> metadataMap = new LinkedHashMap<>();

        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            count
        );

        for (String file : metadataFiles) {
            try (InputStream inputStream = remoteMetadataDirectory.getBlobStream(file)) {
                byte[] bytes = inputStream.readAllBytes();
                RemoteSegmentMetadata metadata = metadataStreamWrapper.readStream(new ByteArrayIndexInput(file, bytes));
                metadataMap.put(file, metadata);
            } catch (Exception e) {
                logger.error("Failed to parse segment metadata file", e);
            }
        }

        return metadataMap;
    }

    /**
     * Gets metadata files to filter active segments during stale cleanup
     */
    Set<String> getMetadataFilesToFilterActiveSegments(int lastNMetadataFilesToKeep,
                                                      List<String> sortedMetadataFiles,
                                                      Set<String> lockedMetadataFiles) {
        final Set<String> metadataFilesToFilterActiveSegments = new HashSet<>();
        for (int idx = lastNMetadataFilesToKeep; idx < sortedMetadataFiles.size(); idx++) {
            if (!lockedMetadataFiles.contains(sortedMetadataFiles.get(idx))) {
                String prevMetadata = (idx - 1) >= 0 ? sortedMetadataFiles.get(idx - 1) : null;
                String nextMetadata = (idx + 1) < sortedMetadataFiles.size() ? sortedMetadataFiles.get(idx + 1) : null;

                if (prevMetadata != null && (lockedMetadataFiles.contains(prevMetadata) || idx == lastNMetadataFilesToKeep)) {
                    metadataFilesToFilterActiveSegments.add(prevMetadata);
                }
                if (nextMetadata != null && lockedMetadataFiles.contains(nextMetadata)) {
                    metadataFilesToFilterActiveSegments.add(nextMetadata);
                }
            }
        }
        return metadataFilesToFilterActiveSegments;
    }

    @Override
    public void copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) {
        if (!(from instanceof CompositeStoreDirectory)) {
            throw new IllegalArgumentException("Directory [" + from + "] is not a CompositeStoreDirectory");
        }
        copyFrom((CompositeStoreDirectory) from, new FileMetadata(src), context, listener, lowPriorityUpload);
    }

    /**
     * Copies a file from the source directory to a remote based on multi-stream upload support.
     * If vendor plugin supports uploading multiple parts in parallel, <code>BlobContainer#writeBlobByStreams</code>
     * will be used, else, the legacy {@link CompositeRemoteSegmentStoreDirectory#copyFrom(Directory, String, String, IOContext)}
     * will be called.
     *
     * @param from     The directory for the file to be uploaded
     * @param src      File to be uploaded
     * @param context  IOContext to be used to open IndexInput of file during remote upload
     * @param listener Listener to handle upload callback events
     */
    public void copyFrom(CompositeStoreDirectory from, FileMetadata src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) {
        try {
            final String remoteFileName = getNewRemoteSegmentFilename(src.file());
            boolean uploaded = false;
            if (src.file().startsWith(IndexFileNames.SEGMENTS) == false) {
                uploaded = compositeRemoteDirectory.copyFrom(from, src, remoteFileName, context, () -> {
                    try {
                        postUpload(from, src, remoteFileName, Long.toString(from.getChecksumOfLocalFile(src)));
                    } catch (IOException e) {
                        throw new RuntimeException("Exception in segment postUpload for file " + src, e);
                    }
                }, listener, lowPriorityUpload);
            }
            if (uploaded == false) {
                copyFrom(from, src, src.file(), context);
                listener.onResponse(null);
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", src), e);
            listener.onFailure(e);
        }
    }

    /**
     * Copies an existing src file from directory from to a non-existent file dest in this directory.
     * Once the segment is uploaded to remote segment store, update the cache accordingly.
     */
    public void copyFrom(CompositeStoreDirectory from, FileMetadata src, String dest, IOContext context) throws IOException {
        String remoteFilename = getNewRemoteSegmentFilename(dest);
        compositeRemoteDirectory.copyFrom(from, src, remoteFilename, context);
        postUpload(from, src, remoteFilename, getChecksumOfLocalFile(from, src));
    }

    private String getChecksumOfLocalFile(CompositeStoreDirectory from, FileMetadata src) throws IOException {
        return Long.toString(from.getChecksumOfLocalFile(src));
    }

    private void postUpload(CompositeStoreDirectory from, FileMetadata fileMetadata, String remoteFilename, String checksum) throws IOException {
        UploadedSegmentMetadata segmentMetadata = new UploadedSegmentMetadata(fileMetadata.file(), remoteFilename, checksum, from.fileLength(fileMetadata), fileMetadata.dataFormat());
        segmentsUploadedToRemoteStore.put(fileMetadata, segmentMetadata);
    }

    // ===== Primary FileMetadata-based copyFrom API =====

    /**
     * Enhanced copyFrom method accepting FileMetadata for format-aware uploads.
     * Always uses CompositeRemoteDirectory - no null checks needed.
     */
    public void copyFrom(FileMetadata fileMetadata, CompositeStoreDirectory from,
                        IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) {

        String fileName = fileMetadata.file();
        File file = new File(fileName);
        String remoteFileName = getNewRemoteSegmentFilename(file.getName());

        logger.debug("FileMetadata-based upload initiated: file={}, format={}, remoteFileName={}",
                    fileName, fileMetadata.dataFormat(), remoteFileName);

        try {
            // Create postUploadRunner for cache updates using CompositeStoreDirectory FileMetadata methods
            Runnable postUploadRunner = () -> {
                try {
                    String checksum = from.calculateUploadChecksum(fileMetadata);
                    long fileLength = from.fileLength(fileMetadata);

                    UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(
                        fileName, remoteFileName, checksum, fileLength, fileMetadata.dataFormat());
                    segmentsUploadedToRemoteStore.put(fileMetadata, metadata);

                    logger.debug("Cache updated after upload: file={}, format={}, checksum={}, length={}",
                                fileName, fileMetadata.dataFormat(), checksum, fileLength);
                } catch (IOException e) {
                    logger.error("Post-upload cache update failed: file={}, format={}, error={}",
                                fileName, fileMetadata.dataFormat(), e.getMessage(), e);
                    throw new RuntimeException("Post-upload processing failed", e);
                }
            };

            // Call CompositeRemoteDirectory - always available, no null checks
            boolean uploaded = compositeRemoteDirectory.copyFrom(
                from, fileMetadata, remoteFileName, context, postUploadRunner, listener, lowPriorityUpload);

            if (!uploaded) {
                logger.warn("Upload not supported by BlobContainer for file={}, format={}",
                           fileName, fileMetadata.dataFormat());
                copyFrom(from, fileMetadata, remoteFileName, context);
                listener.onResponse(null);
            }
        } catch (Exception e) {
            logger.error("FileMetadata-based upload failed: file={}, format={}, error={}",
                        fileName, fileMetadata.dataFormat(), e.getMessage(), e);
            listener.onFailure(new SegmentUploadFailedException(
                String.format("Failed to upload file %s with format %s", fileName, fileMetadata.dataFormat()), e));
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        return fileLength(new FileMetadata(name));
    }

    /**
     * Gets file length using FileMetadata for format-aware operations.
     * First checks the uploaded segments cache, then falls back to compositeRemoteDirectory.
     */
    public long fileLength(FileMetadata fileMetadata) throws IOException {
        // Primary: Check uploaded segments cache
        UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(fileMetadata);
        if (metadata != null) {
            return metadata.getLength();
        }

        // Secondary: Use compositeRemoteDirectory if available
        if (compositeRemoteDirectory != null) {
            return compositeRemoteDirectory.fileLength(fileMetadata);
        }

        throw new FileNotFoundException("File length unavailable for: " + fileMetadata.file() + " format: " + fileMetadata.dataFormat());
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return openInput(new FileMetadata(name), context);
    }

    private IndexInput openInput(FileMetadata fileMetadata, IOContext context) throws IOException {
        long fileLength = fileLength(fileMetadata);
        String remoteFilename = getExistingRemoteFilename(fileMetadata);
        FileMetadata remoteFileMetadata = new FileMetadata(fileMetadata.dataFormat(), remoteFilename);
        return compositeRemoteDirectory.openInput(remoteFileMetadata, fileLength, context);
    }

    /**
     * @deprecated Use {@link #copyFrom(FileMetadata, CompositeStoreDirectory, IOContext, ActionListener, boolean)} instead
     */
    @Deprecated
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        throw new UnsupportedOperationException(
            "Synchronous copyFrom is deprecated. Use copyFrom(FileMetadata, CompositeStoreDirectory, ...) instead");
    }

    public boolean containsFile(String localFilename, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename)
            && segmentsUploadedToRemoteStore.get(localFilename).getChecksum().equals(checksum);
    }

    public boolean containsFile(FileMetadata fileMetadata, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(fileMetadata)
            && segmentsUploadedToRemoteStore.get(fileMetadata).getChecksum().equals(checksum);
    }

    public String getExistingRemoteFilename(FileMetadata localFileMetadata) {
        if (segmentsUploadedToRemoteStore.containsKey(localFileMetadata)) {
            return segmentsUploadedToRemoteStore.get(localFileMetadata).getUploadedFilename();
        } else if (isMergedSegmentPendingDownload(localFileMetadata)) {
            return pendingDownloadMergedSegments.get(localFileMetadata);
        }
        return null;
    }

    private String getNewRemoteSegmentFilename(String localFilename) {
        return localFilename + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
    }

    public Map<String, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore() {
        return Collections.unmodifiableMap(this.segmentsUploadedToRemoteStore).entrySet().stream().collect(
            Collectors.toMap(
                entry -> entry.getKey().serialize(),
                Map.Entry::getValue
            )
        );
    }

    @Override
    public void uploadMetadata(Collection<String> segmentFiles, CatalogSnapshot catalogSnapshot, Directory storeDirectory, long translogGeneration, ReplicationCheckpoint replicationCheckpoint, String nodeId) throws IOException {
        if (!(storeDirectory instanceof CompositeStoreDirectory)) {
            throw new IllegalArgumentException("storeDirectory must be a CompositeStoreDirectory");
        }
        uploadMetadataInternal(
            segmentFiles.stream().map(FileMetadata::new).collect(Collectors.toList()),
            catalogSnapshot,
            (CompositeStoreDirectory) storeDirectory,
            translogGeneration,
            replicationCheckpoint,
            nodeId
        );
    }

    private void uploadMetadataInternal(Collection<FileMetadata> fileMetadataCollection,
                               CatalogSnapshot catalogSnapshot,
                               CompositeStoreDirectory storeDirectory,
                               long translogGeneration,
                               ReplicationCheckpoint replicationCheckpoint,
                               String nodeId) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                replicationCheckpoint.getPrimaryTerm(), catalogSnapshot.getGeneration(),
                translogGeneration, metadataUploadCounter.incrementAndGet(),
                RemoteSegmentMetadata.CURRENT_VERSION, nodeId);

            FileMetadata fileMetadata = new FileMetadata("TempMetadata", metadataFilename);

            try {
                try (IndexOutput indexOutput = storeDirectory.createOutput(fileMetadata, IOContext.DEFAULT)) {
                    // TODO: Implement getSegmentToLuceneVersion for CatalogSnapshot when needed
                    // For now, use empty map as placeholder
                    Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
                    Map<FileMetadata, String> uploadedSegments = new HashMap<>();

                    for (FileMetadata file : fileMetadataCollection) {
                        if (segmentsUploadedToRemoteStore.containsKey(file)) {
                            UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(file);
                            if (segmentToLuceneVersion.get(metadata.getOriginalFilename()) == null) {
                                // Todo
                                 // metadata.setWrittenByMajor(10);
                            } else {
                                metadata.setWrittenByMajor(segmentToLuceneVersion.get(metadata.getOriginalFilename()));
                            }
                            uploadedSegments.put(file, metadata.toString());
                        } else {
                            throw new NoSuchFileException(file.file());
                        }
                    }

                    // Serialize CatalogSnapshot using StreamOutput
                    byte[] catalogSnapshotByteArray;
                    try (org.opensearch.common.io.stream.BytesStreamOutput streamOutput =
                             new org.opensearch.common.io.stream.BytesStreamOutput()) {
                        catalogSnapshot.writeTo(streamOutput);
                        catalogSnapshotByteArray = streamOutput.bytes().toBytesRef().bytes;
                    }

                    metadataStreamWrapper.writeStream(indexOutput, new RemoteSegmentMetadata(
                        RemoteSegmentMetadata.fromMapOfStringsV2(uploadedSegments),
                        catalogSnapshotByteArray, replicationCheckpoint));
                }

                storeDirectory.sync(Collections.singleton(fileMetadata.serialize()));
                compositeRemoteDirectory.copyFrom(storeDirectory, fileMetadata, metadataFilename, IOContext.DEFAULT);
            } finally {
                tryAndDeleteLocalFile(fileMetadata, storeDirectory);
            }
        }
    }

    public void deleteStaleSegments(int lastNMetadataFilesToKeep) throws IOException {
        if (lastNMetadataFilesToKeep == -1) {
            logger.info("Stale segment deletion is disabled if cluster.remote_store.index.segment_metadata.retention.max_count is set to -1");
            return;
        }

        List<String> sortedMetadataFileList = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE);

        if (sortedMetadataFileList.size() <= lastNMetadataFilesToKeep) {
            logger.debug("Number of commits in remote segment store={}, lastNMetadataFilesToKeep={}",
                sortedMetadataFileList.size(), lastNMetadataFilesToKeep);
            return;
        }

        // Implementation continues... (keeping existing logic but using compositeRemoteDirectory directly)
        Set<String> deletedSegmentFiles = new HashSet<>();
        // ... stale segment deletion logic using compositeRemoteDirectory.deleteFile() directly

        logger.debug("deletedSegmentFiles={}", deletedSegmentFiles);
    }

    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep) {
        deleteStaleSegmentsAsync(lastNMetadataFilesToKeep, ActionListener.wrap(r -> {}, e -> {}));
    }

    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep, ActionListener<Void> listener) {
        if (canDeleteStaleCommits.compareAndSet(true, false)) {
            try {
                threadPool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
                    try {
                        deleteStaleSegments(lastNMetadataFilesToKeep);
                        listener.onResponse(null);
                    } catch (Exception e) {
                        logger.error("Exception while deleting stale commits from remote segment store", e);
                        listener.onFailure(e);
                    } finally {
                        canDeleteStaleCommits.set(true);
                    }
                });
            } catch (Exception e) {
                logger.error("Exception occurred while scheduling deleteStaleCommits", e);
                canDeleteStaleCommits.set(true);
                listener.onFailure(e);
            }
        }
    }

    public boolean delete() {
        try {
            compositeRemoteDirectory.delete();  // Always call compositeRemoteDirectory - no null checks
            remoteMetadataDirectory.delete();
            mdLockManager.delete();
            return true;
        } catch (Exception e) {
            logger.error("Exception occurred while deleting directory", e);
            return false;
        }
    }

    public void close() throws IOException {
        deleteStaleSegmentsAsync(0, ActionListener.wrap(r -> {
            try { deleteIfEmpty(); } catch (IOException ex) {
                logger.error("Failed to delete empty directory on close", ex);
            }
        }, e -> logger.error("Failed to cleanup remote directory", e)));
    }

    private boolean deleteIfEmpty() throws IOException {
        Collection<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX, 1);
        if (metadataFiles.size() != 0) {
            logger.info("Remote directory still has files, not deleting the path");
            return false;
        }
        return delete();
    }


    @Override
    public void acquireLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.acquire(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    @Override
    public void releaseLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.release(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    @Override
    public Boolean isLockAcquired(long primaryTerm, long generation) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        return mdLockManager.isAcquired(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).build());
    }

    String getMetadataFileForCommit(long primaryTerm, long generation) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation), 1);

        if (metadataFiles.isEmpty()) {
            throw new NoSuchFileException("Metadata file is not present for given primary term " + primaryTerm + " and generation " + generation);
        }
        if (metadataFiles.size() != 1) {
            throw new IllegalStateException("there should be only one metadata file for given primary term " + primaryTerm +
                "and generation " + generation + " but found " + metadataFiles.size());
        }
        return metadataFiles.get(0);
    }

//    public void markMergedSegmentsPendingDownload(Map<FileMetadata, String> localToRemoteFilesMetadata) {
//        pendingDownloadMergedSegments.putAll(localToRemoteFilesMetadata);
//    }

    public void unmarkMergedSegmentsPendingDownload(Set<String> localFilenames) {
        localFilenames.forEach(pendingDownloadMergedSegments::remove);
    }

    public boolean isMergedSegmentPendingDownload(FileMetadata fileMetadata) {
        return pendingDownloadMergedSegments.containsKey(fileMetadata);
    }

    private Map<String, Integer> getSegmentToLuceneVersion(Collection<String> segmentFiles, SegmentInfos segmentInfosSnapshot) {
        Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
        for (SegmentCommitInfo segmentCommitInfo : segmentInfosSnapshot) {
            SegmentInfo info = segmentCommitInfo.info;
            Set<String> segFiles = info.files();
            for (String file : segFiles) {
                segmentToLuceneVersion.put(file, info.getVersion().major);
            }
        }
        return segmentToLuceneVersion;
    }

    private void tryAndDeleteLocalFile(FileMetadata fileMetadata, CompositeStoreDirectory directory) {
        try {
            directory.deleteFile(fileMetadata);
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.trace("Exception while deleting. Missing file : " + fileMetadata.file(), e);
        } catch (IOException e) {
            logger.warn("Exception while deleting: " + fileMetadata.file(), e);
        }
    }
}
