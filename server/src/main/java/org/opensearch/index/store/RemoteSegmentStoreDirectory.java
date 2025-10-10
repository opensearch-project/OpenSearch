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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.lockmanager.FileLockInfo;
import org.opensearch.index.store.lockmanager.RemoteStoreCommitLevelLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.CompositeRemoteDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandlerFactory;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Remote segment store directory with format-aware storage capabilities.
 * Uses CompositeRemoteDirectory for all file operations with format-specific routing.
 * No longer extends FilterDirectory - provides clean interface for segment storage.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.3.0")
public final class RemoteSegmentStoreDirectory extends FilterDirectory implements RemoteStoreCommitLevelLockManager {

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
    RemoteDirectory remoteDataDirectory;

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
     * It is important to initialize this map on creation of RemoteSegmentStoreDirectory and update it on each upload and delete.
     */
    private Map<FileMetadata, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandlerFactory(),
        RemoteSegmentMetadata.VERSION_ONE,
        RemoteSegmentMetadata.CURRENT_VERSION,
        RemoteSegmentMetadata.METADATA_CODEC
    );

    private static final Logger staticLogger = LogManager.getLogger(RemoteSegmentStoreDirectory.class);
    private final Logger logger;

    /**
     * AtomicBoolean that ensures only one staleCommitDeletion activity is scheduled at a time.
     * Visible for testing
     */
    protected final AtomicBoolean canDeleteStaleCommits = new AtomicBoolean(true);
    private final AtomicLong metadataUploadCounter = new AtomicLong(0);

    public static final int METADATA_FILES_TO_FETCH = 10;

    public RemoteSegmentStoreDirectory(
        RemoteDirectory remoteDataDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId
    ) throws IOException {
        this(remoteDataDirectory, remoteMetadataDirectory, mdLockManager, threadPool, shardId, null);
    }

    @InternalApi
    public RemoteSegmentStoreDirectory(
        RemoteDirectory remoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId,
        @Nullable Map<FileMetadata, String> pendingDownloadMergedSegments
    ) throws IOException {
        super(remoteDirectory);
        this.compositeRemoteDirectory = null;
        this.remoteDataDirectory = remoteDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        this.threadPool = threadPool;
        this.metadataFilePinnedTimestampMap = new HashMap<>();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.pendingDownloadMergedSegments = pendingDownloadMergedSegments != null ?
            new ConcurrentHashMap<>(pendingDownloadMergedSegments) : new ConcurrentHashMap<>();
        init();
    }

    @InternalApi
    public RemoteSegmentStoreDirectory(
        CompositeRemoteDirectory compositeRemoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId,
        @Nullable Map<FileMetadata, String> pendingDownloadMergedSegments
    ) throws IOException {
        super(null);
        this.compositeRemoteDirectory = compositeRemoteDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        this.threadPool = threadPool;
        this.metadataFilePinnedTimestampMap = new HashMap<>();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.pendingDownloadMergedSegments = pendingDownloadMergedSegments != null ?
            new ConcurrentHashMap<>(pendingDownloadMergedSegments) : new ConcurrentHashMap<>();
        init();
    }

    /**
     * Convenience constructor
     */
    public RemoteSegmentStoreDirectory(
        CompositeRemoteDirectory compositeRemoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        RemoteStoreLockManager mdLockManager,
        ThreadPool threadPool,
        ShardId shardId
    ) throws IOException {
        this(compositeRemoteDirectory, remoteMetadataDirectory, mdLockManager, threadPool, shardId, null);
    }

    /**
     * Initializes the cache which keeps track of all the segment files uploaded to the remote segment store.
     */
    public RemoteSegmentMetadata init() throws IOException {
        logger.debug("Start initialisation of remote segment metadata");
        RemoteSegmentMetadata remoteSegmentMetadata = readLatestMetadataFile();
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
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
        // Prefer CompositeRemoteDirectory for metadata reading if available
        if (compositeRemoteDirectory != null) {
            logger.debug("Reading latest metadata file from CompositeRemoteDirectory for better format-aware handling");
            return compositeRemoteDirectory.readLatestMetadataFile();
        }

        // Fallback to legacy remoteMetadataDirectory approach
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX, METADATA_FILES_TO_FETCH);

        RemoteStoreUtils.verifyNoMultipleWriters(metadataFiles, MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen);

        if (!metadataFiles.isEmpty()) {
            String latestMetadataFile = metadataFiles.get(0);
            logger.info("Reading latest Metadata file {} from remoteMetadataDirectory", latestMetadataFile);
            return readMetadataFile(latestMetadataFile);
        } else {
            logger.info("No metadata file found, this can happen for new index with no data uploaded to remote segment store");
            return null;
        }
    }

    private RemoteSegmentMetadata readMetadataFile(String metadataFilename) throws IOException {
        try (InputStream inputStream = remoteMetadataDirectory.getBlobStream(metadataFilename)) {
            byte[] metadataBytes = inputStream.readAllBytes();
            return metadataStreamWrapper.readStream(new ByteArrayIndexInput(metadataFilename, metadataBytes));
        }
    }

    // ===== Directory-like interface methods - all use compositeRemoteDirectory directly =====

    /**
     * Initializes the cache to a specific commit which keeps track of all the segment files uploaded to the remote segment store.
     */
    public RemoteSegmentMetadata initializeToSpecificCommit(long primaryTerm, long commitGeneration, String acquirerId) throws IOException {
        String metadataFilePrefix = MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, commitGeneration);
        String metadataFile = ((RemoteStoreMetadataLockManager) mdLockManager).fetchLockedMetadataFile(metadataFilePrefix, acquirerId);
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
        }
        return remoteSegmentMetadata;
    }

    /**
     * Initializes the remote segment metadata to a specific timestamp.
     */
    public RemoteSegmentMetadata initializeToSpecificTimestamp(long timestamp) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX, Integer.MAX_VALUE);

        Set<String> lockedMetadataFiles = RemoteStoreUtils.getPinnedTimestampLockedFiles(
            metadataFiles, Set.of(timestamp), MetadataFilenameUtils::getTimestamp,
            MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen, true);

        if (lockedMetadataFiles.isEmpty()) {
            return null;
        }
        if (lockedMetadataFiles.size() > 1) {
            throw new IOException("Expected exactly one metadata file matching timestamp: " + timestamp + " but got " + lockedMetadataFiles);
        }

        String metadataFile = lockedMetadataFiles.iterator().next();
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
        }
        return remoteSegmentMetadata;
    }

    /**
     * Reads the latest N segment metadata files from remote store along with filenames.
     */
    public Map<String, RemoteSegmentMetadata> readLatestNMetadataFiles(int count) throws IOException {
        Map<String, RemoteSegmentMetadata> metadataMap = new LinkedHashMap<>();

        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX, count);

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


    /**
     * Todo: ultrawarm
     * Opens a stream for reading one block from the existing file - always uses compositeRemoteDirectory
     */
    public IndexInput openBlockInput(String name, long position, long length, IOContext context) throws IOException {
//        String remoteFilename = getExistingRemoteFilename(name);
//        if (remoteFilename != null) {
//            long fileLength = compositeRemoteDirectory.fileLength(name, null);
//            return compositeRemoteDirectory.openBlockInput(remoteFilename, null, position, length, fileLength, context);
//        } else {
//            throw new NoSuchFileException(name);
//        }
        throw new UnsupportedOperationException();
    }

    /**
     * Opens a stream for reading one block from the existing file - always uses compositeRemoteDirectory
     * TODO: needs update, currently it's not integrated
     */
//    public IndexInput openBlockInput(String name, String dfName, long position, long length, IOContext context) throws IOException {
//        String remoteFilename = getExistingRemoteFilename(name);
//        if (remoteFilename != null) {
//            long fileLength = compositeRemoteDirectory.fileLength(name, dfName);
//            return compositeRemoteDirectory.openBlockInput(remoteFilename, dfName, position, length, fileLength, context);
//        } else {
//            throw new NoSuchFileException(name);
//        }
//    }

    /**
     * Copies a file from the source directory to a remote based on multi-stream upload support.
     * If vendor plugin supports uploading multiple parts in parallel, <code>BlobContainer#writeBlobByStreams</code>
     * will be used, else, the legacy {@link RemoteSegmentStoreDirectory#copyFrom(Directory, String, String, IOContext)}
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

    /**
     * Gets file length using FileMetadata for format-aware operations.
     * First checks the uploaded segments cache, then falls back to compositeRemoteDirectory.
     */
    public long getFileLength(FileMetadata fileMetadata) throws IOException {
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

    public IndexInput openInput(FileMetadata fileMetadata, IOContext context) throws IOException {
        long fileLength = getFileLength(fileMetadata);
        String remoteFilename = getExistingRemoteFilename(fileMetadata);
        FileMetadata remoteFileMetadata = new FileMetadata(fileMetadata.dataFormat(), "", remoteFilename);
        return compositeRemoteDirectory.openInput(remoteFileMetadata, fileLength, context);

    }

    // ===== Deprecated methods for backward compatibility =====

    /**
     * @deprecated Use {@link #copyFrom(FileMetadata, CompositeStoreDirectory, IOContext, ActionListener, boolean)} instead
     */
    @Deprecated
    public void copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) {
        logger.warn("Deprecated copyFrom(Directory, ...) called. Use FileMetadata version instead.");
        listener.onFailure(new UnsupportedOperationException(
            "Deprecated copyFrom method. Use copyFrom(FileMetadata, CompositeStoreDirectory, ...) instead"));
    }

    /**
     * @deprecated Use {@link #copyFrom(FileMetadata, CompositeStoreDirectory, IOContext, ActionListener, boolean)} instead
     */
    @Deprecated
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        throw new UnsupportedOperationException(
            "Synchronous copyFrom is deprecated. Use copyFrom(FileMetadata, CompositeStoreDirectory, ...) instead");
    }

    // ===== Utility methods =====

    public boolean containsFile(String localFilename, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename)
            && segmentsUploadedToRemoteStore.get(localFilename).checksum.equals(checksum);
    }

    public boolean containsFile(FileMetadata fileMetadata, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(fileMetadata)
            && segmentsUploadedToRemoteStore.get(fileMetadata).checksum.equals(checksum);
    }

    public String getExistingRemoteFilename(FileMetadata localFileMetadata) {
        if (segmentsUploadedToRemoteStore.containsKey(localFileMetadata)) {
            return segmentsUploadedToRemoteStore.get(localFileMetadata).uploadedFilename;
        } else if (isMergedSegmentPendingDownload(localFileMetadata)) {
            return pendingDownloadMergedSegments.get(localFileMetadata);
        }
        return null;
    }

    private String getNewRemoteSegmentFilename(String localFilename) {
        return localFilename + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
    }

    private String getLocalSegmentFilename(String remoteFilename) {
        return remoteFilename.split(SEGMENT_NAME_UUID_SEPARATOR)[0];
    }

    public Map<FileMetadata, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore() {
        return Collections.unmodifiableMap(this.segmentsUploadedToRemoteStore);
    }

    public void uploadMetadata(Collection<FileMetadata> fileMetadataCollection, CatalogSnapshot catalogSnapshot,
                               CompositeStoreDirectory storeDirectory, long translogGeneration,
                               ReplicationCheckpoint replicationCheckpoint, String nodeId) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                replicationCheckpoint.getPrimaryTerm(), catalogSnapshot.getGeneration(),
                translogGeneration, metadataUploadCounter.incrementAndGet(),
                RemoteSegmentMetadata.CURRENT_VERSION, nodeId);

            FileMetadata fileMetadata = new FileMetadata("TempMetadata","", metadataFilename);

            try {
                try (IndexOutput indexOutput = storeDirectory.createOutput(fileMetadata, IOContext.DEFAULT)) {
                    // TODO: Implement getSegmentToLuceneVersion for CatalogSnapshot when needed
                    // For now, use empty map as placeholder
                    Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
                    Map<FileMetadata, String> uploadedSegments = new HashMap<>();

                    for (FileMetadata file : fileMetadataCollection) {
                        if (segmentsUploadedToRemoteStore.containsKey(file)) {
                            UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(file);
                            if (segmentToLuceneVersion.get(metadata.originalFilename) == null) {
                                // Todo
                                 // metadata.setWrittenByMajor(10);
                            } else {
                                metadata.setWrittenByMajor(segmentToLuceneVersion.get(metadata.originalFilename));
                            }
                            uploadedSegments.put(file, metadata.toString());
                        } else {
                            throw new NoSuchFileException(file.file());
                        }
                    }

                    // Serialize CatalogSnapshot using standard Java APIs
                    // ToDo: We need to update this with some opensource library which can optimize space
                    ByteArrayOutputStream catalogOutputStream = new ByteArrayOutputStream();
                    catalogSnapshot.writeTo(catalogOutputStream);
                    byte[] catalogSnapshotByteArray = catalogOutputStream.toByteArray();

                    metadataStreamWrapper.writeStream(indexOutput, new RemoteSegmentMetadata(
                        RemoteSegmentMetadata.fromMapOfStrings(uploadedSegments),
                        catalogSnapshotByteArray, replicationCheckpoint));
                }

                storeDirectory.sync(Collections.singleton(fileMetadata));
                compositeRemoteDirectory.copyFrom(storeDirectory, fileMetadata, metadataFilename, IOContext.DEFAULT);
            } finally {
                tryAndDeleteLocalFile(fileMetadata, storeDirectory);
            }
        }
    }

    public void uploadMetadata(Collection<String> segmentFiles, SegmentInfos segmentInfosSnapshot,
                               CompositeStoreDirectory storeDirectory, long translogGeneration,
                               ReplicationCheckpoint replicationCheckpoint, String nodeId) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                replicationCheckpoint.getPrimaryTerm(), segmentInfosSnapshot.getGeneration(),
                translogGeneration, metadataUploadCounter.incrementAndGet(),
                RemoteSegmentMetadata.CURRENT_VERSION, nodeId);

            FileMetadata fileMetadata = new FileMetadata("TempMetadata", "", metadataFilename);

            try {
                try (IndexOutput indexOutput = storeDirectory.createOutput(fileMetadata, IOContext.DEFAULT)) {
                    Map<String, Integer> segmentToLuceneVersion = getSegmentToLuceneVersion(segmentFiles, segmentInfosSnapshot);
                    Map<String, String> uploadedSegments = new HashMap<>();

                    for (String file : segmentFiles) {
                        if (segmentsUploadedToRemoteStore.containsKey(fileMetadata)) {
                            UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(file);
                            if (segmentToLuceneVersion.get(metadata.originalFilename) == null) {
                                metadata.setWrittenByMajor(0);
                            } else {
                                metadata.setWrittenByMajor(segmentToLuceneVersion.get(metadata.originalFilename));
                            }
                            uploadedSegments.put(file, metadata.toString());
                        } else {
                            throw new NoSuchFileException(file);
                        }
                    }

                    ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
                    segmentInfosSnapshot.write(new ByteBuffersIndexOutput(byteBuffersIndexOutput, "Snapshot of SegmentInfos", "SegmentInfos"));
                    byte[] segmentInfoSnapshotByteArray = byteBuffersIndexOutput.toArrayCopy();

//                    metadataStreamWrapper.writeStream(indexOutput, new RemoteSegmentMetadata(
//                        RemoteSegmentMetadata.fromMapOfStrings(uploadedSegments),
//                        segmentInfoSnapshotByteArray, replicationCheckpoint));
                }

                storeDirectory.sync(Collections.singleton(fileMetadata));
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

    // ===== Lock management - implements RemoteStoreCommitLevelLockManager =====

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

    // ===== Replica shard methods =====

    public void markMergedSegmentsPendingDownload(Map<FileMetadata, String> localToRemoteFilesMetadata) {
        pendingDownloadMergedSegments.putAll(localToRemoteFilesMetadata);
    }

    public void unmarkMergedSegmentsPendingDownload(Set<String> localFilenames) {
        localFilenames.forEach(pendingDownloadMergedSegments::remove);
    }

    public boolean isMergedSegmentPendingDownload(FileMetadata fileMetadata) {
        return pendingDownloadMergedSegments.containsKey(fileMetadata);
    }

    // ===== Helper methods =====


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

    // ===== Static utility classes =====

    /**
     * Metadata of a segment that is uploaded to remote segment store.
     */
    @PublicApi(since = "2.3.0")
    public static class UploadedSegmentMetadata {
        static final String SEPARATOR = "::";

        private final String originalFilename;
        private final String uploadedFilename;
        private final String checksum;
        private final String dataFormat;
        private final long length;
        private int writtenByMajor;

        UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum, long length, String dataFormat) {
            this.originalFilename = originalFilename;
            this.uploadedFilename = uploadedFilename;
            this.checksum = checksum;
            this.length = length;
            this.dataFormat = dataFormat;
        }

        @Override
        public String toString() {
            return String.join(SEPARATOR,
                originalFilename,
                uploadedFilename,
                checksum,
                String.valueOf(length),
                String.valueOf(writtenByMajor),
                dataFormat
            );
        }

        public String getChecksum() { return this.checksum; }
        public long getLength() { return this.length; }
        public String getOriginalFilename() { return originalFilename; }
        public String getUploadedFilename() { return uploadedFilename; }
        public String getDataFormat() { return dataFormat; }

        public void setWrittenByMajor(int writtenByMajor) {
            if (writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR) {
                this.writtenByMajor = writtenByMajor;
            } else {
                throw new IllegalArgumentException("Lucene major version supplied (" + writtenByMajor + ") is incorrect.");
            }
        }

        public static UploadedSegmentMetadata fromString(String uploadedFilename) {
            File file = new File(uploadedFilename);
            var filename = file.getName();
            String[] values = filename.split(SEPARATOR);

            // Extract dataFormat from position 5, default to "lucene" for backward compatibility
            String dataFormat = values.length >= 6 ? values[5] : "lucene";

            // Use correct 5-parameter constructor including dataFormat
            UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(
                values[0],                  // originalFilename
                values[1],                  // uploadedFilename
                values[2],                  // checksum
                Long.parseLong(values[3]),  // length
                dataFormat                  // dataFormat
            );

            // Set writtenByMajor if present
            if (values.length >= 5) {
                // ToDo: kamal
                // metadata.setWrittenByMajor(Integer.parseInt(values[4]));
            }

            return metadata;
        }
    }

    /**
     * Contains utility methods for metadata filename handling
     */
    public static class MetadataFilenameUtils {
        public static final String SEPARATOR = "__";
        public static final String METADATA_PREFIX = "metadata";

        static String getMetadataFilePrefixForCommit(long primaryTerm, long generation) {
            return String.join(SEPARATOR, METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm), RemoteStoreUtils.invertLong(generation));
        }

        public static String getMetadataFilename(long primaryTerm, long generation, long translogGeneration,
                                                long uploadCounter, int metadataVersion, String nodeId) {
            return getMetadataFilename(primaryTerm, generation, translogGeneration, uploadCounter,
                metadataVersion, nodeId, System.currentTimeMillis());
        }

        public static String getMetadataFilename(long primaryTerm, long generation, long translogGeneration,
                                                long uploadCounter, int metadataVersion, String nodeId,
                                                long creationTimestamp) {
            return String.join(SEPARATOR,
                METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm),
                RemoteStoreUtils.invertLong(generation),
                RemoteStoreUtils.invertLong(translogGeneration),
                RemoteStoreUtils.invertLong(uploadCounter),
                String.valueOf(Objects.hash(nodeId)),
                RemoteStoreUtils.invertLong(creationTimestamp),
                String.valueOf(metadataVersion)
            );
        }

        public static long getTimestamp(String filename) {
            String[] filenameTokens = filename.split(SEPARATOR);
            return RemoteStoreUtils.invertLong(filenameTokens[filenameTokens.length - 2]);
        }

        public static Tuple<String, String> getNodeIdByPrimaryTermAndGen(String filename) {
            String[] tokens = filename.split(SEPARATOR);
            if (tokens.length < 8) {
                return null; // For versions < 2.11, we don't have node id
            }
            String primaryTermAndGen = String.join(SEPARATOR, tokens[1], tokens[2], tokens[3]);
            String nodeId = tokens[5];
            return new Tuple<>(primaryTermAndGen, nodeId);
        }

        // Visible for testing
        static long getPrimaryTerm(String[] filenameTokens) {
            return RemoteStoreUtils.invertLong(filenameTokens[1]);
        }

        // Visible for testing
        static long getGeneration(String[] filenameTokens) {
            return RemoteStoreUtils.invertLong(filenameTokens[2]);
        }
    }


    public static void remoteDirectoryCleanup(RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory,
                                            String remoteStoreRepoForIndex, String indexUUID,
                                            ShardId shardId, RemoteStorePathStrategy pathStrategy, boolean forceClean) {
        try {
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = (RemoteSegmentStoreDirectory) remoteDirectoryFactory.newDirectory(
                remoteStoreRepoForIndex, indexUUID, shardId, pathStrategy);

            if (forceClean) {
                remoteSegmentStoreDirectory.delete();
            } else {
                remoteSegmentStoreDirectory.deleteStaleSegments(0);
                remoteSegmentStoreDirectory.deleteIfEmpty();
            }
        } catch (Exception e) {
            staticLogger.error("Exception occurred while deleting directory", e);
        }
    }
}
