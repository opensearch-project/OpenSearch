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
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.VersionedCodecStreamWrapper;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.lockmanager.FileLockInfo;
import org.opensearch.index.store.lockmanager.RemoteStoreCommitLevelLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadataHandler;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A RemoteDirectory extension for remote segment store. We need to make sure we don't overwrite a segment file once uploaded.
 * In order to prevent segment overwrite which can occur due to two primary nodes for the same shard at the same time,
 * a unique suffix is added to the uploaded segment file. This class keeps track of filename of segments stored
 * in remote segment store vs filename in local filesystem and provides the consistent Directory interface so that
 * caller will be accessing segment files in the same way as {@code FSDirectory}. Apart from storing actual segment files,
 * remote segment store also keeps track of refresh checkpoints as metadata in a separate path which is handled by
 * another instance of {@code RemoteDirectory}.
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
     * remoteDataDirectory is used to store segment files at path: cluster_UUID/index_UUID/shardId/segments/data
     */
    private final RemoteDirectory remoteDataDirectory;
    /**
     * remoteMetadataDirectory is used to store metadata files at path: cluster_UUID/index_UUID/shardId/segments/metadata
     */
    private final RemoteDirectory remoteMetadataDirectory;

    private final RemoteStoreLockManager mdLockManager;

    private final ThreadPool threadPool;

    /**
     * Keeps track of local segment filename to uploaded filename along with other attributes like checksum.
     * This map acts as a cache layer for uploaded segment filenames which helps avoid calling listAll() each time.
     * It is important to initialize this map on creation of RemoteSegmentStoreDirectory and update it on each upload and delete.
     */
    private Map<String, UploadedSegmentMetadata> segmentsUploadedToRemoteStore;

    private static final VersionedCodecStreamWrapper<RemoteSegmentMetadata> metadataStreamWrapper = new VersionedCodecStreamWrapper<>(
        new RemoteSegmentMetadataHandler(),
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
        super(remoteDataDirectory);
        this.remoteDataDirectory = remoteDataDirectory;
        this.remoteMetadataDirectory = remoteMetadataDirectory;
        this.mdLockManager = mdLockManager;
        this.threadPool = threadPool;
        this.logger = Loggers.getLogger(getClass(), shardId);
        init();
    }

    /**
     * Initializes the cache which keeps track of all the segment files uploaded to the remote segment store.
     * As this cache is specific to an instance of RemoteSegmentStoreDirectory, it is possible that cache becomes stale
     * if another instance of RemoteSegmentStoreDirectory is used to upload/delete segment files.
     * It is caller's responsibility to call init() again to ensure that cache is properly updated.
     *
     * @throws IOException if there were any failures in reading the metadata file
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
     * Initializes the cache to a specific commit which keeps track of all the segment files uploaded to the
     * remote segment store.
     * this is currently used to restore snapshots, where we want to copy segment files from a given commit.
     * TODO: check if we can return read only RemoteSegmentStoreDirectory object from here.
     *
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata initializeToSpecificCommit(long primaryTerm, long commitGeneration, String acquirerId) throws IOException {
        String metadataFilePrefix = MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, commitGeneration);
        String metadataFile = ((RemoteStoreMetadataLockManager) mdLockManager).fetchLock(metadataFilePrefix, acquirerId);
        RemoteSegmentMetadata remoteSegmentMetadata = readMetadataFile(metadataFile);
        if (remoteSegmentMetadata != null) {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(remoteSegmentMetadata.getMetadata());
        } else {
            this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>();
        }
        return remoteSegmentMetadata;
    }

    /**
     * Read the latest metadata file to get the list of segments uploaded to the remote segment store.
     * We upload a metadata file per refresh, but it is not unique per refresh. Refresh metadata file is unique for a given commit.
     * The format of refresh metadata filename is: refresh_metadata__PrimaryTerm__Generation__UUID
     * Refresh metadata files keep track of active segments for the shard at the time of refresh.
     * In order to get the list of segment files uploaded to the remote segment store, we need to read the latest metadata file.
     * Each metadata file contains a map where
     * Key is - Segment local filename and
     * Value is - local filename::uploaded filename::checksum
     *
     * @return Map of segment filename to uploaded filename with checksum
     * @throws IOException if there were any failures in reading the metadata file
     */
    public RemoteSegmentMetadata readLatestMetadataFile() throws IOException {
        RemoteSegmentMetadata remoteSegmentMetadata = null;

        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            METADATA_FILES_TO_FETCH
        );

        RemoteStoreUtils.verifyNoMultipleWriters(metadataFiles, MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen);

        if (metadataFiles.isEmpty() == false) {
            String latestMetadataFile = metadataFiles.get(0);
            logger.trace("Reading latest Metadata file {}", latestMetadataFile);
            remoteSegmentMetadata = readMetadataFile(latestMetadataFile);
        } else {
            logger.trace("No metadata file found, this can happen for new index with no data uploaded to remote segment store");
        }

        return remoteSegmentMetadata;
    }

    private RemoteSegmentMetadata readMetadataFile(String metadataFilename) throws IOException {
        try (InputStream inputStream = remoteMetadataDirectory.getBlobStream(metadataFilename)) {
            byte[] metadataBytes = inputStream.readAllBytes();
            return metadataStreamWrapper.readStream(new ByteArrayIndexInput(metadataFilename, metadataBytes));
        }
    }

    /**
     * Metadata of a segment that is uploaded to remote segment store.
     *
     * @opensearch.api
     */
    @PublicApi(since = "2.3.0")
    public static class UploadedSegmentMetadata {
        // Visible for testing
        static final String SEPARATOR = "::";

        private final String originalFilename;
        private final String uploadedFilename;
        private final String checksum;
        private final long length;

        /**
         * The Lucene major version that wrote the original segment files.
         * As part of the Lucene version compatibility check, this version information stored in the metadata
         * will be used to skip downloading the segment files unnecessarily
         * if they were written by an incompatible Lucene version.
         */
        private int writtenByMajor;

        UploadedSegmentMetadata(String originalFilename, String uploadedFilename, String checksum, long length) {
            this.originalFilename = originalFilename;
            this.uploadedFilename = uploadedFilename;
            this.checksum = checksum;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.join(
                SEPARATOR,
                originalFilename,
                uploadedFilename,
                checksum,
                String.valueOf(length),
                String.valueOf(writtenByMajor)
            );
        }

        public String getChecksum() {
            return this.checksum;
        }

        public long getLength() {
            return this.length;
        }

        public static UploadedSegmentMetadata fromString(String uploadedFilename) {
            String[] values = uploadedFilename.split(SEPARATOR);
            UploadedSegmentMetadata metadata = new UploadedSegmentMetadata(values[0], values[1], values[2], Long.parseLong(values[3]));
            if (values.length < 5) {
                staticLogger.error("Lucene version is missing for UploadedSegmentMetadata: " + uploadedFilename);
            }

            metadata.setWrittenByMajor(Integer.parseInt(values[4]));

            return metadata;
        }

        public String getOriginalFilename() {
            return originalFilename;
        }

        public void setWrittenByMajor(int writtenByMajor) {
            if (writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR) {
                this.writtenByMajor = writtenByMajor;
            } else {
                throw new IllegalArgumentException(
                    "Lucene major version supplied ("
                        + writtenByMajor
                        + ") is incorrect. Should be between Version.LATEST ("
                        + Version.LATEST.major
                        + ") and Version.MIN_SUPPORTED_MAJOR ("
                        + Version.MIN_SUPPORTED_MAJOR
                        + ")."
                );
            }
        }
    }

    /**
     * Contains utility methods that provide various parts of metadata filename along with comparator
     * Each metadata filename is of format: PREFIX__PrimaryTerm__Generation__UUID
     */
    public static class MetadataFilenameUtils {
        public static final String SEPARATOR = "__";
        public static final String METADATA_PREFIX = "metadata";

        static String getMetadataFilePrefixForCommit(long primaryTerm, long generation) {
            return String.join(
                SEPARATOR,
                METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm),
                RemoteStoreUtils.invertLong(generation)
            );
        }

        // Visible for testing
        public static String getMetadataFilename(
            long primaryTerm,
            long generation,
            long translogGeneration,
            long uploadCounter,
            int metadataVersion,
            String nodeId
        ) {
            return String.join(
                SEPARATOR,
                METADATA_PREFIX,
                RemoteStoreUtils.invertLong(primaryTerm),
                RemoteStoreUtils.invertLong(generation),
                RemoteStoreUtils.invertLong(translogGeneration),
                RemoteStoreUtils.invertLong(uploadCounter),
                String.valueOf(Objects.hash(nodeId)),
                RemoteStoreUtils.invertLong(System.currentTimeMillis()),
                String.valueOf(metadataVersion)
            );
        }

        // Visible for testing
        static long getPrimaryTerm(String[] filenameTokens) {
            return RemoteStoreUtils.invertLong(filenameTokens[1]);
        }

        // Visible for testing
        static long getGeneration(String[] filenameTokens) {
            return RemoteStoreUtils.invertLong(filenameTokens[2]);
        }

        public static Tuple<String, String> getNodeIdByPrimaryTermAndGen(String filename) {
            String[] tokens = filename.split(SEPARATOR);
            if (tokens.length < 8) {
                // For versions < 2.11, we don't have node id.
                return null;
            }
            String primaryTermAndGen = String.join(SEPARATOR, tokens[1], tokens[2], tokens[3]);

            String nodeId = tokens[5];
            return new Tuple<>(primaryTermAndGen, nodeId);
        }

    }

    /**
     * Returns list of all the segment files uploaded to remote segment store till the last refresh checkpoint.
     * Any segment file that is uploaded without corresponding metadata file will not be visible as part of listAll().
     * We chose not to return cache entries for listAll as cache can have entries for stale segments as well.
     * Even if we plan to delete stale segments from remote segment store, it will be a periodic operation.
     *
     * @return segment filenames stored in remote segment store
     * @throws IOException if there were any failures in reading the metadata file
     */
    @Override
    public String[] listAll() throws IOException {
        return readLatestMetadataFile().getMetadata().keySet().toArray(new String[0]);
    }

    /**
     * Delete segment file from remote segment store.
     *
     * @param name the name of an existing segment file in local filesystem.
     * @throws IOException if the file exists but could not be deleted.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            remoteDataDirectory.deleteFile(remoteFilename);
            segmentsUploadedToRemoteStore.remove(name);
        }
    }

    /**
     * Returns the byte length of a segment file in the remote segment store.
     *
     * @param name the name of an existing segment file in local filesystem.
     * @throws IOException         in case of I/O error
     * @throws NoSuchFileException if the file does not exist in the cache or remote segment store
     */
    @Override
    public long fileLength(String name) throws IOException {
        if (segmentsUploadedToRemoteStore.containsKey(name)) {
            return segmentsUploadedToRemoteStore.get(name).getLength();
        }
        String remoteFilename = getExistingRemoteFilename(name);
        if (remoteFilename != null) {
            return remoteDataDirectory.fileLength(remoteFilename);
        } else {
            throw new NoSuchFileException(name);
        }
    }

    /**
     * Creates and returns a new instance of {@link RemoteIndexOutput} which will be used to copy files to the remote
     * segment store.
     *
     * @param name the name of the file to create.
     * @throws IOException in case of I/O error
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return remoteDataDirectory.createOutput(getNewRemoteSegmentFilename(name), context);
    }

    /**
     * Opens a stream for reading an existing file and returns {@link RemoteIndexInput} enclosing the stream.
     *
     * @param name the name of an existing file.
     * @throws IOException         in case of I/O error
     * @throws NoSuchFileException if the file does not exist either in cache or remote segment store
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        String remoteFilename = getExistingRemoteFilename(name);
        long fileLength = fileLength(name);
        if (remoteFilename != null) {
            return remoteDataDirectory.openInput(remoteFilename, fileLength, context);
        } else {
            throw new NoSuchFileException(name);
        }
    }

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
    public void copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener) {
        try {
            final String remoteFileName = getNewRemoteSegmentFilename(src);
            boolean uploaded = remoteDataDirectory.copyFrom(from, src, remoteFileName, context, () -> {
                try {
                    postUpload(from, src, remoteFileName, getChecksumOfLocalFile(from, src));
                } catch (IOException e) {
                    throw new RuntimeException("Exception in segment postUpload for file " + src, e);
                }
            }, listener);
            if (uploaded == false) {
                copyFrom(from, src, src, context);
                listener.onResponse(null);
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", src), e);
            listener.onFailure(e);
        }
    }

    /**
     * This acquires a lock on a given commit by creating a lock file in lock directory using {@code FileLockInfo}
     *
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation  Commit Generation
     * @param acquirerId  Lock Acquirer ID which wants to acquire lock on the commit.
     * @throws IOException         will be thrown in case i) listing file failed or ii) Writing the lock file failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public void acquireLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.acquire(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    /**
     * Releases a lock which was acquired on given segment commit.
     *
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation  Commit Generation
     * @param acquirerId  Acquirer ID for which lock needs to be released.
     * @throws IOException         will be thrown in case i) listing lock files failed or ii) deleting the lock file failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public void releaseLock(long primaryTerm, long generation, String acquirerId) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        mdLockManager.release(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).withAcquirerId(acquirerId).build());
    }

    /**
     * Checks if a specific commit have any corresponding lock file.
     *
     * @param primaryTerm Primary Term of index at the time of commit.
     * @param generation  Commit Generation
     * @return True if there is at least one lock for given primary term and generation.
     * @throws IOException         will be thrown in case listing lock files failed.
     * @throws NoSuchFileException when metadata file is not present for given commit point.
     */
    @Override
    public Boolean isLockAcquired(long primaryTerm, long generation) throws IOException {
        String metadataFile = getMetadataFileForCommit(primaryTerm, generation);
        return isLockAcquired(metadataFile);
    }

    // Visible for testing
    Boolean isLockAcquired(String metadataFile) throws IOException {
        return mdLockManager.isAcquired(FileLockInfo.getLockInfoBuilder().withFileToLock(metadataFile).build());
    }

    // Visible for testing
    String getMetadataFileForCommit(long primaryTerm, long generation) throws IOException {
        List<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.getMetadataFilePrefixForCommit(primaryTerm, generation),
            1
        );

        if (metadataFiles.isEmpty()) {
            throw new NoSuchFileException(
                "Metadata file is not present for given primary term " + primaryTerm + " and generation " + generation
            );
        }
        if (metadataFiles.size() != 1) {
            throw new IllegalStateException(
                "there should be only one metadata file for given primary term "
                    + primaryTerm
                    + "and generation "
                    + generation
                    + " but found "
                    + metadataFiles.size()
            );
        }
        return metadataFiles.get(0);
    }

    private void postUpload(Directory from, String src, String remoteFilename, String checksum) throws IOException {
        UploadedSegmentMetadata segmentMetadata = new UploadedSegmentMetadata(src, remoteFilename, checksum, from.fileLength(src));
        segmentsUploadedToRemoteStore.put(src, segmentMetadata);
    }

    /**
     * Copies an existing src file from directory from to a non-existent file dest in this directory.
     * Once the segment is uploaded to remote segment store, update the cache accordingly.
     */
    @Override
    public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
        String remoteFilename = getNewRemoteSegmentFilename(dest);
        remoteDataDirectory.copyFrom(from, src, remoteFilename, context);
        postUpload(from, src, remoteFilename, getChecksumOfLocalFile(from, src));
    }

    /**
     * Checks if the file exists in the uploadedSegments cache and the checksum matches.
     * It is important to match the checksum as the same segment filename can be used for different
     * segments due to a concurrency issue.
     *
     * @param localFilename filename of segment stored in local filesystem
     * @param checksum      checksum of the segment file
     * @return true if file exists in cache and checksum matches.
     */
    public boolean containsFile(String localFilename, String checksum) {
        return segmentsUploadedToRemoteStore.containsKey(localFilename)
            && segmentsUploadedToRemoteStore.get(localFilename).checksum.equals(checksum);
    }

    /**
     * Upload metadata file
     *
     * @param segmentFiles         segment files that are part of the shard at the time of the latest refresh
     * @param segmentInfosSnapshot SegmentInfos bytes to store as part of metadata file
     * @param storeDirectory instance of local directory to temporarily create metadata file before upload
     * @param translogGeneration translog generation
     * @param replicationCheckpoint ReplicationCheckpoint of primary shard
     * @param nodeId node id
     * @throws IOException in case of I/O error while uploading the metadata file
     */
    public void uploadMetadata(
        Collection<String> segmentFiles,
        SegmentInfos segmentInfosSnapshot,
        Directory storeDirectory,
        long translogGeneration,
        ReplicationCheckpoint replicationCheckpoint,
        String nodeId
    ) throws IOException {
        synchronized (this) {
            String metadataFilename = MetadataFilenameUtils.getMetadataFilename(
                replicationCheckpoint.getPrimaryTerm(),
                segmentInfosSnapshot.getGeneration(),
                translogGeneration,
                metadataUploadCounter.incrementAndGet(),
                RemoteSegmentMetadata.CURRENT_VERSION,
                nodeId
            );
            try {
                try (IndexOutput indexOutput = storeDirectory.createOutput(metadataFilename, IOContext.DEFAULT)) {
                    Map<String, Integer> segmentToLuceneVersion = getSegmentToLuceneVersion(segmentFiles, segmentInfosSnapshot);
                    Map<String, String> uploadedSegments = new HashMap<>();
                    for (String file : segmentFiles) {
                        if (segmentsUploadedToRemoteStore.containsKey(file)) {
                            UploadedSegmentMetadata metadata = segmentsUploadedToRemoteStore.get(file);
                            metadata.setWrittenByMajor(segmentToLuceneVersion.get(metadata.originalFilename));
                            uploadedSegments.put(file, metadata.toString());
                        } else {
                            throw new NoSuchFileException(file);
                        }
                    }

                    ByteBuffersDataOutput byteBuffersIndexOutput = new ByteBuffersDataOutput();
                    segmentInfosSnapshot.write(
                        new ByteBuffersIndexOutput(byteBuffersIndexOutput, "Snapshot of SegmentInfos", "SegmentInfos")
                    );
                    byte[] segmentInfoSnapshotByteArray = byteBuffersIndexOutput.toArrayCopy();

                    metadataStreamWrapper.writeStream(
                        indexOutput,
                        new RemoteSegmentMetadata(
                            RemoteSegmentMetadata.fromMapOfStrings(uploadedSegments),
                            segmentInfoSnapshotByteArray,
                            replicationCheckpoint
                        )
                    );
                }
                storeDirectory.sync(Collections.singleton(metadataFilename));
                remoteMetadataDirectory.copyFrom(storeDirectory, metadataFilename, metadataFilename, IOContext.DEFAULT);
            } finally {
                tryAndDeleteLocalFile(metadataFilename, storeDirectory);
            }
        }
    }

    /**
     * Parses the provided SegmentInfos to retrieve a mapping of the provided segment files to
     * the respective Lucene major version that wrote the segments
     *
     * @param segmentFiles         List of segment files for which the Lucene major version is needed
     * @param segmentInfosSnapshot SegmentInfos instance to parse
     * @return Map of the segment file to its Lucene major version
     */
    private Map<String, Integer> getSegmentToLuceneVersion(Collection<String> segmentFiles, SegmentInfos segmentInfosSnapshot) {
        Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
        for (SegmentCommitInfo segmentCommitInfo : segmentInfosSnapshot) {
            SegmentInfo info = segmentCommitInfo.info;
            Set<String> segFiles = info.files();
            for (String file : segFiles) {
                segmentToLuceneVersion.put(file, info.getVersion().major);
            }
        }

        for (String file : segmentFiles) {
            if (segmentToLuceneVersion.containsKey(file) == false) {
                if (file.equals(segmentInfosSnapshot.getSegmentsFileName())) {
                    segmentToLuceneVersion.put(file, segmentInfosSnapshot.getCommitLuceneVersion().major);
                } else {
                    // Fallback to the Lucene major version of the respective segment's .si file
                    String segmentInfoFileName = RemoteStoreUtils.getSegmentName(file) + ".si";
                    segmentToLuceneVersion.put(file, segmentToLuceneVersion.get(segmentInfoFileName));
                }
            }
        }

        return segmentToLuceneVersion;
    }

    /**
     * Try to delete file from local store. Fails silently on failures
     *
     * @param filename: name of the file to be deleted
     */
    private void tryAndDeleteLocalFile(String filename, Directory directory) {
        try {
            logger.debug("Deleting file: " + filename);
            directory.deleteFile(filename);
        } catch (NoSuchFileException | FileNotFoundException e) {
            logger.trace("Exception while deleting. Missing file : " + filename, e);
        } catch (IOException e) {
            logger.warn("Exception while deleting: " + filename, e);
        }
    }

    private String getChecksumOfLocalFile(Directory directory, String file) throws IOException {
        try (IndexInput indexInput = directory.openInput(file, IOContext.DEFAULT)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    private String getExistingRemoteFilename(String localFilename) {
        if (segmentsUploadedToRemoteStore.containsKey(localFilename)) {
            return segmentsUploadedToRemoteStore.get(localFilename).uploadedFilename;
        } else {
            return null;
        }
    }

    private String getNewRemoteSegmentFilename(String localFilename) {
        return localFilename + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
    }

    private String getLocalSegmentFilename(String remoteFilename) {
        return remoteFilename.split(SEGMENT_NAME_UUID_SEPARATOR)[0];
    }

    // Visible for testing
    public Map<String, UploadedSegmentMetadata> getSegmentsUploadedToRemoteStore() {
        return Collections.unmodifiableMap(this.segmentsUploadedToRemoteStore);
    }

    /**
     * Delete stale segment and metadata files
     * One metadata file is kept per commit (refresh updates the same file). To read segments uploaded to remote store,
     * we just need to read the latest metadata file. All the stale metadata files can be safely deleted.
     *
     * @param lastNMetadataFilesToKeep number of metadata files to keep
     * @throws IOException in case of I/O error while reading from / writing to remote segment store
     */
    public void deleteStaleSegments(int lastNMetadataFilesToKeep) throws IOException {
        List<String> sortedMetadataFileList = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            Integer.MAX_VALUE
        );
        if (sortedMetadataFileList.size() <= lastNMetadataFilesToKeep) {
            logger.debug(
                "Number of commits in remote segment store={}, lastNMetadataFilesToKeep={}",
                sortedMetadataFileList.size(),
                lastNMetadataFilesToKeep
            );
            return;
        }

        List<String> metadataFilesEligibleToDelete = sortedMetadataFileList.subList(
            lastNMetadataFilesToKeep,
            sortedMetadataFileList.size()
        );
        List<String> metadataFilesToBeDeleted = metadataFilesEligibleToDelete.stream().filter(metadataFile -> {
            try {
                return !isLockAcquired(metadataFile);
            } catch (IOException e) {
                logger.error(
                    "skipping metadata file ("
                        + metadataFile
                        + ") deletion for this run,"
                        + " as checking lock for metadata is failing with error: "
                        + e
                );
                return false;
            }
        }).collect(Collectors.toList());

        sortedMetadataFileList.removeAll(metadataFilesToBeDeleted);
        logger.debug(
            "metadataFilesEligibleToDelete={} metadataFilesToBeDeleted={}",
            metadataFilesEligibleToDelete,
            metadataFilesEligibleToDelete
        );

        Map<String, UploadedSegmentMetadata> activeSegmentFilesMetadataMap = new HashMap<>();
        Set<String> activeSegmentRemoteFilenames = new HashSet<>();
        for (String metadataFile : sortedMetadataFileList) {
            Map<String, UploadedSegmentMetadata> segmentMetadataMap = readMetadataFile(metadataFile).getMetadata();
            activeSegmentFilesMetadataMap.putAll(segmentMetadataMap);
            activeSegmentRemoteFilenames.addAll(
                segmentMetadataMap.values().stream().map(metadata -> metadata.uploadedFilename).collect(Collectors.toSet())
            );
        }
        for (String metadataFile : metadataFilesToBeDeleted) {
            Map<String, UploadedSegmentMetadata> staleSegmentFilesMetadataMap = readMetadataFile(metadataFile).getMetadata();
            Set<String> staleSegmentRemoteFilenames = staleSegmentFilesMetadataMap.values()
                .stream()
                .map(metadata -> metadata.uploadedFilename)
                .collect(Collectors.toSet());
            AtomicBoolean deletionSuccessful = new AtomicBoolean(true);
            List<String> nonActiveDeletedSegmentFiles = new ArrayList<>();
            staleSegmentRemoteFilenames.stream().filter(file -> !activeSegmentRemoteFilenames.contains(file)).forEach(file -> {
                try {
                    remoteDataDirectory.deleteFile(file);
                    nonActiveDeletedSegmentFiles.add(file);
                    if (!activeSegmentFilesMetadataMap.containsKey(getLocalSegmentFilename(file))) {
                        segmentsUploadedToRemoteStore.remove(getLocalSegmentFilename(file));
                    }
                } catch (NoSuchFileException e) {
                    logger.info("Segment file {} corresponding to metadata file {} does not exist in remote", file, metadataFile);
                } catch (IOException e) {
                    deletionSuccessful.set(false);
                    logger.info(
                        "Exception while deleting segment file {} corresponding to metadata file {}. Deletion will be re-tried",
                        file,
                        metadataFile
                    );
                }
            });
            logger.debug("nonActiveDeletedSegmentFiles={}", nonActiveDeletedSegmentFiles);
            if (deletionSuccessful.get()) {
                logger.debug("Deleting stale metadata file {} from remote segment store", metadataFile);
                remoteMetadataDirectory.deleteFile(metadataFile);
            }
        }
    }

    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep) {
        deleteStaleSegmentsAsync(lastNMetadataFilesToKeep, ActionListener.wrap(r -> {}, e -> {}));
    }

    /**
     * Delete stale segment and metadata files asynchronously.
     * This method calls {@link RemoteSegmentStoreDirectory#deleteStaleSegments(int)} in an async manner.
     *
     * @param lastNMetadataFilesToKeep number of metadata files to keep
     */
    public void deleteStaleSegmentsAsync(int lastNMetadataFilesToKeep, ActionListener<Void> listener) {
        if (canDeleteStaleCommits.compareAndSet(true, false)) {
            try {
                threadPool.executor(ThreadPool.Names.REMOTE_PURGE).execute(() -> {
                    try {
                        deleteStaleSegments(lastNMetadataFilesToKeep);
                        listener.onResponse(null);
                    } catch (Exception e) {
                        logger.error(
                            "Exception while deleting stale commits from remote segment store, will retry delete post next commit",
                            e
                        );
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

    /*
    Tries to delete shard level directory if it is empty
    Return true if it deleted it successfully
     */
    private boolean deleteIfEmpty() throws IOException {
        Collection<String> metadataFiles = remoteMetadataDirectory.listFilesByPrefixInLexicographicOrder(
            MetadataFilenameUtils.METADATA_PREFIX,
            1
        );
        if (metadataFiles.size() != 0) {
            logger.info("Remote directory still has files, not deleting the path");
            return false;
        }

        try {
            remoteDataDirectory.delete();
            remoteMetadataDirectory.delete();
            mdLockManager.delete();
        } catch (Exception e) {
            logger.error("Exception occurred while deleting directory", e);
            return false;
        }

        return true;
    }

    @Override
    public void close() throws IOException {
        deleteStaleSegmentsAsync(0, ActionListener.wrap(r -> deleteIfEmpty(), e -> logger.error("Failed to cleanup remote directory")));
    }
}
