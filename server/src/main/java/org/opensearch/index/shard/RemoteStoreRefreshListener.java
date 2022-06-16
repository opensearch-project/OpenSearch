/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.opensearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.text.ParseException;
import java.util.Set;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    public static final String REMOTE_SEGMENTS_METADATA = "remote_segments_metadata";
    private static final int DELETE_PERIOD_IN_MINS = 15;
    private static final Set<String> EXCLUDE_FILES = Set.of("write.lock", REMOTE_SEGMENTS_METADATA);
    private final Directory storeDirectory;
    private final Directory remoteDirectory;
    private final ConcurrentHashMap<String, StoreFileMetadata> segmentsUploadedToRemoteStore;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(Directory storeDirectory, Directory remoteDirectory, ScheduledThreadPoolExecutor executor)
        throws IOException {
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        // ToDo: Handle failures in reading list of files (GitHub #3397)
        this.segmentsUploadedToRemoteStore = readRemoteSegmentsMetadata(remoteDirectory);
        scheduleStaleSegmentDeletion(executor);
    }

    private void scheduleStaleSegmentDeletion(ScheduledThreadPoolExecutor executor) {
        executor.scheduleWithFixedDelay(() -> {
            try {
                Set<String> localFiles = Set.of(storeDirectory.listAll());
                // Upload new segments, if any, before proceeding with deleting stale segments
                uploadNewSegments(localFiles);
                deleteStaleSegments(localFiles);
                uploadRemoteSegmentsMetadata(localFiles);
            } catch (Throwable t) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Exception while deleting stale segments from the remote segment store, will retry again after {} minutes",
                        DELETE_PERIOD_IN_MINS
                    ),
                    t
                );
            }
        }, DELETE_PERIOD_IN_MINS, DELETE_PERIOD_IN_MINS, TimeUnit.MINUTES);
    }

    // Visible for testing
    ConcurrentHashMap<String, StoreFileMetadata> readRemoteSegmentsMetadata(Directory directory) throws IOException {
        ConcurrentHashMap<String, StoreFileMetadata> remoteSegmentsMetadata = new ConcurrentHashMap<>();
        try (IndexInput input = directory.openInput(REMOTE_SEGMENTS_METADATA, IOContext.READ)) {
            StoreFileMetadata storeFileMetadata = readMetadata(input);
            while (storeFileMetadata != null) {
                remoteSegmentsMetadata.put(storeFileMetadata.name(), storeFileMetadata);
                storeFileMetadata = readMetadata(input);
            }
            return remoteSegmentsMetadata;
        } catch (NoSuchFileException e) {
            logger.info(
                () -> new ParameterizedMessage(
                    "Missing file {}, this can happen during the very first refresh of the shard",
                    REMOTE_SEGMENTS_METADATA
                )
            );
            return new ConcurrentHashMap<>();
        }
    }

    private StoreFileMetadata readMetadata(IndexInput in) {
        try {
            String name = in.readString();
            if (name.isEmpty()) {
                return null;
            }
            long length = in.readVLong();
            String checksum = in.readString();
            try {
                Version writtenBy = org.apache.lucene.util.Version.parse(in.readString());
                return new StoreFileMetadata(name, length, checksum, writtenBy);
            } catch (ParseException e) {
                throw new AssertionError(e);
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do Nothing
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     * @param didRefresh true if the refresh opened a new reference
     * @throws IOException in case of I/O error in reading list of local files
     */
    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (didRefresh) {
            try {
                Set<String> localFiles = Set.of(storeDirectory.listAll());
                boolean uploadStatus = uploadNewSegments(localFiles);
                if (uploadStatus) {
                    uploadRemoteSegmentsMetadata(localFiles);
                }
            } catch (IOException e) {
                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
                logger.warn("Exception while uploading new segments to the remote segment store", e);
            }
        }
    }

    // Visible for testing
    synchronized boolean uploadNewSegments(Set<String> localFiles) throws IOException {
        AtomicBoolean newSegmentsUploaded = new AtomicBoolean(false);
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        localFiles.stream().filter(file -> !EXCLUDE_FILES.contains(file)).forEach(file -> {
            try {
                String checksum = getChecksumOfLocalFile(file);
                StoreFileMetadata storeFileMetadata = segmentsUploadedToRemoteStore.get(file);
                if (storeFileMetadata != null && storeFileMetadata.checksum().equals(checksum)) {
                    return;
                }
                newSegmentsUploaded.set(true);
                try {
                    remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                    StoreFileMetadata newFileMetadata = new StoreFileMetadata(
                        file,
                        storeDirectory.fileLength(file),
                        checksum,
                        org.opensearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion
                    );
                    segmentsUploadedToRemoteStore.put(file, newFileMetadata);
                } catch (NoSuchFileException e) {
                    logger.info("The file {} does not exist anymore. It can happen in case of temp files", file);
                } catch (IOException e) {
                    uploadSuccess.set(false);
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file), e);
                }
            } catch (Exception e) {
                logger.info("Exception while uploading segment file {} to remote store", file);
            }
        });

        return newSegmentsUploaded.get() && uploadSuccess.get();
    }

    private String getChecksumOfLocalFile(String file) throws IOException {
        try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.DEFAULT)) {
            return Long.toString(CodecUtil.retrieveChecksum(indexInput));
        }
    }

    /**
     * Deletes segment files from remote store which are not part of local filesystem.
     * @param localFiles set of segment files in local
     * @throws IOException in case of I/O error in reading list of remote files
     */
    // Visible for testing
    synchronized void deleteStaleSegments(Set<String> localFiles) throws IOException {
        Arrays.stream(remoteDirectory.listAll())
            .filter(file -> !localFiles.contains(file))
            .filter(file -> !file.equals(REMOTE_SEGMENTS_METADATA))
            .forEach(file -> {
                try {
                    remoteDirectory.deleteFile(file);
                    segmentsUploadedToRemoteStore.remove(file);
                } catch (IOException e) {
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the remote segment store", file), e);
                }
            });
    }

    // Visible for testing
    synchronized void uploadRemoteSegmentsMetadata(Set<String> localFiles) throws IOException {
        try {
            storeDirectory.deleteFile(REMOTE_SEGMENTS_METADATA);
        } catch (NoSuchFileException e) {
            logger.info(
                () -> new ParameterizedMessage(
                    "Missing file {}, this can happen during the very first refresh of the shard",
                    REMOTE_SEGMENTS_METADATA
                )
            );
        }
        try (IndexOutput indexOutput = storeDirectory.createOutput(REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT)) {
            segmentsUploadedToRemoteStore.forEach((file, metadata) -> {
                try {
                    if (localFiles.contains(file)) {
                        writeMetadata(indexOutput, metadata);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        remoteDirectory.copyFrom(storeDirectory, REMOTE_SEGMENTS_METADATA, REMOTE_SEGMENTS_METADATA, IOContext.DEFAULT);
    }

    private void writeMetadata(IndexOutput indexOutput, StoreFileMetadata storeFileMetadata) throws IOException {
        indexOutput.writeString(storeFileMetadata.name());
        indexOutput.writeVLong(storeFileMetadata.length());
        indexOutput.writeString(storeFileMetadata.checksum());
        indexOutput.writeString(storeFileMetadata.writtenBy().toString());
    }

    // Visible for testing
    Map<String, StoreFileMetadata> getUploadedSegments() {
        return this.segmentsUploadedToRemoteStore;
    }
}
