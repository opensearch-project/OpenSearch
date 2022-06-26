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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.ChecksumIndexInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    public static final String REFRESHED_SEGMENTINFOS_FILENAME = "refreshed_segments_";
    private static final int DELETE_PERIOD_IN_MINS = 15;
    private static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final Directory remoteDirectory;
    private final ConcurrentHashMap<String, StoreFileMetadata> segmentsUploadedToRemoteStore;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(IndexShard indexShard, Directory storeDirectory, Directory remoteDirectory)
        throws IOException {
        this.indexShard = indexShard;
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        // ToDo: Handle failures in reading list of files (GitHub #3397)
        this.segmentsUploadedToRemoteStore = readRemoteSegmentsMetadata(storeDirectory, remoteDirectory);
        scheduleStaleSegmentDeletion(indexShard.getThreadPool());
    }

    // Visible for testing
    ConcurrentHashMap<String, StoreFileMetadata> readRemoteSegmentsMetadata(Directory storeDirectory, Directory remoteDirectory) throws IOException {
        ConcurrentHashMap<String, StoreFileMetadata> remoteSegmentsMetadata = new ConcurrentHashMap<>();
        for(String segmentFile: remoteDirectory.listAll()) {
            try {
                String checksum = getChecksumOfLocalFile(segmentFile);
                StoreFileMetadata segmentFileMetadata = new StoreFileMetadata(
                    segmentFile,
                    storeDirectory.fileLength(segmentFile),
                    checksum,
                    org.opensearch.Version.CURRENT.minimumIndexCompatibilityVersion().luceneVersion
                );
                remoteSegmentsMetadata.put(segmentFile, segmentFileMetadata);
            } catch (IOException e) {
                logger.info("Error reading checksum of segment file {}. This can happen if segmentFile does not exist anymore in local", segmentFile);
            }
        }
        return remoteSegmentsMetadata;
    }

    private void scheduleStaleSegmentDeletion(ThreadPool threadPool) {
        threadPool.scheduleWithFixedDelay(() -> {
            try {
                deleteStaleSegments();
            } catch (Throwable t) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Exception while deleting stale segments from the remote segment store, will retry again after {} minutes",
                        DELETE_PERIOD_IN_MINS
                    ),
                    t
                );
            }
        }, TimeValue.timeValueMinutes(DELETE_PERIOD_IN_MINS), ThreadPool.Names.REMOTE_STORE);
    }

    /**
     * Deletes segment files from remote store which are not part of local filesystem.
     * @throws IOException in case of I/O error in reading list of remote files
     */
    // Visible for testing
    synchronized void deleteStaleSegments() throws IOException {
        Collection<String> refreshedSegmentFiles;
        Set<String> remoteSegments = Set.of(remoteDirectory.listAll());
        Set<String> allRefreshedSegmentInfos = remoteSegments.stream()
            .filter(file -> file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .collect(Collectors.toSet());
        Optional<Integer> lastGeneration = allRefreshedSegmentInfos.stream()
            .map(file -> Integer.parseInt(file.substring(REFRESHED_SEGMENTINFOS_FILENAME.length()), Character.MAX_RADIX))
            .max(Comparator.naturalOrder());
        if(lastGeneration.isEmpty()) {
            logger.info("{}_N is yet to be uploaded to the remote store", REFRESHED_SEGMENTINFOS_FILENAME);
            return;
        }
        String lastRefreshedSegmentInfos = REFRESHED_SEGMENTINFOS_FILENAME + lastGeneration.get();
        try (ChecksumIndexInput input = remoteDirectory.openChecksumInput(lastRefreshedSegmentInfos, IOContext.READ)) {
            try {
                SegmentInfos segmentInfos = SegmentInfos.readCommit(remoteDirectory, input, lastGeneration.get());
                refreshedSegmentFiles = segmentInfos.files(true);
            } catch (EOFException | NoSuchFileException | FileNotFoundException e) {
                throw new CorruptIndexException(
                    "Unexpected file read error while reading index.", input, e);
            }
        }

        Arrays.stream(remoteDirectory.listAll())
            .filter(file -> !refreshedSegmentFiles.contains(file))
            .filter(file -> !allRefreshedSegmentInfos.contains(file))
            .forEach(file -> {
                try {
                    remoteDirectory.deleteFile(file);
                    segmentsUploadedToRemoteStore.remove(file);
                } catch (IOException e) {
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the remote segment store", file), e);
                }
            });

        allRefreshedSegmentInfos.stream().filter(file -> !file.equals(lastRefreshedSegmentInfos)).forEach(file -> {
            try {
                storeDirectory.deleteFile(file);
                remoteDirectory.deleteFile(file);
            } catch (IOException e) {
                logger.info("Exception while deleting stalle {}", file);
            }
        });
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
                SegmentInfos segmentInfos = indexShard.getSegmentInfosSnapshot().get();
                Set<String> localFiles = (Set<String>) segmentInfos.files(true);
                boolean uploadStatus = uploadNewSegments(localFiles);
                if (uploadStatus) {
                    uploadRemoteSegmentsMetadata(segmentInfos);
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
        localFiles.stream().filter(file -> !EXCLUDE_FILES.contains(file)).filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME)).forEach(file -> {
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

    // Visible for testing
    synchronized void uploadRemoteSegmentsMetadata(SegmentInfos segmentInfos) throws IOException {
        for(String segmentFile: segmentInfos.files(true)) {
            if(!segmentsUploadedToRemoteStore.containsKey(segmentFile)) {
                logger.info("Skipping uploading refreshed_segments_N file as not all the corresponding segment files are uploaded");
                return;
            }
        }
        String segmentInfosFileName = REFRESHED_SEGMENTINFOS_FILENAME + segmentInfos.getGeneration();
        try {
            storeDirectory.deleteFile(segmentInfosFileName);
        } catch(NoSuchFileException e) {
            logger.info("File {} is missing in local filesystem. This can happen for a very first refresh of the shard", segmentInfosFileName);
        }
        IndexOutput indexOutput = storeDirectory.createOutput(segmentInfosFileName, IOContext.DEFAULT);
        segmentInfos.write(indexOutput);
        indexOutput.close();
        storeDirectory.sync(Collections.singleton(segmentInfosFileName));
        remoteDirectory.copyFrom(storeDirectory, segmentInfosFileName, segmentInfosFileName, IOContext.DEFAULT);
    }

    // Visible for testing
    Map<String, StoreFileMetadata> getUploadedSegments() {
        return this.segmentsUploadedToRemoteStore;
    }
}
