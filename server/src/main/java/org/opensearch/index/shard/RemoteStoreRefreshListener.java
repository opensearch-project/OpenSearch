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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.EngineException;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    public static final String COMMITTED_SEGMENTINFOS_FILENAME = "segments_";
    public static final String REFRESHED_SEGMENTINFOS_FILENAME = "refreshed_segments_";
    private static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final Directory remoteDirectory;
    private final ConcurrentHashMap<String, String> segmentsUploadedToRemoteStore;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(IndexShard indexShard) throws IOException {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate()).getDelegate();
        // ToDo: Handle failures in reading list of files (GitHub #3397)
        this.segmentsUploadedToRemoteStore = new ConcurrentHashMap<>(Arrays.stream(remoteDirectory.listAll()).collect(Collectors.toMap(Function.identity(), Function.identity())));
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
        try {
            Set<String> localFiles = new HashSet<>();
            String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
            if(!segmentsUploadedToRemoteStore.containsKey(lastCommittedLocalSegmentFileName)) {
                Collection<String> committedLocalFiles = SegmentInfos.readCommit(storeDirectory, lastCommittedLocalSegmentFileName).files(true);
                localFiles.addAll(committedLocalFiles);
                boolean uploadStatus = uploadNewSegments(committedLocalFiles);
                if(uploadStatus) {
                    remoteDirectory.copyFrom(storeDirectory, lastCommittedLocalSegmentFileName, lastCommittedLocalSegmentFileName, IOContext.DEFAULT);
                    segmentsUploadedToRemoteStore.put(lastCommittedLocalSegmentFileName, lastCommittedLocalSegmentFileName);
                }
            } else {
                logger.info("Latest commit point {} is present in remote store", lastCommittedLocalSegmentFileName);
            }
            try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
                Collection<String> refreshedLocalFiles = segmentInfos.files(true);
                localFiles.addAll(refreshedLocalFiles);
                boolean uploadStatus = uploadNewSegments(refreshedLocalFiles);
                if (uploadStatus) {
                    uploadRemoteSegmentsMetadata(segmentInfos);
                }
            } catch (EngineException e) {
                logger.warn("Exception while reading SegmentInfosSnapshot", e);
            }
            deleteStaleSegments(Set.of(storeDirectory.listAll()));
        } catch (IOException e) {
            // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
            // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
            logger.warn("Exception while uploading new segments to the remote segment store", e);
        }
    }

    // Visible for testing
    synchronized boolean uploadNewSegments(Collection<String> localFiles) throws IOException {
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        localFiles.stream()
            .filter(file -> !EXCLUDE_FILES.contains(file))
            .filter(file -> !file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .filter(file -> !file.startsWith(COMMITTED_SEGMENTINFOS_FILENAME))
            .filter(file -> !segmentsUploadedToRemoteStore.containsKey(file))
            .forEach(file -> {
                try {
                    remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
                    segmentsUploadedToRemoteStore.put(file, file);
                } catch (NoSuchFileException e) {
                    logger.info("The file {} does not exist anymore. It can happen in case of temp files", file);
                } catch (IOException e) {
                    uploadSuccess.set(false);
                    // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                    logger.warn(
                        () -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file),
                        e
                    );
                }
            });

        return uploadSuccess.get();
    }

    // Visible for testing
    synchronized void uploadRemoteSegmentsMetadata(SegmentInfos segmentInfos) throws IOException {
        String segmentInfosFileName = REFRESHED_SEGMENTINFOS_FILENAME + Long.toString(segmentInfos.getGeneration(), Character.MAX_RADIX);
        try {
            storeDirectory.deleteFile(segmentInfosFileName);
        } catch (NoSuchFileException e) {
            logger.info(
                "File {} is missing in local filesystem. This can happen for the first refresh of the generation",
                segmentInfosFileName
            );
        }
        IndexOutput indexOutput = storeDirectory.createOutput(segmentInfosFileName, IOContext.DEFAULT);
        segmentInfos.write(indexOutput);
        indexOutput.close();
        storeDirectory.sync(Collections.singleton(segmentInfosFileName));
        remoteDirectory.copyFrom(storeDirectory, segmentInfosFileName, segmentInfosFileName, IOContext.DEFAULT);
        segmentsUploadedToRemoteStore.put(segmentInfosFileName, segmentInfosFileName);
        Set<String> staleSegmentInfosFiles = segmentsUploadedToRemoteStore.keySet().stream()
            .filter(file -> file.startsWith(REFRESHED_SEGMENTINFOS_FILENAME))
            .filter(file -> !file.equals(segmentInfosFileName))
            .collect(Collectors.toSet());
        staleSegmentInfosFiles.forEach(file -> {
            try {
                storeDirectory.deleteFile(file);
            } catch(NoSuchFileException e) {
                segmentsUploadedToRemoteStore.remove(file);
                logger.warn(() -> new ParameterizedMessage("Delete failed as file {} does not exist in local store", file), e);
            } catch (IOException e) {
                logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the local store", file), e);
            }
        });
    }

    synchronized void deleteStaleSegments(Set<String> localFiles) {
        Set<String> remoteStaleSegments = segmentsUploadedToRemoteStore.keySet().stream()
            .filter(file -> !localFiles.contains(file))
            .collect(Collectors.toSet());
        remoteStaleSegments.forEach(file -> {
            try {
                remoteDirectory.deleteFile(file);
                segmentsUploadedToRemoteStore.remove(file);
            } catch(IOException e) {
                logger.warn(() -> new ParameterizedMessage("Exception while deleting file {} from the remote segment store", file), e);
            }
        });
    }

    // Visible for testing
    Map<String, String> getUploadedSegments() {
        return this.segmentsUploadedToRemoteStore;
    }
}
