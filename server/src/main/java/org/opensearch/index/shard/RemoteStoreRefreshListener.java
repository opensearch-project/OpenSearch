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
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public final class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {
    // Visible for testing
    static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    // Visible for testing
    static final int LAST_N_METADATA_FILES_TO_KEEP = 10;
    public static final String SEGMENT_INFO_SNAPSHOT_FILENAME = "segment_infos_snapshot_filename";

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final Map<String, String> localSegmentChecksumMap;
    private long primaryTerm;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(IndexShard indexShard) {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        this.primaryTerm = indexShard.getOperationPrimaryTerm();
        localSegmentChecksumMap = new HashMap<>();
        if (indexShard.shardRouting.primary()) {
            try {
                this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
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
     */
    @Override
    public void afterRefresh(boolean didRefresh) {
        synchronized (this) {
            try {
                if (indexShard.shardRouting.primary()) {
                    if (this.primaryTerm != indexShard.getOperationPrimaryTerm()) {
                        this.primaryTerm = indexShard.getOperationPrimaryTerm();
                        this.remoteDirectory.init();
                    }
                    try {
                        String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
                        if (lastCommittedLocalSegmentFileName != null
                            && !remoteDirectory.containsFile(
                                lastCommittedLocalSegmentFileName,
                                getChecksumOfLocalFile(lastCommittedLocalSegmentFileName)
                            )) {
                            deleteStaleCommits();
                        }
                        String segment_info_snapshot_filename = null;
                        try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                            SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();

                            Collection<String> refreshedLocalFiles = segmentInfos.files(true);

                            List<String> segmentInfosFiles = refreshedLocalFiles.stream()
                                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS))
                                .collect(Collectors.toList());
                            Optional<String> latestSegmentInfos = segmentInfosFiles.stream()
                                .max(Comparator.comparingLong(IndexFileNames::parseGeneration));

                            if (latestSegmentInfos.isPresent()) {
                                Set<String> segmentFilesFromSnapshot = new HashSet<>(refreshedLocalFiles);
                                refreshedLocalFiles.addAll(SegmentInfos.readCommit(storeDirectory, latestSegmentInfos.get()).files(true));
                                segmentInfosFiles.stream()
                                    .filter(file -> !file.equals(latestSegmentInfos.get()))
                                    .forEach(refreshedLocalFiles::remove);

                                boolean uploadStatus = uploadNewSegments(refreshedLocalFiles);
                                if (uploadStatus) {
                                    if (segmentFilesFromSnapshot.equals(new HashSet<>(refreshedLocalFiles))) {
                                        segment_info_snapshot_filename = uploadSegmentInfosSnapshot(latestSegmentInfos.get(), segmentInfos);
                                        refreshedLocalFiles.add(segment_info_snapshot_filename);
                                    }

                                    remoteDirectory.uploadMetadata(
                                        refreshedLocalFiles,
                                        storeDirectory,
                                        indexShard.getOperationPrimaryTerm(),
                                        segmentInfos.getGeneration()
                                    );
                                    localSegmentChecksumMap.keySet()
                                        .stream()
                                        .filter(file -> !refreshedLocalFiles.contains(file))
                                        .collect(Collectors.toSet())
                                        .forEach(localSegmentChecksumMap::remove);
                                }
                            }
                        } catch (EngineException e) {
                            logger.warn("Exception while reading SegmentInfosSnapshot", e);
                        } finally {
                            try {
                                if (segment_info_snapshot_filename != null) {
                                    storeDirectory.deleteFile(segment_info_snapshot_filename);
                                }
                            } catch (IOException e) {
                                logger.warn("Exception while deleting: " + segment_info_snapshot_filename, e);
                            }
                        }
                    } catch (IOException e) {
                        // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                        // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
                        logger.warn("Exception while uploading new segments to the remote segment store", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
            }
        }
    }

    String uploadSegmentInfosSnapshot(String latestSegmentsNFilename, SegmentInfos segmentInfosSnapshot) throws IOException {
        long localCheckpoint = indexShard.getEngine().getProcessedLocalCheckpoint();
        String commitGeneration = latestSegmentsNFilename.substring((IndexFileNames.SEGMENTS + "_").length());
        String segment_info_snapshot_filename = SEGMENT_INFO_SNAPSHOT_FILENAME + "__" + commitGeneration + "__" + localCheckpoint;
        IndexOutput indexOutput = storeDirectory.createOutput(segment_info_snapshot_filename, IOContext.DEFAULT);
        segmentInfosSnapshot.write(indexOutput);
        indexOutput.close();
        storeDirectory.sync(Collections.singleton(segment_info_snapshot_filename));
        remoteDirectory.copyFrom(storeDirectory, segment_info_snapshot_filename, segment_info_snapshot_filename, IOContext.DEFAULT, true);
        return segment_info_snapshot_filename;
    }

    // Visible for testing
    boolean uploadNewSegments(Collection<String> localFiles) throws IOException {
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        localFiles.stream().filter(file -> !EXCLUDE_FILES.contains(file)).filter(file -> {
            try {
                return !remoteDirectory.containsFile(file, getChecksumOfLocalFile(file));
            } catch (IOException e) {
                logger.info(
                    "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                    file
                );
                return true;
            }
        }).forEach(file -> {
            try {
                remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
            } catch (IOException e) {
                uploadSuccess.set(false);
                // ToDO: Handle transient and permanent un-availability of the remote store (GitHub #3397)
                logger.warn(() -> new ParameterizedMessage("Exception while uploading file {} to the remote segment store", file), e);
            }
        });
        return uploadSuccess.get();
    }

    private String getChecksumOfLocalFile(String file) throws IOException {
        if (!localSegmentChecksumMap.containsKey(file)) {
            try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.DEFAULT)) {
                String checksum = Long.toString(CodecUtil.retrieveChecksum(indexInput));
                localSegmentChecksumMap.put(file, checksum);
            }
        }
        return localSegmentChecksumMap.get(file);
    }

    private void deleteStaleCommits() {
        try {
            remoteDirectory.deleteStaleSegments(LAST_N_METADATA_FILES_TO_KEEP);
        } catch (IOException e) {
            logger.info("Exception while deleting stale commits from remote segment store, will retry delete post next commit", e);
        }
    }
}
