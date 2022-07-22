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
import org.apache.lucene.store.IOContext;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreRefreshListener implements ReferenceManager.RefreshListener {

    private static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private boolean isPrimary;
    private static final Logger logger = LogManager.getLogger(RemoteStoreRefreshListener.class);

    public RemoteStoreRefreshListener(IndexShard indexShard) throws IOException {
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate()).getDelegate();
        this.isPrimary = indexShard.shardRouting.primary();
        if(indexShard.shardRouting.primary()) {
            this.remoteDirectory.init();
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
        synchronized (this) {
            if (indexShard.shardRouting.primary()) {
                if (!isPrimary) {
                    isPrimary = true;
                    this.remoteDirectory.init();
                }
                try {
                    String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
                    if (!remoteDirectory.containsFile(lastCommittedLocalSegmentFileName)) {
                        SegmentInfos commitSegmentInfos = SegmentInfos.readCommit(storeDirectory, lastCommittedLocalSegmentFileName);
                        Collection<String> committedLocalFiles = commitSegmentInfos.files(true);
                        boolean uploadStatus = uploadNewSegments(committedLocalFiles);
                        if (uploadStatus) {
                            remoteDirectory.copyFrom(storeDirectory, lastCommittedLocalSegmentFileName, lastCommittedLocalSegmentFileName, IOContext.DEFAULT);
                            remoteDirectory.uploadCommitMapping(committedLocalFiles, storeDirectory, indexShard.getOperationPrimaryTerm(), commitSegmentInfos.getGeneration());
                        }
                    } else {
                        logger.info("Latest commit point {} is present in remote store", lastCommittedLocalSegmentFileName);
                    }
                    try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                        SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
                        Collection<String> refreshedLocalFiles = segmentInfos.files(true);
                        boolean uploadStatus = uploadNewSegments(refreshedLocalFiles);
                        if (uploadStatus) {
                            remoteDirectory.uploadRefreshMapping(refreshedLocalFiles, storeDirectory, indexShard.getOperationPrimaryTerm(), segmentInfos.getGeneration());
                        }
                    } catch (EngineException e) {
                        logger.warn("Exception while reading SegmentInfosSnapshot", e);
                    }
                } catch (IOException e) {
                    // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                    // in the next refresh. This should not affect durability of the indexed data after remote trans-log integration.
                    logger.warn("Exception while uploading new segments to the remote segment store", e);
                }
            }
        }
    }

    // Visible for testing
    synchronized boolean uploadNewSegments(Collection<String> localFiles) throws IOException {
        AtomicBoolean uploadSuccess = new AtomicBoolean(true);
        localFiles.stream()
            .filter(file -> !EXCLUDE_FILES.contains(file))
            .filter(file -> !remoteDirectory.containsFile(file))
            .forEach(file -> {
                try {
                    remoteDirectory.copyFrom(storeDirectory, file, file, IOContext.DEFAULT);
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
}
