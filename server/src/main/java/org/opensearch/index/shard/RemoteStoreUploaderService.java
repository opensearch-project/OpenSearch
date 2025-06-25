/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The service essentially acts as a bridge between local segment storage and remote storage,
 * ensuring efficient and reliable segment synchronization while providing comprehensive monitoring and error handling.
 */
public class RemoteStoreUploaderService implements RemoteStoreUploader {

    private final Logger logger;

    public static final Set<String> EXCLUDE_FILES = Set.of("write.lock");

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final Map<String, String> localSegmentChecksumMap;
    // todo: check if we need to create a separate segment tracker as it is said to be linked to RL
    private final RemoteSegmentTransferTracker segmentTracker;

    public RemoteStoreUploaderService(
        IndexShard indexShard,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteSegmentTransferTracker segmentTracker
    ) {
        this.indexShard = indexShard;
        // todo: check the prefix to be add for this class
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        this.segmentTracker = segmentTracker;
        this.localSegmentChecksumMap = new HashMap<>();
    }

    @Override
    public void syncAndUploadNewSegments(
        Collection<String> localSegments,
        Map<String, Long> localSegmentsSizeMap,
        ActionListener<Void> listener
    ) {

        Collection<String> filteredFiles = localSegments.stream().filter(file -> !skipUpload(file)).toList();
        if (filteredFiles.isEmpty()) {
            logger.debug("No new segments to upload in uploadNewSegments");
            listener.onResponse(null);
            return;
        }

        logger.debug("Effective new segments files to upload {}", filteredFiles);
        ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, filteredFiles.size());
        Directory directory = ((FilterDirectory) (((FilterDirectory) storeDirectory).getDelegate())).getDelegate();

        for (String filteredFile : filteredFiles) {
            // Initializing listener here to ensure that the stats increment operations are thread-safe
            UploadListener statsListener = createUploadListener(localSegmentsSizeMap);
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                statsListener.onSuccess(filteredFile);
                batchUploadListener.onResponse(resp);
                if (directory instanceof CompositeDirectory) {
                    ((CompositeDirectory) directory).afterSyncToRemote(filteredFile);
                }
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                }
                statsListener.onFailure(filteredFile);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(filteredFile);
            // Place where the actual upload is happening
            remoteDirectory.copyFrom(storeDirectory, filteredFile, IOContext.DEFAULT, aggregatedListener, isLowPriorityUpload());
        }
    }

    boolean isLowPriorityUpload() {
        return isLocalOrSnapshotRecoveryOrSeeding();
    }

    boolean isLocalOrSnapshotRecoveryOrSeeding() {
        // In this case when the primary mode is false, we need to upload segments to Remote Store
        // This is required in case of remote migration seeding/snapshots/shrink/ split/clone where we need to durable persist
        // all segments to remote before completing the recovery to ensure durability.
        return (indexShard.state() == IndexShardState.RECOVERING && indexShard.shardRouting.primary())
            && indexShard.recoveryState() != null
            && (indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS
                || indexShard.recoveryState().getRecoverySource().getType() == RecoverySource.Type.SNAPSHOT
                || indexShard.shouldSeedRemoteStore());
    }

    /**
     * Creates an {@link UploadListener} containing the stats population logic which would be triggered before and after segment upload events
     *
     * @param fileSizeMap updated map of current snapshot of local segments to their sizes
     */
    private UploadListener createUploadListener(Map<String, Long> fileSizeMap) {
        return new UploadListener() {
            private long uploadStartTime = 0;

            @Override
            public void beforeUpload(String file) {
                // Start tracking the upload bytes started
                segmentTracker.addUploadBytesStarted(fileSizeMap.get(file));
                uploadStartTime = System.currentTimeMillis();
            }

            @Override
            public void onSuccess(String file) {
                // Track upload success
                segmentTracker.addUploadBytesSucceeded(fileSizeMap.get(file));
                segmentTracker.addToLatestUploadedFiles(file);
                segmentTracker.addUploadTimeInMillis(Math.max(1, System.currentTimeMillis() - uploadStartTime));
            }

            @Override
            public void onFailure(String file) {
                // Track upload failure
                segmentTracker.addUploadBytesFailed(fileSizeMap.get(file));
                segmentTracker.addUploadTimeInMillis(Math.max(1, System.currentTimeMillis() - uploadStartTime));
            }
        };
    }

    /**
     * Whether to upload a file or not depending on whether file is in excluded list or has been already uploaded.
     *
     * @param file that needs to be uploaded.
     * @return true if the upload has to be skipped for the file.
     */
    private boolean skipUpload(String file) {
        try {
            // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
            // todo: Check if we need the second condition or is it just fail safe
            return EXCLUDE_FILES.contains(file) || remoteDirectory.containsFile(file, getChecksumOfLocalFile(file));
        } catch (IOException e) {
            logger.error(
                "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                file
            );
        }
        return false;
    }

    private String getChecksumOfLocalFile(String file) throws IOException {
        if (!localSegmentChecksumMap.containsKey(file)) {
            try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.READONCE)) {
                String checksum = Long.toString(CodecUtil.retrieveChecksum(indexInput));
                localSegmentChecksumMap.put(file, checksum);
            }
        }
        return localSegmentChecksumMap.get(file);
    }
}
