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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.store.SegmentUploadFailedException;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The service essentially acts as a bridge between local segment storage and remote storage,
 * ensuring efficient and reliable segment synchronization while providing comprehensive monitoring and error handling.
 */
public class RemoteStoreUploaderService implements RemoteStoreUploader {

    private final Logger logger;

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final boolean isOptimizedIndex;

    public RemoteStoreUploaderService(IndexShard indexShard, Directory storeDirectory, RemoteSegmentStoreDirectory remoteDirectory, boolean isOptimizedIndex) {
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory =  storeDirectory;
        this.remoteDirectory = remoteDirectory;
        this.isOptimizedIndex = isOptimizedIndex;
    }

    @Override
    public void uploadSegments(
        Collection<FileMetadata> fileMetadataCollection,
        Map<FileMetadata, Long> fileMetadataSizeMap,
        ActionListener<Void> listener,
        Function<Map<FileMetadata, Long>, UploadListener> uploadListenerFunction,
        boolean isLowPriorityUpload
    ) {
        if (fileMetadataCollection.isEmpty()) {
            logger.debug("No new segments to upload in uploadNewSegments");
            listener.onResponse(null);
            return;
        }

        // Log format-aware upload statistics
        Map<String, Long> formatCounts = fileMetadataCollection.stream()
            .collect(Collectors.groupingBy(
                fm -> fm.dataFormat(),
                Collectors.counting()
            ));

        Map<String, Long> formatSizes = fileMetadataCollection.stream()
            .collect(Collectors.groupingBy(
                fm -> fm.dataFormat(),
                Collectors.summingLong(fm -> fileMetadataSizeMap.getOrDefault(fm.file(), 0L))
            ));

        logger.debug("Format-aware segment upload starting: totalFiles={}, formatCounts={}, formatSizes={}",
                    fileMetadataCollection.size(), formatCounts, formatSizes);
        ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, fileMetadataCollection.size());

        for (FileMetadata fileMetadata : fileMetadataCollection) {
            String fileName = fileMetadata.file();
            // Initializing listener here to ensure that the stats increment operations are thread-safe
            UploadListener statsListener = uploadListenerFunction.apply(fileMetadataSizeMap);
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                statsListener.onSuccess(fileMetadata);
                batchUploadListener.onResponse(resp);

                // Log format-specific upload success
                long fileSize = fileMetadataSizeMap.getOrDefault(fileMetadata, 0L);
                logger.debug("Format-aware upload completed: file={}, format={}, size={} bytes",
                            fileName, fileMetadata.dataFormat(), fileSize);

                // Once uploaded to Remote, local files become eligible for eviction from FileCache
                // Todo:@Kamal Update compositeDirectory for ultrawarm support
//                if (directory instanceof CompositeDirectory) {
//                    ((CompositeDirectory) directory).afterSyncToRemote(fileName);
//                }
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                } else if (ex instanceof SegmentUploadFailedException) {
                    logger.error("Segment upload failed: file={}, format={}, error={}",
                                fileName, fileMetadata.dataFormat(), ex.getMessage());
                } else {
                    logger.warn("Unexpected upload failure: file={}, format={}, error={}",
                               fileName, fileMetadata.dataFormat(), ex.getMessage(), ex);
                }

                statsListener.onFailure(fileMetadata);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(fileMetadata);
            remoteDirectory.copyFrom(
                storeDirectory,
                isOptimizedIndex ? fileMetadata.serialize() : fileMetadata.file(),
                IOContext.DEFAULT,
                aggregatedListener,
                isLowPriorityUpload
            );
        }
    }
}
