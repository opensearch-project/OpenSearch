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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.FormatNotSupportedException;
import org.opensearch.index.store.SegmentUploadFailedException;
import org.opensearch.index.store.CompositeStoreDirectory;
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
    private final CompositeStoreDirectory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;

    // Todo: Remove
    public RemoteStoreUploaderService(IndexShard indexShard, Directory storeDirectory, RemoteSegmentStoreDirectory remoteDirectory) {
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory =  null;
        this.remoteDirectory = remoteDirectory;
    }

    public RemoteStoreUploaderService(IndexShard indexShard, CompositeStoreDirectory storeDirectory, RemoteSegmentStoreDirectory remoteDirectory) {
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory =  storeDirectory;
        this.remoteDirectory = remoteDirectory;
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

        CompositeStoreDirectory directory = storeDirectory;

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
                // Todo: Update compositeDirectory for ultrawarm support
//                if (directory instanceof CompositeDirectory) {
//                    ((CompositeDirectory) directory).afterSyncToRemote(fileName);
//                }
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);

                // Handle different types of upload failures with format-aware tracking
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                } else if (ex instanceof FormatNotSupportedException) {
                    logger.error("Format not supported for file upload: file={}, format={}, error={}",
                                fileName, fileMetadata.dataFormat(), ex.getMessage());
                    // Track format-specific failure
                    trackFormatFailure(fileMetadata.dataFormat(), "format_not_supported");
                    // For format not supported errors, don't retry - fail immediately
                } else if (ex instanceof SegmentUploadFailedException) {
                    logger.error("Segment upload failed: file={}, format={}, error={}",
                                fileName, fileMetadata.dataFormat(), ex.getMessage());
                    // Track format-specific failure
                    trackFormatFailure(fileMetadata.dataFormat(), "upload_failed");
                    // Could implement retry logic here in the future
                } else {
                    logger.warn("Unexpected upload failure: file={}, format={}, error={}",
                               fileName, fileMetadata.dataFormat(), ex.getMessage(), ex);
                    // Track format-specific failure
                    trackFormatFailure(fileMetadata.dataFormat(), "unexpected_error");
                }

                statsListener.onFailure(fileMetadata);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(fileMetadata);
            // Place where the actual upload is happening - use FileMetadata-based copyFrom
            remoteDirectory.copyFrom(fileMetadata, storeDirectory, IOContext.DEFAULT, aggregatedListener, isLowPriorityUpload);
        }
    }

    /** ToDo: remove this as now e need compositeDirectory fileMetdata
     * Legacy method for backward compatibility
     * @deprecated Use {@link #uploadSegments(Collection, Map, ActionListener, Function, boolean)} with FileMetadata instead
     */
    @Override
    @Deprecated
    public void uploadSegmentsLegacy(
        Collection<String> localSegments,
        Map<String, Long> localSegmentsSizeMap,
        ActionListener<Void> listener,
        Function<Map<String, Long>, UploadListener> uploadListenerFunction,
        boolean isLowPriorityUpload
    ) {
        if (localSegments.isEmpty()) {
            logger.debug("No new segments to upload in uploadNewSegments");
            listener.onResponse(null);
            return;
        }

        logger.debug("Effective new segments files to upload {}", localSegments);
        ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, localSegments.size());

        CompositeStoreDirectory directory = storeDirectory;

        for (String localSegment : localSegments) {
            // Initializing listener here to ensure that the stats increment operations are thread-safe
            UploadListener statsListener = uploadListenerFunction.apply(localSegmentsSizeMap);
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
               // statsListener.onSuccess(localSegment);
                batchUploadListener.onResponse(resp);
                // Once uploaded to Remote, local files become eligible for eviction from FileCache
//                if (directory instanceof CompositeDirectory) {
//                    ((CompositeDirectory) directory).afterSyncToRemote(localSegment);
//                }
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                }
               // statsListener.onFailure(localSegment);
                batchUploadListener.onFailure(ex);
            });
            //statsListener.beforeUpload(localSegment);
            // Place where the actual upload is happening - use legacy string-based copyFrom
            //remoteDirectory.copyFrom(storeDirectory, localSegment, IOContext.DEFAULT, aggregatedListener, isLowPriorityUpload);
        }
    }

    /**
     * Tracks format-specific upload failures for monitoring and recovery purposes.
     * This helps identify which formats are experiencing issues and enables
     * format-specific error recovery strategies.
     *
     * @param formatName the name of the format that failed
     * @param failureType the type of failure (for logging purposes)
     */
    private void trackFormatFailure(String formatName, String failureType) {
        try {
            // Get the segment tracker from the index shard to record format-specific failures
            // This assumes RemoteStoreRefreshListener exposes the segment tracker
            // In a real implementation, we might need to pass the tracker as a dependency
            logger.debug("Tracking format failure: format={}, failureType={}", formatName, failureType);

            // For now, just log the failure. In a complete implementation, we would:
            // 1. Get access to RemoteSegmentTransferTracker
            // 2. Call incrementFormatUploadFailure(formatName)
            // 3. Potentially trigger format-specific recovery logic

        } catch (Exception e) {
            logger.warn("Failed to track format failure: format={}, failureType={}, error={}",
                       formatName, failureType, e.getMessage(), e);
        }
    }
}
