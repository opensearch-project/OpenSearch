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
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * The service essentially acts as a bridge between local segment storage and remote storage,
 * ensuring efficient and reliable segment synchronization while providing comprehensive monitoring and error handling.
 */
public class RemoteStoreUploaderService implements RemoteStoreUploader {

    private final Logger logger;

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;

    public RemoteStoreUploaderService(IndexShard indexShard, Directory storeDirectory, RemoteSegmentStoreDirectory remoteDirectory) {
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
    }

    @Override
    public void uploadSegments(
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
        Directory directory = ((FilterDirectory) (((FilterDirectory) storeDirectory).getDelegate())).getDelegate();

        for (String localSegment : localSegments) {
            // Initializing listener here to ensure that the stats increment operations are thread-safe
            UploadListener statsListener = uploadListenerFunction.apply(localSegmentsSizeMap);
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                statsListener.onSuccess(localSegment);
                batchUploadListener.onResponse(resp);
                // Once uploaded to Remote, local files become eligible for eviction from FileCache
                if (directory instanceof CompositeDirectory compositeDirectory) {
                    compositeDirectory.afterSyncToRemote(localSegment);
                }
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                }
                statsListener.onFailure(localSegment);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(localSegment);
            // Place where the actual upload is happening
            remoteDirectory.copyFrom(storeDirectory, localSegment, IOContext.DEFAULT, aggregatedListener, isLowPriorityUpload);
        }
    }
}
