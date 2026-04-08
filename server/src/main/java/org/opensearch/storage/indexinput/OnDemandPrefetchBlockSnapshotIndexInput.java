/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;

public class OnDemandPrefetchBlockSnapshotIndexInput extends OnDemandBlockSnapshotIndexInput {

    protected final ThreadPool threadPool;
    protected FileCache fileCache;
    protected final String resourceDescription;
    private static final Logger logger = LogManager.getLogger(OnDemandPrefetchBlockSnapshotIndexInput.class);

    public OnDemandPrefetchBlockSnapshotIndexInput(
        String resourceDescription,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        long offset,
        long length,
        boolean isClone,
        FSDirectory directory,
        TransferManager transferManager,
        ThreadPool threadPool,
        FileCache fileCache
    ) {
        super(resourceDescription, fileInfo, offset, length, isClone, directory, transferManager);
        this.threadPool = threadPool;
        this.fileCache = fileCache;
        this.resourceDescription = resourceDescription;
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        String blockFileName = fileName + "_block_" + blockId;
        boolean cacheHit = checkCacheHit(blockId);
        // Metric recording will be added when TieredStorageQueryMetricService is available
        fetchNextNBlocks(blockId);
        return super.fetchBlock(blockId);
    }

    public OnDemandPrefetchBlockSnapshotIndexInput(
        AbstractBlockIndexInput.Builder<?> builder,
        String resourceDescription,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager,
        ThreadPool threadPool,
        FileCache fileCache
    ) {
        super(builder, fileInfo, directory, transferManager);
        this.threadPool = threadPool;
        this.fileCache = fileCache;
        this.resourceDescription = resourceDescription;
    }

    @Override
    protected OnDemandPrefetchBlockSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandPrefetchBlockSnapshotIndexInput(
            AbstractBlockIndexInput.builder()
                .blockSizeShift(blockSizeShift)
                .isClone(true)
                .offset(this.offset + offset)
                .length(length)
                .resourceDescription(sliceDescription),
            sliceDescription,
            fileInfo,
            directory,
            transferManager,
            threadPool,
            fileCache
        );
    }

    protected void fetchNextNBlocks(int blockId) {
        // Read-ahead with default block count. TieredStoragePrefetchSettings integration will be added later.
        int readAheadBlockCount = 4; // DEFAULT_READ_AHEAD_BLOCK_COUNT
        readAheadBlockCount = Math.min(readAheadBlockCount, getTotalBlocks() - 1 - blockId);
        if(readAheadBlockCount <= 0) {
            logger.trace("read ahead block is <=0, for File: {} and Block ID: {}", fileName, blockId);
            return;
        }
        logger.trace("Prefetching Read Ahead Block Count: {} from Block ID: {} for File: {}", readAheadBlockCount, blockId, fileName);
        downloadBlocksAsync(blockId + 1, blockId + readAheadBlockCount, true);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        offset = offset + this.offset;
        final int startBlock = getBlock(offset);
        final int endBlock = Math.min(getTotalBlocks() - 1, getBlock(offset + length - 1L));
        logger.trace("Prefetching Index Input Block From {} to {} for File {}", startBlock, endBlock, fileName);
        downloadBlocksAsync(startBlock, endBlock, false);
    }

    protected void downloadBlocksAsync(int startBlock, int endBlock, boolean isReadAhead) {
        for (int nextBlockId = startBlock; nextBlockId <= endBlock; nextBlockId++) {
            String blockFileName = fileName + "_block_" + nextBlockId;
            long blockStart = getBlockStart(nextBlockId);
            long blockEnd = blockStart + getActualBlockSize(nextBlockId, blockSizeShift, originalFileSize);
            logger.trace(
                "File: {} , Block File: {} , BlockStart: {} , BlockEnd: {} , OriginalFileSize: {}",
                fileName,
                blockFileName,
                blockStart,
                blockEnd,
                originalFileSize
            );
            // Metric recording will be added when TieredStorageQueryMetricService is available
            // Block may be present on multiple chunks of a file, so we need
            // to fetch each chunk/blob part separately to fetch an entire block.
            BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
                .blobParts(getBlobParts(blockStart, blockEnd))
                .directory(directory)
                .fileName(blockFileName)
                .build();
            try {
                transferManager.fetchBlobAsync(blobFetchRequest);
            } catch (Exception e) {
                logger.error(
                    "Exception while fetching block asynchronously from remote - " +
                        "File: {} , Block File: {} , BlockStart: {} , BlockEnd: {} , OriginalFileSize: {}",
                    fileName,
                    blockFileName,
                    blockStart,
                    blockEnd,
                    originalFileSize
                );
            }
        }
    }

    @Override
    public OnDemandPrefetchBlockSnapshotIndexInput clone() {
        OnDemandPrefetchBlockSnapshotIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        clone.cloneBlock(this);
        return clone;
    }

    protected int getTotalBlocks() {
        return (int) ((originalFileSize - 1) >>> blockSizeShift) + 1;
    }

    /**
     * Checks if a block file exists in the file cache.
     * This method determines cache hit/miss status for transfer manager operations.
     *
     * @param blockId the id of the block to check
     * @return true if the block exists in cache (cache hit), false otherwise (cache miss)
     */
    protected boolean checkCacheHit(int blockId) {
        final String blockFileName = fileName + "_block_" + blockId;
        Path filePath = directory.getDirectory().resolve(blockFileName);
        return fileCache.getRef(filePath) != null;// File exists (cache hit)
    }
}
