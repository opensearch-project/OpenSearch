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
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.storage.slowlogs.TieredStoragePerQueryMetric;
import org.opensearch.storage.slowlogs.TieredStorageQueryMetricService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import static org.opensearch.storage.prefetch.TieredStoragePrefetchSettings.CFS_FILE_SUFFIX;

/**
 * Block-based index input that prefetches subsequent blocks from remote storage on demand.
 */
public class OnDemandPrefetchBlockSnapshotIndexInput extends OnDemandBlockSnapshotIndexInput {

    /** Supplier for prefetch settings */
    public final Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier;
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
        FileCache fileCache,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(resourceDescription, fileInfo, offset, length, isClone, directory, transferManager);
        this.threadPool = threadPool;
        this.fileCache = fileCache;
        this.resourceDescription = resourceDescription;
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        // Record cache access attempt and track hit/miss
        String blockFileName = fileName + "_block_" + blockId;
        boolean cacheHit = checkCacheHit(blockId);
        final TieredStoragePerQueryMetric metricCollector = TieredStorageQueryMetricService.getInstance()
            .getMetricCollector(Thread.currentThread().threadId());
        metricCollector.recordFileAccess(blockFileName, cacheHit);
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
        FileCache fileCache,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(builder, fileInfo, directory, transferManager);
        this.threadPool = threadPool;
        this.fileCache = fileCache;
        this.resourceDescription = resourceDescription;
        this.tieredStoragePrefetchSettingsSupplier = tieredStoragePrefetchSettingsSupplier;
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
            fileCache,
            tieredStoragePrefetchSettingsSupplier
        );
    }

    protected void fetchNextNBlocks(int blockId) {
        // check if read ahead was enabled and file type was doc values
        if (!checkIfFileEnabledReadAhead()) {
            return;
        }
        int readAheadBlockCount = tieredStoragePrefetchSettingsSupplier.get().getReadAheadBlockCount();
        readAheadBlockCount = Math.min(readAheadBlockCount, getTotalBlocks() - 1 - blockId);
        if (readAheadBlockCount <= 0) {
            logger.trace("read ahead block is <=0, for File: {} and Block ID: {}", fileName, blockId);
            return;
        }
        logger.trace("Prefetching Read Ahead Block Count: {} from Block ID: {} for File: {}", readAheadBlockCount, blockId, fileName);
        downloadBlocksAsync(blockId + 1, blockId + readAheadBlockCount, true);
        TieredStorageQueryMetricService.getInstance().recordDocValuesPrefetch(true);
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        // This can trigger by lucene as well internally having validation here will make us to stop async download if needed.
        if (!checkIfStoredFieldsPrefetchEnabled()) {
            return;
        }
        if (length <= 0) {
            return;
        }
        offset = offset + this.offset;
        final int startBlock = getBlock(offset);
        final int endBlock = Math.min(getTotalBlocks() - 1, getBlock(offset + length - 1L));
        logger.trace("Prefetching Index Input Block From {} to {} for File {}", startBlock, endBlock, fileName);
        downloadBlocksAsync(startBlock, endBlock, false);
    }

    protected void downloadBlocksAsync(int startBlock, int endBlock, boolean isReadAhead) {
        final TieredStoragePerQueryMetric metricCollector = TieredStorageQueryMetricService.getInstance()
            .getMetricCollector(Thread.currentThread().threadId());
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
            if (isReadAhead) {
                metricCollector.recordReadAhead(fileName, nextBlockId);
            } else {
                metricCollector.recordPrefetch(fileName, nextBlockId);
            }
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
                    "Exception while fetching block asynchronously from remote - "
                        + "File: {} , Block File: {} , BlockStart: {} , BlockEnd: {} , OriginalFileSize: {}",
                    fileName,
                    blockFileName,
                    blockStart,
                    blockEnd,
                    originalFileSize
                );
            }
        }
    }

    /**
     * Checks if read-ahead is enabled for the current file format.
     * @return true if the file format supports read-ahead
     */
    protected boolean checkIfFileEnabledReadAhead() {
        return tieredStoragePrefetchSettingsSupplier.get()
            .getReadAheadEnableFileFormats()
            .stream()
            .anyMatch(format -> fileName.endsWith(format) || (resourceDescription.endsWith(format) && fileName.endsWith(CFS_FILE_SUFFIX)));
    }

    /**
     * Checks if stored fields prefetch is enabled.
     * @return true if stored fields prefetch is enabled
     */
    protected boolean checkIfStoredFieldsPrefetchEnabled() {
        return tieredStoragePrefetchSettingsSupplier.get().isStoredFieldsPrefetchEnabled();
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
