/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.indexinput.OnDemandPrefetchBlockSnapshotIndexInput;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * Test helper that exposes protected methods of OnDemandPrefetchBlockSnapshotIndexInput for testing.
 */
public class TestOnDemandPrefetchBlockSnapshotIndexInput extends OnDemandPrefetchBlockSnapshotIndexInput {

    public TestOnDemandPrefetchBlockSnapshotIndexInput(
        Builder<?> builder,
        String resourceDescription,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager,
        ThreadPool threadPool,
        FileCache fileCache,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(
            builder,
            resourceDescription,
            fileInfo,
            directory,
            transferManager,
            threadPool,
            fileCache,
            tieredStoragePrefetchSettingsSupplier
        );
    }

    @Override
    protected TestOnDemandPrefetchBlockSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new TestOnDemandPrefetchBlockSnapshotIndexInput(
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

    protected Boolean isCloned() {
        return this.isClone;
    }

    protected TransferManager getTransferManager() {
        return this.transferManager;
    }

    protected String getFileName() {
        return this.fileName;
    }

    protected int getBlockMask() {
        return this.blockMask;
    }

    protected int getBlockSize() {
        return this.blockSize;
    }

    protected int getBlockSizeShift() {
        return this.blockSizeShift;
    }

    protected FSDirectory getDirectory() {
        return this.directory;
    }

    protected long getOffset() {
        return this.offset;
    }

    protected long getLength() {
        return this.length;
    }

    @Override
    protected int getBlock(long pos) {
        return super.getBlock(pos);
    }

    @Override
    protected int getTotalBlocks() {
        return super.getTotalBlocks();
    }

    @Override
    protected int getBlockOffset(long pos) {
        return super.getBlockOffset(pos);
    }

    @Override
    protected long getBlockStart(int blockId) {
        return super.getBlockStart(blockId);
    }

    @Override
    protected List<BlobFetchRequest.BlobPart> getBlobParts(long blockStart, long blockEnd) {
        return super.getBlobParts(blockStart, blockEnd);
    }

    protected String getResourceDescription() {
        return this.resourceDescription;
    }

    @Override
    protected boolean checkIfFileEnabledReadAhead() {
        return super.checkIfFileEnabledReadAhead();
    }

    @Override
    protected boolean checkIfStoredFieldsPrefetchEnabled() {
        return super.checkIfStoredFieldsPrefetchEnabled();
    }

    @Override
    protected int currentBlockPosition() {
        return super.currentBlockPosition();
    }

    @Override
    protected long currentBlockStart() {
        return super.currentBlockStart();
    }

    @Override
    public boolean checkCacheHit(int blockId) {
        return super.checkCacheHit(blockId);
    }

    @Override
    public void downloadBlocksAsync(int startBlock, int endBlock, boolean isReadAhead) {
        super.downloadBlocksAsync(startBlock, endBlock, isReadAhead);
    }

    /**
     * Expose fetchBlock for testing.
     * @param blockId the block id to fetch
     * @return the fetched IndexInput
     * @throws IOException if an I/O error occurs
     */
    public IndexInput fetchBlockPublic(int blockId) throws IOException {
        return super.fetchBlock(blockId);
    }
}
