/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * This is an implementation of {@link OnDemandBlockIndexInput} where this class provides the main IndexInput using shard snapshot files.
 * <br>
 * This class rely on {@link TransferManager} to really fetch the snapshot files from the remote blob store and maybe cache them
 *
 * @opensearch.internal
 */
public class OnDemandBlockSnapshotIndexInput extends OnDemandBlockIndexInput {
    private static final Logger logger = LogManager.getLogger(OnDemandBlockSnapshotIndexInput.class);

    /**
     * Where this class fetches IndexInput parts from
     */
    final TransferManager transferManager;

    /**
     * FileInfo contains snapshot metadata references for this IndexInput
     */
    protected final FileInfo fileInfo;

    /**
     * Underlying lucene directory to open blocks and for caching
     */
    protected final FSDirectory directory;
    /**
     * File Name
     */
    protected final String fileName;

    /**
     * part size  in bytes
     */
    protected final long partSize;

    /**
     * Size of the file, larger than length if it's a slice
     */
    protected final long originalFileSize;

    public OnDemandBlockSnapshotIndexInput(FileInfo fileInfo, FSDirectory directory, TransferManager transferManager) {
        this(
            "BlockedSnapshotIndexInput(path=\""
                + directory.getDirectory().toString()
                + "/"
                + fileInfo.physicalName()
                + "\", "
                + "offset="
                + 0
                + ", length= "
                + fileInfo.length()
                + ")",
            fileInfo,
            0L,
            fileInfo.length(),
            false,
            directory,
            transferManager
        );
    }

    public OnDemandBlockSnapshotIndexInput(
        String resourceDescription,
        FileInfo fileInfo,
        long offset,
        long length,
        boolean isClone,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        this(
            OnDemandBlockIndexInput.builder().resourceDescription(resourceDescription).isClone(isClone).offset(offset).length(length),
            fileInfo,
            directory,
            transferManager
        );
    }

    OnDemandBlockSnapshotIndexInput(
        OnDemandBlockIndexInput.Builder builder,
        FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        super(builder);
        this.transferManager = transferManager;
        this.fileInfo = fileInfo;
        this.partSize = fileInfo.partSize().getBytes();
        this.fileName = fileInfo.physicalName();
        this.directory = directory;
        this.originalFileSize = fileInfo.length();
    }

    @Override
    protected OnDemandBlockSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandBlockSnapshotIndexInput(
            OnDemandBlockIndexInput.builder()
                .blockSizeShift(blockSizeShift)
                .isClone(true)
                .offset(this.offset + offset)
                .length(length)
                .resourceDescription(sliceDescription),
            fileInfo,
            directory,
            transferManager
        );
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        final String blockFileName = fileName + "." + blockId;

        final long blockStart = getBlockStart(blockId);
        final long blockEnd = blockStart + getActualBlockSize(blockId);
        final int part = (int) (blockStart / partSize);
        final long partStart = part * partSize;

        final long position = blockStart - partStart;
        final long offset = blockEnd - blockStart - partStart;

        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .position(position)
            .length(offset)
            .blobName(fileInfo.partName(part))
            .directory(directory)
            .fileName(blockFileName)
            .build();
        try {
            return transferManager.asyncFetchBlob(blobFetchRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure while fetching [{}]", blobFetchRequest), e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public OnDemandBlockSnapshotIndexInput clone() {
        OnDemandBlockSnapshotIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        if (currentBlock != null) {
            clone.currentBlock = currentBlock.clone();
            clone.currentBlockId = currentBlockId;
        }

        return clone;
    }

    protected long getActualBlockSize(int blockId) {
        return (blockId != getBlock(originalFileSize - 1)) ? blockSize : getBlockOffset(originalFileSize - 1) + 1;
    }
}
