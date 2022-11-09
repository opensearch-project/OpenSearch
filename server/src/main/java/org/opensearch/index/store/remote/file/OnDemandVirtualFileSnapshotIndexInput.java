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
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/**
 * This is an implementation of {@link OnDemandBlockIndexInput} where this class provides the main IndexInput using shard snapshot virtual
 * files and will basically read virtual file from memory and write it to disk .
 *
 * @opensearch.internal
 */
public class OnDemandVirtualFileSnapshotIndexInput extends OnDemandBlockIndexInput {
    private static final Logger logger = LogManager.getLogger(OnDemandVirtualFileSnapshotIndexInput.class);

    // 2^30 should keep the virtual file in memory un-partitioned when written to disk
    private static final int BLOCK_SIZE_SHIFT = 30;

    /**
     * Where this class fetches IndexInput parts from
     */
    private final TransferManager transferManager;

    /**
     * FileInfo contains snapshot metadata references for this IndexInput
     */
    private final FileInfo fileInfo;

    /**
     * underlying lucene directory to open file
     */
    protected final FSDirectory directory;

    /**
     * file name
     */
    protected final String fileName;

    public OnDemandVirtualFileSnapshotIndexInput(FileInfo fileInfo, FSDirectory directory, TransferManager transferManager) {
        this(
            "VirtualFileSnapshotIndexInput(path=\""
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
            directory,
            transferManager,
            fileInfo.physicalName(),
            0L,
            fileInfo.length(),
            false
        );
    }

    public OnDemandVirtualFileSnapshotIndexInput(
        String resourceDescription,
        FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager,
        String fileName,
        long offset,
        long length,
        boolean isClone
    ) {
        super(
            OnDemandBlockIndexInput.builder()
                .resourceDescription(resourceDescription)
                .isClone(isClone)
                .offset(offset)
                .length(length)
                .blockSizeShift(BLOCK_SIZE_SHIFT)
        );
        this.fileInfo = fileInfo;
        this.directory = directory;
        this.fileName = fileName;
        this.transferManager = transferManager;
    }

    @Override
    protected OnDemandVirtualFileSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandVirtualFileSnapshotIndexInput(
            sliceDescription,
            this.fileInfo,
            this.directory,
            this.transferManager,
            this.fileName,
            offset,
            length,
            true
        );
    }

    @Override
    public OnDemandVirtualFileSnapshotIndexInput clone() {
        OnDemandVirtualFileSnapshotIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        if (currentBlock != null) {
            clone.currentBlock = currentBlock.clone();
            clone.currentBlockId = currentBlockId;
        }
        return clone;
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        // will always have one block.
        final Path filePath = directory.getDirectory().resolve(fileName);
        try {
            return transferManager.asyncFetchBlob(filePath, () -> new ByteArrayIndexInput(fileName, fileInfo.metadata().hash().bytes))
                .get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(() -> new ParameterizedMessage("unexpected failure while fetching [{}]", filePath), e);
            throw new IllegalStateException(e);
        }
    }
}
