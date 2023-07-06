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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;

/**
 * This is an implementation of {@link OnDemandBlockIndexInput} where this class provides the main IndexInput using uploaded segment metadata files.
 * <br>
 * This class rely on {@link TransferManager} to really fetch the segment files from the remote blob store and maybe cache them
 *
 * @opensearch.internal
 */
public class OnDemandBlockSearchIndexInput extends OnDemandBlockIndexInput {
    private static final Logger logger = LogManager.getLogger(OnDemandBlockSearchIndexInput.class);

    /**
     * Where this class fetches IndexInput parts from
     */
    final TransferManager transferManager;

    /**
     * Uploaded metadata info for this IndexInput
     */

    protected final RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata;

    /**
     * Underlying lucene directory to open blocks and for caching
     */
    protected final FSDirectory directory;
    /**
     * File Name
     */
    protected final String fileName;

    /**
     * Size of the file, larger than length if it's a slice
     */
    protected final long originalFileSize;

    public OnDemandBlockSearchIndexInput(RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata, FSDirectory directory, TransferManager transferManager) {
        this(
            "BlockedSearchIndexInput(path=\""
                + directory.getDirectory().toString()
                + "/"
                + uploadedSegmentMetadata.getOriginalFilename()
                + "\", "
                + "offset="
                + 0
                + ", length= "
                + uploadedSegmentMetadata.getLength()
                + ")",
            uploadedSegmentMetadata,
            0L,
            uploadedSegmentMetadata.getLength(),
            false,
            directory,
            transferManager
        );
    }

    public OnDemandBlockSearchIndexInput(
        String resourceDescription,
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata,
        long offset,
        long length,
        boolean isClone,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        this(
            OnDemandBlockIndexInput.builder().resourceDescription(resourceDescription).isClone(isClone).offset(offset).length(length),
            uploadedSegmentMetadata,
            directory,
            transferManager
        );
    }

    OnDemandBlockSearchIndexInput(
        Builder builder,
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata uploadedSegmentMetadata,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        super(builder);
        this.transferManager = transferManager;
        this.uploadedSegmentMetadata  = uploadedSegmentMetadata;
        this.fileName = uploadedSegmentMetadata.getOriginalFilename();
        this.directory = directory;
        this.originalFileSize = uploadedSegmentMetadata.getLength();
    }

    @Override
    protected OnDemandBlockSearchIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandBlockSearchIndexInput(
            OnDemandBlockIndexInput.builder()
                .blockSizeShift(blockSizeShift)
                .isClone(true)
                .offset(this.offset + offset)
                .length(length)
                .resourceDescription(sliceDescription),
            uploadedSegmentMetadata,
            directory,
            transferManager
        );
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        final String blockFileName = uploadedSegmentMetadata.getUploadedFilename() + "." + blockId;

        final long blockStart = getBlockStart(blockId);
        final long blockEnd = blockStart + getActualBlockSize(blockId);
        final long length = blockEnd - blockStart;

        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .position(blockStart)
            .length(length)
            .blobName(uploadedSegmentMetadata.getUploadedFilename())
            .directory(directory)
            .fileName(blockFileName)
            .build();
        return transferManager.fetchBlob(blobFetchRequest);
    }

    @Override
    public OnDemandBlockSearchIndexInput clone() {
        OnDemandBlockSearchIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        clone.cloneBlock(this);
        return clone;
    }

    protected long getActualBlockSize(int blockId) {
        return (blockId != getBlock(originalFileSize - 1)) ? blockSize : getBlockOffset(originalFileSize - 1) + 1;
    }
}
