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
import org.opensearch.index.store.CompositeDirectoryRemoteStoreFileTrackerAdapter;
import org.opensearch.index.store.RemoteStoreFileTrackerAdapter;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;

import java.io.IOException;

public class OnDemandCompositeBlockIndexInput extends OnDemandBlockIndexInput {

    private static final Logger logger = LogManager.getLogger(OnDemandCompositeBlockIndexInput.class);
    private final RemoteStoreFileTrackerAdapter remoteStoreFileTrackerAdapter;
    private final String fileName;
    private final Long originalFileSize;
    private final FSDirectory directory;

    public OnDemandCompositeBlockIndexInput(RemoteStoreFileTrackerAdapter remoteStoreFileTrackerAdapter, String fileName, FSDirectory directory) {
        this(
            OnDemandBlockIndexInput.builder().
                resourceDescription("OnDemandCompositeBlockIndexInput").
                isClone(false).
                offset(0L).
                length(getFileLength(remoteStoreFileTrackerAdapter, fileName)),
            remoteStoreFileTrackerAdapter,
            fileName,
            directory);
    }

    public OnDemandCompositeBlockIndexInput(Builder builder, RemoteStoreFileTrackerAdapter remoteStoreFileTrackerAdapter, String fileName, FSDirectory directory) {
        super(builder);
        this.remoteStoreFileTrackerAdapter = remoteStoreFileTrackerAdapter;
        this.directory = directory;
        this.fileName = fileName;
        originalFileSize = getFileLength(remoteStoreFileTrackerAdapter, fileName);
    }

    @Override
    protected OnDemandCompositeBlockIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandCompositeBlockIndexInput(
            OnDemandBlockIndexInput.builder().
                blockSizeShift(blockSizeShift).
                isClone(true).
                offset(this.offset + offset).
                length(length).
                resourceDescription(sliceDescription),
            remoteStoreFileTrackerAdapter,
            fileName,
            directory
        );
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        logger.trace("fetchBlock called with blockId -> " + blockId);
        final String uploadedFileName = ((CompositeDirectoryRemoteStoreFileTrackerAdapter)remoteStoreFileTrackerAdapter).getUploadedFileName(fileName);
        final String blockFileName = fileName + "_block_" + blockId;
        final long blockStart = getBlockStart(blockId);
        final long length = getActualBlockSize(blockId);
        logger.trace("File: " + uploadedFileName +
            ", Block File: " + blockFileName +
            ", BlockStart: " + blockStart +
            ", Length: " + length +
            ", BlockSize: " + blockSize +
            ", OriginalFileSize: " + originalFileSize);

        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .position(blockStart)
            .length(length)
            .blobName(uploadedFileName)
            .directory(directory)
            .fileName(blockFileName)
            .build();
        return remoteStoreFileTrackerAdapter.fetchBlob(blobFetchRequest);
    }

    @Override
    public OnDemandBlockIndexInput clone() {
        OnDemandCompositeBlockIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        clone.cloneBlock(this);
        return clone;
    }

    private long getActualBlockSize(int blockId) {
        return (blockId != getBlock(originalFileSize - 1)) ? blockSize : getBlockOffset(originalFileSize - 1) + 1;
    }

    private static long getFileLength(RemoteStoreFileTrackerAdapter remoteStoreFileTrackerAdapter, String fileName) {
        return ((CompositeDirectoryRemoteStoreFileTrackerAdapter)remoteStoreFileTrackerAdapter).getFileLength(fileName);
    }
}
