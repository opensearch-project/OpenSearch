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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.BlockIOContext;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * OnDemandCompositeBlockIndexInput is used by the Composite Directory to read data in blocks from Remote and cache those blocks in FileCache
 */
public class OnDemandCompositeBlockIndexInput extends OnDemandBlockIndexInput {

    private static final Logger logger = LogManager.getLogger(OnDemandCompositeBlockIndexInput.class);
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final String fileName;
    private final Long originalFileSize;
    private final FSDirectory localDirectory;
    private final IOContext context;
    private final FileCache fileCache;
    private final TransferManager transferManager;

    public OnDemandCompositeBlockIndexInput(
        RemoteSegmentStoreDirectory remoteDirectory,
        String fileName,
        FSDirectory localDirectory,
        FileCache fileCache,
        IOContext context
    ) throws IOException {
        this(
            OnDemandBlockIndexInput.builder()
                .resourceDescription("OnDemandCompositeBlockIndexInput")
                .isClone(false)
                .offset(0L)
                .length(remoteDirectory.fileLength(fileName)),
            remoteDirectory,
            fileName,
            localDirectory,
            fileCache,
            context
        );
    }

    public OnDemandCompositeBlockIndexInput(
        Builder builder,
        RemoteSegmentStoreDirectory remoteDirectory,
        String fileName,
        FSDirectory localDirectory,
        FileCache fileCache,
        IOContext context
    ) throws IOException {
        super(builder);
        this.remoteDirectory = remoteDirectory;
        this.localDirectory = localDirectory;
        this.fileName = fileName;
        this.fileCache = fileCache;
        this.context = context;
        this.transferManager = new TransferManager(
            (name, position, length) -> new InputStreamIndexInput(remoteDirectory.openInput(name, new BlockIOContext(context, position, length)), length),
            fileCache);
        originalFileSize = remoteDirectory.fileLength(fileName);
    }

    @Override
    protected OnDemandCompositeBlockIndexInput buildSlice(String sliceDescription, long offset, long length) {
        try {
            return new OnDemandCompositeBlockIndexInput(
                OnDemandBlockIndexInput.builder()
                    .blockSizeShift(blockSizeShift)
                    .isClone(true)
                    .offset(this.offset + offset)
                    .length(length)
                    .resourceDescription(sliceDescription),
                remoteDirectory,
                fileName,
                localDirectory,
                fileCache,
                context
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        logger.trace("fetchBlock called with blockId -> {}", blockId);
        final String blockFileName = fileName + "_block_" + blockId;
        final long blockStart = getBlockStart(blockId);
        final long length = getActualBlockSize(blockId);
        logger.trace(
            "File: {} , Block File: {} , Length: {} , BlockSize: {} , OriginalFileSize: {}",
            fileName,
            blockFileName,
            blockStart,
            length,
            originalFileSize
        );
        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .directory(localDirectory)
            .fileName(blockFileName)
            .blobParts(new ArrayList<>(List.of(new BlobFetchRequest.BlobPart(fileName, blockStart, length))))
            .build();
        return transferManager.fetchBlob(blobFetchRequest);
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

}
