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
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.BlockIOContext;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

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
        Path blockFilePath = getLocalFilePath(blockFileName);
        final CachedIndexInput cacheEntry = fileCache.compute(blockFilePath, (path, cachedIndexInput) -> {
            if (cachedIndexInput == null || cachedIndexInput.isClosed()) {
                // Doesn't exist or is closed, either way create a new one
                IndexInput indexInput = fetchIndexInput(blockFileName, blockStart, length);
                return new CachedIndexInputImpl(indexInput);
            } else {
                logger.trace("Block already present in cache");
                // already in the cache and ready to be used (open)
                return cachedIndexInput;
            }
        });

        return cacheEntry.getIndexInput();
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

    private Path getLocalFilePath(String file) {
        return localDirectory.getDirectory().resolve(file);
    }

    private IndexInput fetchIndexInput(String blockFileName, long start, long length) {
        IndexInput indexInput;
        Path filePath = getLocalFilePath(blockFileName);
        try {
            // Fetch from local if block file is present locally in disk
            indexInput = localDirectory.openInput(blockFileName, IOContext.READ);
            logger.trace("Block file present locally, just putting it in cache");
        } catch (FileNotFoundException | NoSuchFileException e) {
            logger.trace("Block file not present locally, fetching from Remote");
            // If block file is not present locally in disk, fetch from remote and persist the block file in disk
            try (
                OutputStream fileOutputStream = Files.newOutputStream(filePath);
                OutputStream localFileOutputStream = new BufferedOutputStream(fileOutputStream)
            ) {
                logger.trace("Fetching block file from Remote");
                indexInput = remoteDirectory.openInput(fileName, new BlockIOContext(IOContext.READ, start, length));
                logger.trace("Persisting the fetched blocked file from Remote");
                int indexInputLength = (int) indexInput.length();
                byte[] bytes = new byte[indexInputLength];
                indexInput.readBytes(bytes, 0, indexInputLength);
                localFileOutputStream.write(bytes);
            } catch (Exception err) {
                logger.trace("Exception while fetching block from remote and persisting it on disk");
                throw new RuntimeException(err);
            }
        } catch (Exception e) {
            logger.trace("Exception while fetching block file locally");
            throw new RuntimeException(e);
        }
        return indexInput;
    }

    /**
     * Implementation of the CachedIndexInput interface
     */
    private class CachedIndexInputImpl implements CachedIndexInput {

        IndexInput indexInput;
        AtomicBoolean isClosed;

        /**
         * Constructor - takes IndexInput as parameter
         */
        CachedIndexInputImpl(IndexInput indexInput) {
            this.indexInput = indexInput;
            isClosed = new AtomicBoolean(false);
        }

        /**
         * Returns the wrapped indexInput
         */
        @Override
        public IndexInput getIndexInput() throws IOException {
            return indexInput;
        }

        /**
         * Returns the length of the wrapped indexInput
         */
        @Override
        public long length() {
            return indexInput.length();
        }

        /**
         * Checks if the wrapped indexInput is closed
         */
        @Override
        public boolean isClosed() {
            return isClosed.get();
        }

        /**
         * Closes the wrapped indexInput
         */
        @Override
        public void close() throws Exception {
            indexInput.close();
            isClosed.set(true);
        }
    }
}
