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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;

import java.io.IOException;
import java.util.Objects;

/**
 * This is a virtual index input that is backed by block files. The block files are downloaded from remote store.
 * The block files are a sequence of bytes of fixed block size belonging to a segment file and is persisted as blocks.
 */
public class BlockIndexInput extends AbstractBlockIndexInput {
    /**
     * logger
     */
    private static final Logger logger = LogManager.getLogger(BlockIndexInput.class);

    /**
     * file name
     */
    protected final String fileName;

    /**
     * size of the file, larger than length if it's a slice
     */
    protected final long fileSize;

    /**
     * underlying lucene directory to open block files.
     */
    protected final FSDirectory directory;

    protected final IOContext context;

    /**
     * Constructor to create BlockIndexInput using builder.
     * @param builder blocked indexinput builder
     */
    BlockIndexInput(Builder builder) {
        super(builder);
        this.fileName = builder.name;
        this.fileSize = builder.fileSize;
        this.directory = builder.directory;
        this.context = builder.context;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public BlockIndexInput clone() {
        BlockIndexInput clone = buildSlice("clone", 0, length());
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        clone.cloneBlock(this);
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this
            );
        }

        // The slice is seeked to the beginning.
        return buildSlice(sliceDescription, offset, length);
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    public BlockIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return builder().resourceDescription(getFullSliceDescription(sliceDescription))
            .name(fileName)
            .localDirectory(directory)
            .offset(this.offset + offset)
            .length(length)
            .isClone(true)
            .fileSize(fileSize)
            .context(this.context)
            .blockSizeShift(blockSizeShift)
            .build();
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        logger.debug("fetchBlock called with blockId -> {}", blockId);
        final String blockFileName = getBlockFileName(fileName, blockId);

        final long blockStart = getBlockStart(blockId);
        final long blockEnd = blockStart + getActualBlockSize(blockId, blockSizeShift, fileSize);
        logger.debug(
            "File: {} , Block File: {} , BlockStart: {} , BlockEnd: {} , OriginalFileSize: {}",
            fileName,
            blockFileName,
            blockStart,
            blockEnd,
            fileSize
        );
        return this.directory.openInput(blockFileName, this.context);
    }

    /**
     * Builder for constructing {@link BlockIndexInput} instances.
     */
    public static final class Builder extends AbstractBlockIndexInput.Builder<Builder> {

        private String name;
        private FSDirectory directory;
        private long fileSize;
        private IOContext context;

        private Builder() {
            super();
        }

        public Builder name(String fileName) {
            this.name = Objects.requireNonNull(fileName, "File name cannot be null");
            return this;
        }

        public Builder localDirectory(FSDirectory directory) {
            this.directory = Objects.requireNonNull(directory, "Directory cannot be null");
            return this;
        }

        public Builder fileSize(long fileSize) {
            this.fileSize = fileSize;
            return this;
        }

        public Builder context(IOContext context) {
            this.context = context;
            return this;
        }

        public String getResourceDescription() {
            return resourceDescription != null
                ? resourceDescription
                : "BlockedLuceneFile(path=\"" + directory.getDirectory().toString() + "/" + name + "\")";
        }

        public BlockIndexInput build() {
            // Validate this class's fields
            if (name == null) throw new IllegalStateException("File name must be set");
            if (directory == null) throw new IllegalStateException("Directory must be set");
            if (fileSize <= 0) throw new IllegalStateException("File size must be positive");
            if (context == null) throw new IllegalStateException("IOContext must be set");

            return new BlockIndexInput(this);
        }
    }
}
