/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.nio.file.Path;

/**
 * Class to represent a fetch request for a block of a file.
 * Field names: directory, fileName, blockFileName, blockStart, blockSize, filePath.
 * Builder pattern and getters will be added in the implementation PR.
 */
@ExperimentalApi
public class BlockFetchRequest {

    private final Directory directory;
    private final String fileName;
    private final String blockFileName;
    private final long blockStart;
    private final long blockSize;
    private final Path filePath;

    private BlockFetchRequest(Builder builder) {
        this.fileName = builder.fileName;
        this.blockFileName = builder.blockFileName;
        this.filePath = builder.directory.getDirectory().resolve(blockFileName);
        this.directory = builder.directory;
        this.blockSize = builder.blockSize;
        this.blockStart = builder.blockStart;
    }

    /**
     * Creates a new Builder.
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the file path.
     * @return the file path
     */
    public Path getFilePath() {
        return filePath;
    }

    /**
     * Returns the directory.
     * @return the directory
     */
    public Directory getDirectory() {
        return directory;
    }

    /**
     * Returns the file name.
     * @return the file name
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * Returns the block file name.
     * @return the block file name
     */
    public String getBlockFileName() {
        return blockFileName;
    }

    /**
     * Returns the block size.
     * @return the block size
     */
    public long getBlockSize() {
        return blockSize;
    }

    /**
     * Returns the block start.
     * @return the block start
     */
    public long getBlockStart() {
        return blockStart;
    }

    /**
    * Builder for BlobFetchRequest
    */
    @ExperimentalApi
    public static final class Builder {
        private FSDirectory directory;
        private String fileName;
        private String blockFileName;
        private long blockSize;
        private long blockStart;

        private Builder() {}

        /**
         * Sets the directory.
         * @param directory the directory
         * @return this builder
         */
        public Builder directory(FSDirectory directory) {
            this.directory = directory;
            return this;
        }

        /**
         * Sets the file name.
         * @param fileName the file name
         * @return this builder
         */
        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        /**
         * Sets the block file name.
         * @param blockFileName the block file name
         * @return this builder
         */
        public Builder blockFileName(String blockFileName) {
            this.blockFileName = blockFileName;
            return this;
        }

        /**
         * Sets the block size.
         * @param blockSize the block size
         * @return this builder
         */
        public Builder blockSize(long blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Sets the block start.
         * @param blockStart the block start
         * @return this builder
         */
        public Builder blockStart(long blockStart) {
            this.blockStart = blockStart;
            return this;
        }

        /** Builds the BlockFetchRequest. @return the built request */
        public BlockFetchRequest build() {
            return new BlockFetchRequest(this);
        }
    }
}
