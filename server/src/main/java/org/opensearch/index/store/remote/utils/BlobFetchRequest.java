/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;

import java.nio.file.Path;

/**
 * The specification to fetch specific block from blob store
 *
 * @opensearch.internal
 */
public class BlobFetchRequest {

    private final long position;

    private final long length;

    private final String blobName;

    private final BlobStoreIndexShardSnapshot.FileInfo fileInfo;

    private final Path filePath;

    private final Directory directory;

    private final String fileName;

    private BlobFetchRequest(Builder builder) {
        this.position = builder.position;
        this.length = builder.length;
        this.blobName = builder.blobName;
        this.fileInfo = builder.fileInfo;
        this.fileName = builder.fileName;
        this.filePath = builder.directory.getDirectory().resolve(fileName);
        this.directory = builder.directory;
    }

    public long getPosition() {
        return position;
    }

    public long getLength() {
        return length;
    }

    public String getBlobName() {
        return blobName;
    }

    public BlobStoreIndexShardSnapshot.FileInfo getFileInfo() {
        return fileInfo;
    }

    public Path getFilePath() {
        return filePath;
    }

    public Directory getDirectory() {
        return directory;
    }

    public String getFileName() {
        return fileName;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BlobFetchRequest{"
            + "position="
            + position
            + ", length="
            + length
            + ", blobName='"
            + blobName
            + '\''
            + ", filePath="
            + filePath
            + ", directory="
            + directory
            + ", fileName='"
            + fileName
            + '\''
            + '}';
    }

    /**
     * Builder for BlobFetchRequest
     */
    public static final class Builder {
        private long position;
        private long length;
        private String blobName;

        private BlobStoreIndexShardSnapshot.FileInfo fileInfo;
        private FSDirectory directory;
        private String fileName;

        private Builder() {}

        public Builder position(long position) {
            this.position = position;
            return this;
        }

        public Builder length(long length) {
            if (length <= 0) {
                throw new IllegalArgumentException("Length for blob fetch request needs to be non-negative");
            }
            this.length = length;
            return this;
        }

        public Builder blobName(String blobName) {
            this.blobName = blobName;
            return this;
        }

        public Builder fileInfo(BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
            this.fileInfo = fileInfo;
            return this;
        }

        public Builder directory(FSDirectory directory) {
            this.directory = directory;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public BlobFetchRequest build() {
            return new BlobFetchRequest(this);
        }
    }
}
