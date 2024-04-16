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

import java.nio.file.Path;
import java.util.List;

/**
 * The specification to fetch specific block from blob store
 *
 * @opensearch.internal
 */
public class BlobFetchRequest {

    private final Path filePath;

    private final Directory directory;

    private final String fileName;

    private final List<BlobPart> blobParts;

    private final long blobLength;

    private BlobFetchRequest(Builder builder) {
        this.fileName = builder.fileName;
        this.filePath = builder.directory.getDirectory().resolve(fileName);
        this.directory = builder.directory;
        this.blobParts = builder.blobParts;
        this.blobLength = builder.blobParts.stream().mapToLong(o -> o.getLength()).sum();
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

    public List<BlobPart> blobParts() {
        return blobParts;
    }

    public long getBlobLength() {
        return blobLength;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "BlobFetchRequest{"
            + "blobParts="
            + blobParts
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
     * BlobPart represents a single chunk of a file
     */
    public static class BlobPart {
        private String blobName;
        private long position;
        private long length;

        public BlobPart(String blobName, long position, long length) {
            this.blobName = blobName;
            if (length <= 0) {
                throw new IllegalArgumentException("Length for blob part fetch request needs to be non-negative");
            }
            this.length = length;
            this.position = position;
        }

        public String getBlobName() {
            return blobName;
        }

        public long getPosition() {
            return position;
        }

        public long getLength() {
            return length;
        }
    }

    /**
     * Builder for BlobFetchRequest
     */
    public static final class Builder {
        private List<BlobPart> blobParts;
        private FSDirectory directory;
        private String fileName;

        private Builder() {}

        public Builder directory(FSDirectory directory) {
            this.directory = directory;
            return this;
        }

        public Builder fileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public Builder blobParts(List<BlobPart> blobParts) {
            this.blobParts = blobParts;
            return this;
        }

        public BlobFetchRequest build() {
            return new BlobFetchRequest(this);
        }
    }
}
