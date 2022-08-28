/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.Nullable;

import java.nio.file.Path;
import java.util.Objects;

public class FileSnapshot {

    private final long checksum;
    private final byte[] content;
    private final String name;
    private final long contentLength;
    @Nullable
    private long primaryTerm;
    @Nullable
    private Path path;

    public FileSnapshot(String name, Path path, long checksum, byte[] content, long primaryTerm) {
        this.name = name;
        this.path = path;
        this.checksum = checksum;
        this.content = content;
        this.contentLength = content.length;
        this.primaryTerm = primaryTerm;
    }

    public FileSnapshot(String name, long checksum, byte[] content) {
        this.name = name;
        this.checksum = checksum;
        this.content = content;
        this.contentLength = content.length;
    }

    public Path getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public byte[] getContent() {
        return content;
    }

    public long getChecksum() {
        return checksum;
    }

    public long getContentLength() {
        return contentLength;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, checksum, contentLength);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileSnapshot other = (FileSnapshot) o;
        return Objects.equals(this.name, other.name)
            && Objects.equals(this.path, other.path)
            && Objects.equals(this.checksum, other.checksum)
            && Objects.equals(this.contentLength, other.contentLength)
            && Objects.equals(this.primaryTerm, other.primaryTerm);
    }

    @Override
    public String toString() {
        return new StringBuilder("FileInfo [").append(name).append(path.toUri()).append(checksum).append(contentLength).toString();
    }

    public static class TranslogFileSnapshot extends FileSnapshot {

        private final long generation;

        public TranslogFileSnapshot(long primaryTerm, long generation, String name, Path path, long checksum, byte[] content) {
            super(name, path, checksum, content, primaryTerm);
            this.generation = generation;
        }

        public long getGeneration() {
            return generation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, super.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o)) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                TranslogFileSnapshot other = (TranslogFileSnapshot) o;
                return Objects.equals(this.generation, other.generation);
            }
            return false;
        }
    }

    public static class CheckpointFileSnapshot extends FileSnapshot {

        private final long minTranslogGeneration;

        public CheckpointFileSnapshot(long primaryTerm, long minTranslogGeneration, String name, Path path, long checksum, byte[] content) {
            super(name, path, checksum, content, primaryTerm);
            this.minTranslogGeneration = minTranslogGeneration;
        }

        public long getMinTranslogGeneration() {
            return minTranslogGeneration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minTranslogGeneration, super.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o)) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                CheckpointFileSnapshot other = (CheckpointFileSnapshot) o;
                return Objects.equals(this.minTranslogGeneration, other.minTranslogGeneration);
            }
            return false;
        }
    }
}
