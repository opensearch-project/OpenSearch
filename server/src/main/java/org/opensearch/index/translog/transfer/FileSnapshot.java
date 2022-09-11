/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Snapshot of a single file that gets transferred
 *
 * @opensearch.internal
 */
public class FileSnapshot {

    private final long checksum;
    private final byte[] content;
    private final String name;
    private final long contentLength;
    @Nullable
    private Path path;

    public FileSnapshot(Path path) throws IOException {
        Objects.requireNonNull(path);
        this.name = path.getFileName().toString();
        this.path = path;
        try (CheckedInputStream stream = new CheckedInputStream(Files.newInputStream(path), new CRC32())) {
            this.content = stream.readAllBytes();
            this.checksum = stream.getChecksum().getValue();
            this.contentLength = content.length;
        }
    }

    public FileSnapshot(String name, byte[] content) throws IOException {
        Objects.requireNonNull(content);
        Objects.requireNonNull(name);
        this.name = name;
        try (CheckedInputStream stream = new CheckedInputStream(new ByteArrayInputStream(content), new CRC32())) {
            this.content = stream.readAllBytes();
            this.checksum = stream.getChecksum().getValue();
            this.contentLength = content.length;
        }
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
            && Objects.equals(this.contentLength, other.contentLength);
    }

    @Override
    public String toString() {
        return new StringBuilder("FileInfo [").append(" name = ")
            .append(name)
            .append(", path = ")
            .append(path.toUri())
            .append(", checksum = ")
            .append(checksum)
            .append(", contentLength = ")
            .append(contentLength)
            .append("]")
            .toString();
    }

    public static class TransferFileSnapshot extends FileSnapshot {

        private final long primaryTerm;

        public TransferFileSnapshot(Path path, long primaryTerm) throws IOException {
            super(path);
            this.primaryTerm = primaryTerm;
        }

        public TransferFileSnapshot(String name, byte[] content, long primaryTerm) throws IOException {
            super(name, content);
            this.primaryTerm = primaryTerm;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryTerm, super.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o)) {
                if (this == o) return true;
                if (getClass() != o.getClass()) return false;
                TransferFileSnapshot other = (TransferFileSnapshot) o;
                return Objects.equals(this.primaryTerm, other.primaryTerm);
            }
            return false;
        }
    }

    public static class TranslogFileSnapshot extends TransferFileSnapshot {

        private final long generation;

        public TranslogFileSnapshot(long primaryTerm, long generation, Path path) throws IOException {
            super(path, primaryTerm);
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
                if (getClass() != o.getClass()) return false;
                TranslogFileSnapshot other = (TranslogFileSnapshot) o;
                return Objects.equals(this.generation, other.generation);
            }
            return false;
        }
    }

    public static class CheckpointFileSnapshot extends TransferFileSnapshot {

        private final long minTranslogGeneration;

        public CheckpointFileSnapshot(long primaryTerm, long minTranslogGeneration, Path path) throws IOException {
            super(path, primaryTerm);
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
                if (getClass() != o.getClass()) return false;
                CheckpointFileSnapshot other = (CheckpointFileSnapshot) o;
                return Objects.equals(this.minTranslogGeneration, other.minTranslogGeneration);
            }
            return false;
        }
    }
}
