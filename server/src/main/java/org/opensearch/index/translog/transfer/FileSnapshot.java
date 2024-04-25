/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.common.util.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Snapshot of a single file that gets transferred
 *
 * @opensearch.internal
 */
public class FileSnapshot implements Closeable {

    private final String name;
    @Nullable
    private final FileChannel fileChannel;

    @Nullable
    private Path path;
    @Nullable
    private byte[] content;

    private FileSnapshot(Path path) throws IOException {
        Objects.requireNonNull(path);
        this.name = path.getFileName().toString();
        this.path = path;
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
    }

    private FileSnapshot(String name, byte[] content) {
        Objects.requireNonNull(name);
        this.name = name;
        this.content = content;
        this.fileChannel = null;
    }

    public Path getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public long getContentLength() throws IOException {
        return fileChannel == null ? content.length : fileChannel.size();
    }

    public InputStream inputStream() throws IOException {
        return fileChannel != null
            ? new BufferedInputStream(Channels.newInputStream(fileChannel))
            : new InputStreamIndexInput(new ByteArrayIndexInput(this.name, content), content.length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, content, path);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileSnapshot other = (FileSnapshot) o;
        return Objects.equals(this.name, other.name) && Arrays.equals(this.content, other.content) && Objects.equals(this.path, other.path);
    }

    @Override
    public String toString() {
        return new StringBuilder("FileInfo [").append(" name = ")
            .append(name)
            .append(", path = ")
            .append(path.toUri())
            .append("]")
            .toString();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(fileChannel);
    }

    /**
     * Snapshot of a single file with primary term that gets transferred
     *
     * @opensearch.internal
     */
    public static class TransferFileSnapshot extends FileSnapshot {

        private final long primaryTerm;
        private Long checksum;
        @Nullable
        private long generation;
        @Nullable
        private long minTranslogGeneration;
        @Nullable
        private Path ckpFilePath;
        @Nullable
        private Long ckpFileChecksum;
        @Nullable
        private Map<String, String> metadata;

        public TransferFileSnapshot(Path path, long primaryTerm, long generation, Long checksum) throws IOException {
            super(path);
            this.primaryTerm = primaryTerm;
            this.checksum = checksum;
            this.generation = generation;
        }

        public TransferFileSnapshot(
            long primaryTerm,
            long generation,
            long minTranslogGeneration,
            Path path,
            Long checksum,
            Path ckpFilePath,
            Long ckpFileChecksum
        ) throws IOException {
            super(path);
            this.primaryTerm = primaryTerm;
            this.checksum = checksum;
            this.generation = generation;
            this.minTranslogGeneration = minTranslogGeneration;
            this.ckpFilePath = ckpFilePath;
            this.ckpFileChecksum = ckpFileChecksum;
        }

        public TransferFileSnapshot(String name, byte[] content, long primaryTerm) throws IOException {
            super(name, content);
            this.primaryTerm = primaryTerm;
        }

        public CheckpointFileSnapshot provideCheckpointFileSnapshot() throws IOException {
            return new CheckpointFileSnapshot(primaryTerm, generation, minTranslogGeneration, ckpFilePath, ckpFileChecksum);
        }

        public Long getChecksum() {
            return checksum;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        public Map<String, String> getTransferFileSnapshotMetadata() {
            return metadata;
        }

        public long getGeneration() {
            return generation;
        }

        public long getMinTranslogGeneration() {
            return minTranslogGeneration;
        }

        public Path getCkpFilePath() {
            return ckpFilePath;
        }

        public Long getCkpFileChecksum() {
            return ckpFileChecksum;
        }

        public long getCkpFileContentLength() throws IOException {
            FileChannel fileChannel = FileChannel.open(ckpFilePath, StandardOpenOption.READ);
            long ckpFileSize = fileChannel.size();
            fileChannel.close();
            return ckpFileSize;
        }

        public String getCkpFileName() {
            return ckpFilePath.getFileName().toString();
        }

        public void setTransferFileSnapshotMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
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

    /**
     * Snapshot of a single .ckp file that gets transferred
     *
     * @opensearch.internal
     */
    public static final class CheckpointFileSnapshot extends TransferFileSnapshot {

        private final long generation;

        private final long minTranslogGeneration;

        public CheckpointFileSnapshot(long primaryTerm, long generation, long minTranslogGeneration, Path path, Long checksum)
            throws IOException {
            super(path, primaryTerm, generation, checksum);
            this.minTranslogGeneration = minTranslogGeneration;
            this.generation = generation;
        }

        public long getGeneration() {
            return generation;
        }

        public long getMinTranslogGeneration() {
            return minTranslogGeneration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, minTranslogGeneration, super.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o)) {
                if (this == o) return true;
                if (getClass() != o.getClass()) return false;
                CheckpointFileSnapshot other = (CheckpointFileSnapshot) o;
                return Objects.equals(this.minTranslogGeneration, other.minTranslogGeneration)
                    && Objects.equals(this.generation, other.generation);
            }
            return false;
        }
    }

    /**
     * Single snapshot of combined translog.tlog and translog.ckp files that gets transferred
     *
     * @opensearch.internal
     */
    public static final class TranslogAndCheckpointFileSnapshot extends TransferFileSnapshot {

        private final long primaryTerm;
        private final long generation;
        private final long minTranslogGeneration;
        private final long ckpGeneration;

        public TranslogAndCheckpointFileSnapshot(
            long primaryTerm,
            long generation,
            long minTranslogGeneration,
            Path tlogFilePath,
            Long tlogFilechecksum,
            Path ckpFilePath,
            Long ckpFileChecksum,
            Long ckpGeneration
        ) throws IOException {
            super(primaryTerm, generation, minTranslogGeneration, tlogFilePath, tlogFilechecksum, ckpFilePath, ckpFileChecksum);
            this.primaryTerm = primaryTerm;
            this.generation = generation;
            this.minTranslogGeneration = minTranslogGeneration;
            this.ckpGeneration = ckpGeneration;
        }

        public long getGeneration() {
            return generation;
        }

        public long getMinTranslogGeneration() {
            return minTranslogGeneration;
        }

        public long getCkpGeneration() {
            return ckpGeneration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryTerm, generation, super.hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (super.equals(o)) {
                if (this == o) return true;
                if (getClass() != o.getClass()) return false;
                TranslogAndCheckpointFileSnapshot other = (TranslogAndCheckpointFileSnapshot) o;
                return Objects.equals(this.primaryTerm, other.primaryTerm) && Objects.equals(this.generation, other.generation);
            }
            return false;
        }

    }

}
