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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
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
        private Path metadataFilePath;
        @Nullable
        private Long metadataFileChecksum;
        @Nullable
        private String metadataFileName;

        public TransferFileSnapshot(Path path, long primaryTerm, Long checksum, Path metadataFilePath, Long metadataFileChecksum)
            throws IOException {
            super(path);
            this.primaryTerm = primaryTerm;
            this.checksum = checksum;
            Objects.requireNonNull(metadataFilePath);
            this.metadataFilePath = metadataFilePath;
            this.metadataFileChecksum = metadataFileChecksum;
            this.metadataFileName = metadataFilePath.getFileName().toString();
        }

        public TransferFileSnapshot(String name, byte[] content, long primaryTerm) throws IOException {
            super(name, content);
            this.primaryTerm = primaryTerm;
        }

        public TransferFileSnapshot(Path path, long primaryTerm, Long checksum) throws IOException {
            super(path);
            this.primaryTerm = primaryTerm;
            this.checksum = checksum;
        }

        public Long getChecksum() {
            return checksum;
        }

        public long getPrimaryTerm() {
            return primaryTerm;
        }

        public Path getMetadataFilePath() {
            return metadataFilePath;
        }

        public Long getMetadataFileChecksum() {
            return metadataFileChecksum;
        }

        public long getMinTranslogGeneration() {
            return getMinTranslogGeneration();
        }

        public String getMetadataFileName() {
            return metadataFileName;
        }

        public Map<String, String> prepareFileMetadata() throws IOException {
            if (!(this instanceof TranslogCheckpointFileSnapshot)) {
                return null;
            }

            String ckpAsString = provideCheckpointDataAsString();
            Long checkpointChecksum = metadataFileChecksum;

            assert checkpointChecksum != null : "checksum can not be null";

            Map<String, String> metadata = new HashMap<>();
            metadata.put(TranslogFileSnapshot.CHECKPOINT_FILE_DATA_KEY, ckpAsString);
            metadata.put(TranslogFileSnapshot.CHECKPOINT_FILE_CHECKSUM_KEY, checkpointChecksum.toString());
            return metadata;
        }

        public String provideCheckpointDataAsString() throws IOException {
            return buildCheckpointDataAsBase64String(metadataFilePath);
        }

        static String buildCheckpointDataAsBase64String(Path checkpointFilePath) throws IOException {
            long fileSize = Files.size(checkpointFilePath);
            assert fileSize < 1500 : "checkpoint file size is more than 1.5KB size, can't be stored as metadata";
            byte[] fileBytes = Files.readAllBytes(checkpointFilePath);
            return Base64.getEncoder().encodeToString(fileBytes);
        }

        public static byte[] convertBase64StringToCheckpointFileDataBytes(String base64CheckpointString) {
            if (base64CheckpointString == null) {
                return null;
            }
            return Base64.getDecoder().decode(base64CheckpointString);
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
     * Snapshot of a single .tlg file that gets transferred
     *
     * @opensearch.internal
     */
    public static final class TranslogFileSnapshot extends TransferFileSnapshot {

        private final long generation;
        public final static String CHECKPOINT_FILE_DATA_KEY = "ckp-data";
        public final static String CHECKPOINT_FILE_CHECKSUM_KEY = "ckp-checksum";

        public TranslogFileSnapshot(long primaryTerm, long generation, Path path, Long checksum) throws IOException {
            super(path, primaryTerm, checksum);
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
            super(path, primaryTerm, checksum);
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
    public static final class TranslogCheckpointFileSnapshot extends TransferFileSnapshot {

        private final long generation;
        private final long minTranslogGeneration;

        public TranslogCheckpointFileSnapshot(
            Path tlogFilepath,
            long primaryTerm,
            Long tlogFilechecksum,
            long generation,
            long minTranslogGeneration,
            Path ckpFilePath,
            Long ckpFileChecksum
        ) throws IOException {
            super(tlogFilepath, primaryTerm, tlogFilechecksum, ckpFilePath, ckpFileChecksum);
            this.minTranslogGeneration = minTranslogGeneration;
            this.generation = generation;
        }

        public long getGeneration() {
            return generation;
        }

    }
}
