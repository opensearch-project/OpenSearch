/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.opensearch.plugins.ExtensiblePlugin;

/**
 * A reference {@link ArrowSource} backed by a single Arrow IPC (file format) stream, read
 * entirely into memory. Bundled with the plugin as a minimal, generic, always-available data
 * source — useful for local experimentation, and for tests that generate Arrow IPC bytes on the
 * fly rather than exercising an org-specific {@link ArrowSourceFactory}.
 *
 * <p>Not registered via the {@link ExtensiblePlugin} SPI mechanism (a plugin cannot discover its
 * own bundled providers that way); instead {@link ArrowIpcSourceFactory} is registered directly by
 * {@link ArrowPlugin}.
 */
public final class ArrowIpcSource implements ArrowSource {

    private final BufferAllocator allocator;
    private final VectorSchemaRoot rows;
    private final Long versionTimestamp;
    private final String version;

    private ArrowIpcSource(
            BufferAllocator allocator, VectorSchemaRoot rows, Long versionTimestamp, String version) {
        this.allocator = allocator;
        this.rows = rows;
        this.versionTimestamp = versionTimestamp;
        this.version = version;
    }

    /**
     * Reads the whole Arrow IPC file at {@code path} into memory. The file's last-modified time is
     * used as the version timestamp, and the path itself as the version string.
     *
     * @param path the Arrow IPC (file format) file to read
     */
    public static ArrowIpcSource fromPath(Path path) throws IOException {
        long lastModifiedEpochSeconds = Files.getLastModifiedTime(path).toInstant().getEpochSecond();
        try (SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ)) {
            return fromChannel(channel, lastModifiedEpochSeconds, path.toString());
        }
    }

    /**
     * Reads the whole Arrow IPC stream from {@code channel} into memory. Exposed separately from
     * {@link #fromPath} so tests can generate Arrow IPC bytes on the fly (e.g. via {@code
     * ByteArrayReadableSeekableByteChannel}) without going through the filesystem.
     *
     * @param channel the Arrow IPC (file format) stream to read
     * @param versionTimestamp version timestamp to report via {@link #getVersionTimestamp()}
     * @param version version string to report via {@link #getVersion()}
     */
    public static ArrowIpcSource fromChannel(SeekableByteChannel channel, Long versionTimestamp, String version)
            throws IOException {
        BufferAllocator allocator = new RootAllocator();
        try (ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            Schema schema = reader.getVectorSchemaRoot().getSchema();
            VectorSchemaRoot rows = VectorSchemaRoot.create(schema, allocator);
            // VectorSchemaRoot.create() leaves vectors unallocated (zero-capacity buffers);
            // VectorSchemaRootAppender needs an already-allocated target to append into.
            rows.allocateNew();
            while (reader.loadNextBatch()) {
                VectorSchemaRootAppender.append(false, rows, reader.getVectorSchemaRoot());
            }
            return new ArrowIpcSource(allocator, rows, versionTimestamp, version);
        } catch (Throwable t) {
            allocator.close();
            throw t;
        }
    }

    @Override
    public long getRowCount() {
        return rows.getRowCount();
    }

    @Override
    public ArrowReader getReader(long start, long end) {
        if (start < 0 || end < start || end > rows.getRowCount()) {
            throw new IllegalArgumentException(
                    "Invalid row range [" + start + ", " + end + ") for a source with " + rows.getRowCount() + " rows");
        }
        return new RowRangeReader(allocator, rows, start, end);
    }

    @Override
    public Long getVersionTimestamp() {
        return versionTimestamp;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public void close() {
        rows.close();
        allocator.close();
    }

    /**
     * A one-shot {@link ArrowReader} that serves a single batch: the {@code [start, end)} row slice
     * of an in-memory {@link VectorSchemaRoot}, copied into the reader's own root on the first call
     * to {@link #loadNextBatch()}.
     */
    private static final class RowRangeReader extends ArrowReader {

        private final VectorSchemaRoot source;
        private final long start;
        private final long end;
        private boolean served;

        RowRangeReader(BufferAllocator allocator, VectorSchemaRoot source, long start, long end) {
            super(allocator);
            this.source = source;
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
            if (served) {
                return false;
            }
            served = true;
            VectorSchemaRoot target = getVectorSchemaRoot();
            int len = (int) (end - start);
            List<FieldVector> sourceVectors = source.getFieldVectors();
            List<FieldVector> targetVectors = target.getFieldVectors();
            for (int col = 0; col < sourceVectors.size(); col++) {
                FieldVector sourceVector = sourceVectors.get(col);
                FieldVector targetVector = targetVectors.get(col);
                targetVector.allocateNew();
                for (int i = 0; i < len; i++) {
                    targetVector.copyFromSafe((int) start + i, i, sourceVector);
                }
            }
            target.setRowCount(len);
            return true;
        }

        @Override
        public long bytesRead() {
            return 0;
        }

        @Override
        protected void closeReadSource() {
            // The underlying VectorSchemaRoot/allocator are owned by the enclosing ArrowIpcSource.
        }

        @Override
        protected Schema readSchema() {
            return source.getSchema();
        }
    }
}
