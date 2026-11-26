/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.KeyedLock;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.IngestionWriter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * File-based writer for testing ingestion. Appends each message as one line to ${baseDir}/${stream}/${partition}.ndjson,
 * the file {@link FilePartitionConsumer} reads for that shard, and reports the message's line number as its offset.
 * Writers in the same JVM share a lock per file and count its lines on every write, so several writers can append to
 * the same partition; other processes must not write these files.
 */
public class FileIngestionWriter implements IngestionWriter {

    // one lock per file, shared by all writers in the JVM
    private static final KeyedLock<Path> FILE_LOCKS = new KeyedLock<>();

    private final Path streamDir;

    /**
     * Initialize a FileIngestionWriter.
     * @param config the file source config
     */
    public FileIngestionWriter(FileSourceConfig config) {
        this.streamDir = Path.of(config.getBaseDirectory(), config.getStream());
    }

    @Override
    public void write(int partition, String key, byte[] message, ActionListener<IngestionShardPointer> listener) {
        final FileOffset offset;
        try {
            offset = append(partition, message);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(offset);
    }

    private FileOffset append(int partition, byte[] message) throws IOException {
        for (byte b : message) {
            if (b == '\n' || b == '\r') {
                throw new IllegalArgumentException("message must be a single line");
            }
        }
        Path file = streamDir.resolve(partition + ".ndjson").toAbsolutePath().normalize();
        try (Releasable ignored = FILE_LOCKS.acquire(file)) {
            Files.createDirectories(streamDir);
            long line = countLines(file);
            byte[] lineBytes = new byte[message.length + 1];
            System.arraycopy(message, 0, lineBytes, 0, message.length);
            lineBytes[message.length] = '\n';
            Files.write(file, lineBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            return new FileOffset(line);
        }
    }

    // counts newline bytes, so the count does not depend on the encoding; a last line without a
    // newline is terminated first, so the next message starts on its own line
    private static long countLines(Path file) {
        if (Files.exists(file) == false) {
            return 0;
        }
        try {
            long newlines = 0;
            int last = '\n';
            try (InputStream in = new BufferedInputStream(Files.newInputStream(file))) {
                int b;
                while ((b = in.read()) != -1) {
                    if (b == '\n') {
                        newlines++;
                    }
                    last = b;
                }
            }
            if (last != '\n') {
                Files.write(file, new byte[] { '\n' }, StandardOpenOption.APPEND);
                newlines++;
            }
            return newlines;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {}
}
