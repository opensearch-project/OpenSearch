/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

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
import java.util.HashMap;
import java.util.Map;

/**
 * File-based writer for testing ingestion. Appends each message as one line to ${baseDir}/${stream}/${partition}.ndjson,
 * the file {@link FilePartitionConsumer} reads for that shard, and reports the message's line number as its offset.
 * Assumes it is the only writer of these files.
 */
public class FileIngestionWriter implements IngestionWriter {

    private final Path streamDir;
    // next line number per partition, counted from the file on first use
    private final Map<Integer, Long> nextLines = new HashMap<>();

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

    private synchronized FileOffset append(int partition, byte[] message) throws IOException {
        for (byte b : message) {
            if (b == '\n' || b == '\r') {
                throw new IllegalArgumentException("message must be a single line");
            }
        }
        Path file = streamDir.resolve(partition + ".ndjson");
        long line = nextLines.computeIfAbsent(partition, p -> countLines(file));
        Files.createDirectories(streamDir);
        byte[] lineBytes = new byte[message.length + 1];
        System.arraycopy(message, 0, lineBytes, 0, message.length);
        lineBytes[message.length] = '\n';
        Files.write(file, lineBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        nextLines.put(partition, line + 1);
        return new FileOffset(line);
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
