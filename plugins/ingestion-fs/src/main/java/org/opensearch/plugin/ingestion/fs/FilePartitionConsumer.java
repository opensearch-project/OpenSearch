/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * File-based consumer for testing ingestion. Reads from ${baseDir}/${stream}/${shardId}.ndjson.
 */
public class FilePartitionConsumer implements IngestionShardConsumer<FileOffset, FileMessage> {
    private static final Logger logger = LogManager.getLogger(FilePartitionConsumer.class);

    private final Path shardFile;
    private final int shardId;

    private BufferedReader reader = null;
    private long currentLineInReader = 0;
    private long lastReadLine = -1;

    /**
     * Initialize a FilePartitionConsumer that will read messages from provided file and index documents.
     * @param config the file source config
     * @param shardId shard ID
     */
    public FilePartitionConsumer(FileSourceConfig config, int shardId) {
        String baseDir = config.getBaseDirectory();
        String stream = config.getStream();
        this.shardFile = Path.of(baseDir, stream, shardId + ".ndjson");
        this.shardId = shardId;

        if (!Files.exists(shardFile)) {
            logger.warn("FilePartitionConsumer: File {} does not exist.", shardFile.toAbsolutePath());
        } else {
            logger.info("Initialized FilePartitionConsumer for shard {} with file: {}", shardId, shardFile.toAbsolutePath());
        }
    }

    @Override
    public List<ReadResult<FileOffset, FileMessage>> readNext(FileOffset offset, boolean includeStart, long maxMessages, int timeoutMillis)
        throws TimeoutException {
        long startLine = includeStart ? offset.getLine() : offset.getLine() + 1;
        return readFromFile(startLine, maxMessages);
    }

    @Override
    public List<ReadResult<FileOffset, FileMessage>> readNext(long maxMessages, int timeoutMillis) throws TimeoutException {
        return readFromFile(lastReadLine + 1, maxMessages);
    }

    private synchronized List<ReadResult<FileOffset, FileMessage>> readFromFile(long startLine, long maxLines) throws TimeoutException {
        List<ReadResult<FileOffset, FileMessage>> results = new ArrayList<>();

        if (!Files.exists(shardFile)) {
            return results;
        }

        try {
            if (reader == null) {
                reader = Files.newBufferedReader(shardFile);
                currentLineInReader = 0;
            }

            if (startLine < currentLineInReader) {
                reader.close();
                reader = Files.newBufferedReader(shardFile);
                currentLineInReader = 0;
            }

            while (currentLineInReader < startLine && reader.readLine() != null) {
                lastReadLine = currentLineInReader;
                currentLineInReader++;
            }

            String line;
            while (results.size() < maxLines && (line = reader.readLine()) != null) {
                FileOffset offset = new FileOffset(currentLineInReader);
                FileMessage message = new FileMessage(line.getBytes(StandardCharsets.UTF_8), System.currentTimeMillis());
                results.add(new ReadResult<>(offset, message));
                lastReadLine = currentLineInReader;
                currentLineInReader++;
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to read from file: " + shardFile.toAbsolutePath(), e);
        }

        return results;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new FileOffset(0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        if (!Files.exists(shardFile)) {
            return new FileOffset(0);
        }

        try (LineNumberReader lineNumberReader = new LineNumberReader(Files.newBufferedReader(shardFile))) {
            while (lineNumberReader.readLine() != null) {
                // do nothing
            }
            return new FileOffset(lineNumberReader.getLineNumber());
        } catch (IOException e) {
            throw new RuntimeException("Failed to compute latest pointer", e);
        }
    }

    /**
     * Timestamp-based pointer is not supported in file mode. Defaults to earliest pointer.
     * @param timestampMillis the timestamp in milliseconds
     * @return shard pointer
     */
    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        return earliestPointer();
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return new FileOffset(Long.parseLong(offset));
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        if (!Files.exists(shardFile)) {
            return 0;
        }

        FileOffset latestOffset = (FileOffset) latestPointer();
        if (lastReadLine < 0) {
            // Haven't read anything yet, use the expected start pointer
            long startLine = ((FileOffset) expectedStartPointer).getLine();
            return Math.max(0, latestOffset.getLine() - startLine);
        }
        // return lag as number of remaining lines from lastReadLineNumber
        return latestOffset.getLine() - lastReadLine - 1;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
