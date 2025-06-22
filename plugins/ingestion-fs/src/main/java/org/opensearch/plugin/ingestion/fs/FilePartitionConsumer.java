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
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * File-based consumer for testing ingestion. Reads from ${baseDir}/${topic}/${shardId}.ndjson.
 */
@SuppressForbidden(reason = "uses user provided path to read files for local testing")
public class FilePartitionConsumer implements IngestionShardConsumer<FileOffset, FileMessage> {
    private static final Logger logger = LogManager.getLogger(FilePartitionConsumer.class);

    private final Path shardFilePath;
    private final int shardId;

    private BufferedReader reader = null;
    private long currentLineInReader = 0;
    private long lastReadLine = -1;

    public FilePartitionConsumer(FileSourceConfig config, int shardId) {
        String baseDir = config.getBaseDirectory();
        String topic = config.getTopic();

        this.shardFilePath = PathUtils.get(baseDir, topic, shardId + ".ndjson");
        this.shardId = shardId;

        if (!Files.exists(shardFilePath)) {
            logger.warn("FilePartitionConsumer: File {} does not exist.", shardFilePath.toAbsolutePath());
        } else {
            logger.info("Initialized FilePartitionConsumer for shard {} with file: {}", shardId, shardFilePath.toAbsolutePath());
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

        if (!Files.exists(shardFilePath)) {
            return results;
        }

        try {
            if (reader == null) {
                reader = Files.newBufferedReader(shardFilePath, StandardCharsets.UTF_8);
                currentLineInReader = 0;
            }

            if (startLine < currentLineInReader) {
                reader.close();
                reader = Files.newBufferedReader(shardFilePath, StandardCharsets.UTF_8);
                currentLineInReader = 0;
            }

            while (currentLineInReader < startLine && reader.readLine() != null) {
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
            throw new RuntimeException("Failed to read from file: " + shardFilePath.toAbsolutePath(), e);
        }

        return results;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new FileOffset(0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        if (!Files.exists(shardFilePath)) {
            return new FileOffset(0);
        }

        try (
            LineNumberReader lineNumberReader = new LineNumberReader(
                new InputStreamReader(Files.newInputStream(shardFilePath), StandardCharsets.UTF_8)
            )
        ) {
            lineNumberReader.skip(Long.MAX_VALUE);
            return new FileOffset(lineNumberReader.getLineNumber());
        } catch (IOException e) {
            throw new RuntimeException("Failed to compute latest pointer", e);
        }
    }

    /**
     * Timestamp based pointer is not supported in file mode. Defaults to earliest pointer.
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
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
