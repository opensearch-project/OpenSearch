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
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * File-based consumer for testing ingestion. Reads from ${baseDir}/${stream}/${shardId}.ndjson.
 */
@SuppressForbidden(reason = "using Java file APIs for local testing purpose")
public class FilePartitionConsumer implements IngestionShardConsumer<FileOffset, FileMessage> {
    private static final Logger logger = LogManager.getLogger(FilePartitionConsumer.class);

    private final File shardFile;
    private final int shardId;

    private BufferedReader reader = null;
    private long currentLineInReader = 0;
    private long lastReadLine = -1;
    // the start of a line whose newline has not been written yet
    private final StringBuilder partialLine = new StringBuilder();

    /**
     * Initialize a FilePartitionConsumer that will read messages from provided file and index documents.
     * @param config the file source config
     * @param shardId shard ID
     */
    public FilePartitionConsumer(FileSourceConfig config, int shardId) {
        String baseDir = config.getBaseDirectory();
        String stream = config.getStream();
        this.shardFile = new File(baseDir, stream + File.separator + shardId + ".ndjson");
        this.shardId = shardId;

        if (!shardFile.exists()) {
            logger.warn("FilePartitionConsumer: File {} does not exist.", shardFile.getAbsolutePath());
        } else {
            logger.info("Initialized FilePartitionConsumer for shard {} with file: {}", shardId, shardFile.getAbsolutePath());
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

        if (!shardFile.exists()) {
            return results;
        }

        try {
            if (reader == null) {
                reader = new BufferedReader(new FileReader(shardFile));
                currentLineInReader = 0;
                partialLine.setLength(0);
            }

            if (startLine < currentLineInReader) {
                reader.close();
                reader = new BufferedReader(new FileReader(shardFile));
                currentLineInReader = 0;
                partialLine.setLength(0);
            }

            while (currentLineInReader < startLine && readCompleteLine() != null) {
                lastReadLine = currentLineInReader;
                currentLineInReader++;
            }

            String line;
            while (results.size() < maxLines && (line = readCompleteLine()) != null) {
                FileOffset offset = new FileOffset(currentLineInReader);
                FileMessage message = new FileMessage(line.getBytes(StandardCharsets.UTF_8), System.currentTimeMillis());
                results.add(new ReadResult<>(offset, message));
                lastReadLine = currentLineInReader;
                currentLineInReader++;
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to read from file: " + shardFile.getAbsolutePath(), e);
        }

        return results;
    }

    // returns the next newline-terminated line, or null at end of file; an unterminated tail is kept for the next poll
    private String readCompleteLine() throws IOException {
        int c;
        while ((c = reader.read()) != -1) {
            if (c == '\n') {
                int end = partialLine.length();
                if (end > 0 && partialLine.charAt(end - 1) == '\r') {
                    end--;
                }
                String line = partialLine.substring(0, end);
                partialLine.setLength(0);
                return line;
            }
            partialLine.append((char) c);
        }
        return null;
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new FileOffset(0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        if (!shardFile.exists()) {
            return new FileOffset(0);
        }

        // counts newline bytes, so a last line without a newline is not counted until it is complete
        try (InputStream in = new BufferedInputStream(new FileInputStream(shardFile))) {
            long lines = 0;
            int b;
            while ((b = in.read()) != -1) {
                if (b == '\n') {
                    lines++;
                }
            }
            return new FileOffset(lines);
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
        if (!shardFile.exists()) {
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
