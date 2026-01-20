/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import com.parquet.parquetdataformat.ParquetSettings;
import org.opensearch.index.IndexSettings;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for native Parquet writer with lifecycle management.
 */
public class NativeParquetWriter implements Closeable {

    private final AtomicBoolean writerClosed = new AtomicBoolean(false);
    private final String filePath;

    /**
     * Creates a new native Parquet writer.
     * @param filePath path to the Parquet file
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @param indexSettings index-level settings
     * @throws IOException if writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress, IndexSettings indexSettings) throws IOException {
        this.filePath = filePath;

        // Build writer configuration from index settings
        WriterConfig config = new WriterConfig();
        config.setCompressionType(indexSettings.getValue(ParquetSettings.COMPRESSION_TYPE));
        config.setCompressionLevel(indexSettings.getValue(ParquetSettings.COMPRESSION_LEVEL));
        config.setPageSize(indexSettings.getValue(ParquetSettings.PAGE_SIZE_BYTES).getBytes());
        config.setPageRowLimit(indexSettings.getValue(ParquetSettings.PAGE_ROW_LIMIT));
        config.setDictSizeBytes(indexSettings.getValue(ParquetSettings.DICT_SIZE_BYTES).getBytes());

        try {
            RustBridge.createWriter(filePath, schemaAddress, config.toJson());
        } catch (Exception e) {
            throw new IOException("Failed to create Parquet writer", e);
        }
    }

    /**
     * Writes a batch to the Parquet file.
     * @param arrayAddress Arrow C Data Interface array pointer
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @throws IOException if write fails
     */
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        RustBridge.write(filePath, arrayAddress, schemaAddress);
    }

    /**
     * Flushes buffered data to disk.
     * @throws IOException if flush fails
     */
    public void flush() throws IOException {
        RustBridge.flushToDisk(filePath);
    }

    private ParquetFileMetadata metadata;

    @Override
    public void close() {
        if (writerClosed.compareAndSet(false, true)) {
            try {
                metadata = RustBridge.closeWriter(filePath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to close Parquet writer for " + filePath, e);
            }
        }
    }

    public ParquetFileMetadata getMetadata() {
        return metadata;
    }

    public String getFilePath() {
        return filePath;
    }
}
