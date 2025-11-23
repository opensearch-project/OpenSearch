/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import java.io.IOException;

/**
 * Type-safe handle for native Parquet writer with lifecycle management.
 */
public class NativeParquetWriter implements AutoCloseable {

    private final String filePath;

    /**
     * Creates a new native Parquet writer.
     * @param filePath path to the Parquet file
     * @param schemaAddress Arrow C Data Interface schema pointer
     * @throws IOException if writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress) throws IOException {
        this.filePath = filePath;
        RustBridge.createWriter(filePath, schemaAddress);
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

    @Override
    public void close() {
        try {
            RustBridge.closeWriter(filePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to close Parquet writer for " + filePath, e);
        }
    }

    public String getFilePath() {
        return filePath;
    }
}
