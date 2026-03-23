/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

/**
 * Metadata extracted from a Parquet file after the native writer is closed.
 *
 * <p>Returned by {@link RustBridge#closeWriter(String)} and {@link RustBridge#getFileMetadata(String)}.
 * Contains the Parquet format version, total row count, and the creator identifier string
 * embedded in the file footer.
 *
 * @param version   Parquet format version number
 * @param numRows   total number of rows written to the file
 * @param createdBy creator string from the Parquet file footer metadata
 */
public record ParquetFileMetadata(int version, long numRows, String createdBy) {

    /**
     * Creates a new ParquetFileMetadata.
     *
     * @param version   the Parquet format version
     * @param numRows   the total number of rows
     * @param createdBy the creator string from the file footer
     */
    public ParquetFileMetadata {
    }

    /** Returns the Parquet format version. */
    @Override
    public int version() {
        return version;
    }

    /** Returns the total number of rows. */
    @Override
    public long numRows() {
        return numRows;
    }

    /** Returns the creator string from the file footer. */
    @Override
    public String createdBy() {
        return createdBy;
    }

    @Override
    public String toString() {
        return "ParquetFileMetadata{version=" + version + ", numRows=" + numRows + ", createdBy='" + createdBy + "'}";
    }
}
