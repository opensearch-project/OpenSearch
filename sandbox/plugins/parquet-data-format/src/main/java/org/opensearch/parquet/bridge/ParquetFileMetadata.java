/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

/**
 * Metadata extracted from a Parquet file after writer close.
 */
public record ParquetFileMetadata(int version, long numRows, String createdBy) {

    @Override
    public String toString() {
        return "ParquetFileMetadata{version=" + version + ", numRows=" + numRows + ", createdBy='" + createdBy + "'}";
    }
}
