/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/**
 * Container for Arrow C Data Interface exports.
 * Provides a safe wrapper around ArrowArray and ArrowSchema with proper resource management.
 */
public record ArrowExport(ArrowArray arrowArray, ArrowSchema arrowSchema) implements AutoCloseable {

    public long getArrayAddress() {
        return arrowArray.memoryAddress();
    }

    public long getSchemaAddress() {
        return arrowSchema.memoryAddress();
    }

    @Override
    public void close() {
        if (arrowArray != null) {
            arrowArray.release();
            arrowArray.close();
        }
        if (arrowSchema != null) {
            arrowSchema.release();
            arrowSchema.close();
        }
    }
}
