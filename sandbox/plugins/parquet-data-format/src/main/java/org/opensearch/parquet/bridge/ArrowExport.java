/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/**
 * RAII container for Arrow C Data Interface exports.
 *
 * <p>Wraps an {@link ArrowArray} and {@link ArrowSchema} allocated via the Arrow C Data Interface,
 * providing memory address accessors for JNI handoff and automatic resource cleanup on close.
 * Used by {@link org.opensearch.parquet.vsr.ManagedVSR} to export data and schema to the native
 * Parquet writer without copying.
 *
 * <p>Implements {@link AutoCloseable} to ensure native Arrow memory is released and freed
 * even if an exception occurs during the write path.
 */
public record ArrowExport(ArrowArray arrowArray, ArrowSchema arrowSchema) implements AutoCloseable {

    /**
     * Creates a new ArrowExport.
     *
     * @param arrowArray  the Arrow array to export
     * @param arrowSchema the Arrow schema to export
     */
    public ArrowExport {
    }

    /** Returns the arrow array. */
    @Override
    public ArrowArray arrowArray() {
        return arrowArray;
    }

    /** Returns the arrow schema. */
    @Override
    public ArrowSchema arrowSchema() {
        return arrowSchema;
    }

    /**
     * Returns the memory address of the Arrow array.
     *
     * @return the native memory address of the array
     */
    public long getArrayAddress() {
        return arrowArray.memoryAddress();
    }

    /**
     * Returns the memory address of the Arrow schema.
     *
     * @return the native memory address of the schema
     */
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
