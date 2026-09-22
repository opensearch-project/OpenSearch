/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/**
 * RAII container for Arrow C Data Interface exports.
 *
 * <p>Wraps an {@link ArrowArray} and {@link ArrowSchema} allocated via the Arrow C Data Interface,
 * providing memory address accessors for JNI handoff and automatic resource cleanup on close.
 *
 * @param arrowArray  the Arrow array (may be null for schema-only exports)
 * @param arrowSchema the Arrow schema
 */
public record ArrowExport(ArrowArray arrowArray, ArrowSchema arrowSchema) implements AutoCloseable {

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
