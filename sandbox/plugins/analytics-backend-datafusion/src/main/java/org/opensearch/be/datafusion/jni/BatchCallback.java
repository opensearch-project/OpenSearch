/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion.jni;

/**
 * Callback interface for receiving Arrow batches from native DataFusion execution.
 * Called by Rust JNI during executeAndStream().
 */
public interface BatchCallback {

    /**
     * Called for each Arrow RecordBatch produced by the query.
     *
     * @param schemaAddr memory address of a heap-allocated FFI ArrowSchema
     * @param arrayAddr  memory address of a heap-allocated FFI ArrowArray
     */
    void onBatch(long schemaAddr, long arrayAddr);

    /**
     * Called when all batches have been streamed.
     */
    void onComplete();
}
