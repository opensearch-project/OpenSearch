/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.engine;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Interface for pluggable execution engines.
 *
 * <p>Implementations execute a serialized plan fragment using a native engine
 * (e.g., DataFusion) and return results as Arrow.
 */
public interface ExecutionEngine {

    /**
     * Execute a plan fragment and return results.
     *
     * @param planBytes serialized plan fragment (e.g., Substrait protobuf bytes), may be null for stub implementations
     * @param allocator Arrow memory allocator
     * @return query results as Arrow VectorSchemaRoot
     */
    VectorSchemaRoot execute(byte[] planBytes, BufferAllocator allocator);
}
