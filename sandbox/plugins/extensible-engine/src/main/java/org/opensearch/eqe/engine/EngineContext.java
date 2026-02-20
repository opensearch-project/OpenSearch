/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.engine;

import org.apache.arrow.memory.BufferAllocator;

/**
 * Holds the execution engine context: the discovered {@link ExecutionEngine}
 * and the Arrow {@link BufferAllocator}. Returned from
 * {@code ExtensibleEnginePlugin.createComponents()} so Guice can inject it
 * into transport actions.
 */
public class EngineContext {

    private final ExecutionEngine executor;
    private final BufferAllocator allocator;

    public EngineContext(ExecutionEngine executor, BufferAllocator allocator) {
        this.executor = executor;
        this.allocator = allocator;
    }

    /** May be null if no engine plugin was loaded. */
    public ExecutionEngine getExecutor() {
        return executor;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }
}
