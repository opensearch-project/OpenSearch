/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Manages the lifecycle between the decomposer and the
 * {@link IndexFilterBridge}. Registers a context at construction
 * time and ensures cleanup when closed.
 * <p>
 * Typical usage:
 * <pre>{@code
 * try (IndexFilterDelegate delegate = new IndexFilterDelegate(columnToFormat, providers)) {
 *     long contextId = delegate.getContextId();
 *     // pass contextId to JNI for Substrait-driven tree query execution
 * }
 * }</pre>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class IndexFilterDelegate implements Closeable {

    private final long contextId;
    private volatile boolean closed = false;

    /**
     * Creates a delegate, registering a context with the bridge.
     *
     * @param columnToFormat mapping from column name to owning DataFormat
     * @param providers      mapping from DataFormat to provider implementation
     */
    public IndexFilterDelegate(
        Map<String, DataFormat> columnToFormat,
        Map<DataFormat, IndexFilterCollectorProvider> providers
    ) {
        this.contextId = IndexFilterBridge.createContext(columnToFormat, providers);
    }

    /**
     * Returns the context ID for passing to JNI.
     *
     * @return the context ID
     */
    public long getContextId() {
        return contextId;
    }

    /**
     * Unregisters the context from the bridge. Safe to call multiple times;
     * only the first call has effect.
     */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            IndexFilterBridge.unregister(contextId);
        }
    }
}
