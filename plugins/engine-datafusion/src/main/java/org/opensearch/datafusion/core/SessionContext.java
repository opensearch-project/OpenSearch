/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;
/**
 * Session context for datafusion
 */
public class SessionContext implements AutoCloseable {

    // ptr to context in df
    private final long ptr;

    /**
     * Create a new DataFusion session context
     * @return context ID for subsequent operations
     */
    static native long createContext();

    /**
     * Close and cleanup a DataFusion context
     * @param contextId the context ID to close
     */
    public static native void closeContext(long contextId);

    public SessionContext() {
        this.ptr = createContext();
    }

    @Override
    public void close() throws Exception {
        closeContext(this.ptr);
    }
}
