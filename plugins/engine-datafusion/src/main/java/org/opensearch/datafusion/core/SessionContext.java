/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.datafusion.jni.handle.SessionHandle;

/**
 * Session context for DataFusion operations.
 */
public final class SessionContext implements AutoCloseable {

    private final SessionHandle handle;

    /**
     * Creates a new session context with the given runtime.
     * @param runtimeId the runtime environment ID
     */
    public SessionContext(long runtimeId) {
        long ptr = NativeBridge.createSessionContext(runtimeId);
        this.handle = new SessionHandle(ptr);
    }

    /**
     * Gets the native pointer to the session context.
     * @return the native pointer
     */
    public long getPointer() {
        return handle.getPointer();
    }

    @Override
    public void close() {
        handle.close();
    }
}
