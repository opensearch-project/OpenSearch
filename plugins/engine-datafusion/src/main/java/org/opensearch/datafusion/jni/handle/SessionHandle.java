/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.jni.NativeHandle;

/**
 * Session context for DataFusion operations.
 */
public final class SessionHandle extends NativeHandle {

    /**
     * Creates a new session context with the given runtime.
     * @param runtimeId the runtime environment ID
     */
    public SessionHandle(long runtimeId) {
        super(NativeBridge.createSessionContext(runtimeId));
    }


    /**
     * Closes the session context and releases any associated resources.
     */
    @Override
    protected void doClose() {
        NativeBridge.closeSessionContext(ptr);
    }
}
