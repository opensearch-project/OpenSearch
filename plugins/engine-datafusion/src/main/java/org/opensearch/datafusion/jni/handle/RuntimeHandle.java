/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni.handle;

import org.opensearch.datafusion.jni.NativeBridge;

/**
 * Type-safe handle for native runtime environment.
 */
public final class RuntimeHandle extends NativeHandle {

    public RuntimeHandle(long ptr) {
        super(ptr);
    }

    @Override
    protected void doClose() {
        NativeBridge.closeGlobalRuntime(ptr);
    }
}
