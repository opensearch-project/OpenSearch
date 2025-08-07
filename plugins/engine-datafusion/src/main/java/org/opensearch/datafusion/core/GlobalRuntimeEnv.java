/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

import static org.opensearch.datafusion.DataFusionJNI.closeGlobalRuntime;
import static org.opensearch.datafusion.DataFusionJNI.createGlobalRuntime;

public class GlobalRuntimeEnv implements AutoCloseable{
    // ptr to runtime environment in df
    private final long ptr;


    public GlobalRuntimeEnv() {
        this.ptr = createGlobalRuntime();
    }

    public long getPointer() {
        return ptr;
    }

    @Override
    public void close() {
        closeGlobalRuntime(this.ptr);
    }
}
