/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.jni;

import org.opensearch.core.action.ActionListener;

/**
 * Delegates to engine-datafusion's NativeBridge.
 * The native library is loaded by engine-datafusion — we just call through.
 */
public final class NativeBridge {

    private NativeBridge() {}

    public static long createGlobalRuntime(long limit, long cacheManagerPtr, String spillDir, long spillLimit) {
        return org.opensearch.datafusion.jni.NativeBridge.createGlobalRuntime(limit, cacheManagerPtr, spillDir, spillLimit);
    }

    public static void closeGlobalRuntime(long ptr) {
        org.opensearch.datafusion.jni.NativeBridge.closeGlobalRuntime(ptr);
    }

    public static long createDatafusionReader(String path, String[] files) {
        return org.opensearch.datafusion.jni.NativeBridge.createDatafusionReader(path, files);
    }

    public static void closeDatafusionReader(long ptr) {
        org.opensearch.datafusion.jni.NativeBridge.closeDatafusionReader(ptr);
    }

    public static void executeQueryPhaseAsync(long readerPtr, String tableName, byte[] plan,
            boolean isQueryPlanExplainEnabled, int partitionCount, long runtimePtr, ActionListener<Long> listener) {
        org.opensearch.datafusion.jni.NativeBridge.executeQueryPhaseAsync(
            readerPtr, tableName, plan, isQueryPlanExplainEnabled, partitionCount, runtimePtr, listener);
    }

    public static void streamNext(long runtime, long stream, ActionListener<Long> listener) {
        org.opensearch.datafusion.jni.NativeBridge.streamNext(runtime, stream, listener);
    }

    public static void streamGetSchema(long stream, ActionListener<Long> listener) {
        org.opensearch.datafusion.jni.NativeBridge.streamGetSchema(stream, listener);
    }

    public static void streamClose(long stream) {
        org.opensearch.datafusion.jni.NativeBridge.streamClose(stream);
    }
}
