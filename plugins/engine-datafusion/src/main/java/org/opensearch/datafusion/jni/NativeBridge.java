/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni;

import org.opensearch.datafusion.ObjectResultCallback;

/**
 * Core JNI bridge to native DataFusion library.
 * All native method declarations are centralized here.
 */
public final class NativeBridge {

    static {
        NativeLibraryLoader.load();
    }

    private NativeBridge() {}

    // Runtime management
    public static native long createGlobalRuntime();
    public static native long createTokioRuntime();
    public static native void closeGlobalRuntime(long ptr);

    // Session management
    public static native long createSessionContext(long runtimeId);
    public static native void closeSessionContext(long contextId);

    // Query execution
    public static native long executeQueryPhase(long cachePtr, String tableName, byte[] plan, long runtimePtr);
    public static native long executeFetchPhase(long cachePtr, long[] rowIds, String[] projections, long runtimePtr);

    // Stream operations
    public static native void streamNext(long runtime, long stream, ObjectResultCallback callback);
    public static native void streamGetSchema(long stream, ObjectResultCallback callback);
    public static native void streamClose(long stream);

    // Reader management
    public static native long createDatafusionReader(String path, String[] files);
    public static native void closeDatafusionReader(long ptr);

    // Other methods
    public static native String getVersionInfo();
    public static native int registerCsvDirectory(long contextId, String tableName, String directoryPath, String[] fileNames);
}
