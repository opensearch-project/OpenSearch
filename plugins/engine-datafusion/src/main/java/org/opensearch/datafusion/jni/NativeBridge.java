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
        NativeLibraryLoader.load("opensearch_datafusion_jni");
    }

    private NativeBridge() {}

    // Runtime management
    public static native long createGlobalRuntime(long limit);
    public static native void closeGlobalRuntime(long ptr);

    // Session management
    public static native long createSessionContext(long runtimeId);
    public static native void closeSessionContext(long contextId);

    // Query execution
    public static native long executeQueryPhase(long readerPtr, String tableName, byte[] plan, long runtimePtr);
    public static native long executeFetchPhase(long readerPtr, long[] rowIds, String[] projections, long runtimePtr);

    // Stream operations
    public static native void streamNext(long runtime, long stream, ObjectResultCallback callback);
    public static native void streamGetSchema(long stream, ObjectResultCallback callback);
    public static native void streamClose(long stream);

    // Cache management
    public static native long createCustomCacheManager();
    public static native long createCache(long cacheManagerPointer, String cacheType, long sizeLimit, String evictionType);
    public static native void cacheManagerAddFiles(long cacheManagerPointer, String[] filePaths);
    public static native void cacheManagerRemoveFiles(long cacheManagerPointer, String[] filePaths);
    public static native boolean cacheManagerUpdateSizeLimitForCacheType(long cacheManagerPointer, String cacheType, long sizeLimit);
    public static native long cacheManagerGetMemoryConsumedForCacheType(long cacheManagerPointer, String cacheType);
    public static native long cacheManagerGetTotalMemoryConsumed(long cacheManagerPointer);
    public static native void cacheManagerClearByCacheType(long cacheManagerPointer, String cacheType);
    public static native void cacheManagerClear(long cacheManagerPointer);
    public static native void destroyCustomCacheManager(long cacheManagerPointer);
    // For testing-purposes only
    public static native boolean cacheManagerGetItemByCacheType(long cacheManagerPointer, String cacheType, String filePath);


    // Reader management
    public static native long createDatafusionReader(String path, String[] files);
    public static native void closeDatafusionReader(long ptr);

    // Memory monitoring
    public static native void printMemoryPoolAllocation(long runtimePtr);


    // Other methods
    public static native String getVersionInfo();
    public static native int registerCsvDirectory(long contextId, String tableName, String directoryPath, String[] fileNames);
}
