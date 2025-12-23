/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni;

import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileStats;

import java.util.Map;

/**
 * Core JNI bridge to native DataFusion library.
 * All native method declarations are centralized here.
 */
public final class NativeBridge {

    static {
        NativeLibraryLoader.load("opensearch_datafusion_jni");
        initLogger();
    }

    private NativeBridge() {}

    // Runtime management
    public static native long createGlobalRuntime(long limit, long cacheManagerPtr, String spillDir, long spillLimit);
    public static native void closeGlobalRuntime(long ptr);

    // Tokio runtime
    public static native long startTokioRuntimeMonitoring();
    // Initialize tokio runtime manager once on startup
    public static native void initTokioRuntimeManager(int cpuThreads);
    // Shutdown tokio runtime manager on datafusion service
    public static native void shutdownTokioRuntimeManager();

    // Query execution
    public static native void executeQueryPhaseAsync(long readerPtr, String tableName, byte[] plan, boolean isQueryPlanExplainEnabled, long runtimePtr, ActionListener<Long> listener);
    public static native long executeFetchPhase(long readerPtr, long[] rowIds, String[] includeFields, String[] excludeFields, long runtimePtr);

    // File Stats
    public static native void fetchSegmentStats(long readerPtr, ActionListener<Map<String, FileStats>> listener);

    // Stream operations
    public static native void streamNext(long runtime, long stream, ActionListener<Long> listener);
    public static native void streamGetSchema(long stream, ActionListener<Long> listener);
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


    // Logger initialization
    public static native void initLogger();

    // Other methods
    public static native String getVersionInfo();
}
