/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.jni;

/**
 * Core JNI bridge to native DataFusion library.
 * All native method declarations are centralized here.
 */
public final class NativeBridge {

    static {
        // TODO : NativeLibraryLoader.load("opensearch_datafusion_jni");
    }

    private NativeBridge() {}

    /**
     * Creates a native DataFusion reader
     * @param path the directory path containing data files
     * @param files the array of file names to read
     */
    public static native long createDatafusionReader(String path, String[] files);

    /**
     * Closes the native DataFusion reader.
     * @param ptr the native reader pointer
     */
    public static native void closeDatafusionReader(long ptr);

    /**
     * Creates a global DataFusion runtime with the given resource limits.
     * @param memoryLimit the maximum memory in bytes
     * @param cacheManagerPtr the native cache manager pointer
     * @param spillDir the directory path for spill files
     * @param spillLimit the maximum spill size in bytes
     */
    public static native long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit);

    /**
     * Closes the global DataFusion runtime.
     * @param ptr the native runtime pointer
     */
    public static native void closeGlobalRuntime(long ptr);

    /**
     * Executes a substrait plan against the given reader and returns a stream pointer.
     *
     * @param readerPtr the native reader pointer
     * @param tableName the target table name
     * @param substraitPlan the serialized substrait plan bytes
     * @param runtimePtr the native runtime pointer
     * @return native stream pointer (caller must close via {@link #streamClose})
     */
    public static native long executeQuery(long readerPtr, String tableName, byte[] substraitPlan, long runtimePtr);

    /**
     * Returns the Arrow schema address for the given stream.
     *
     * @param streamPtr the native stream pointer
     * @return ArrowSchema C Data Interface address
     */
    public static native long streamGetSchema(long streamPtr);

    /**
     * Loads the next record batch from the stream.
     *
     * @param runtimePtr the native runtime pointer
     * @param streamPtr the native stream pointer
     * @return ArrowArray C Data Interface address, or 0 if end-of-stream
     */
    public static native long streamNext(long runtimePtr, long streamPtr);

    /**
     * Closes the native stream and releases associated resources.
     *
     * @param streamPtr the native stream pointer to close
     */
    public static native void streamClose(long streamPtr);
}
