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

    public static native long createDatafusionReader(String path, String[] files);

    public static native void closeDatafusionReader(long ptr);

    public static native long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit);

    public static native void closeGlobalRuntime(long ptr);

    /**
     * Executes a substrait plan against the given reader and returns a stream pointer.
     *
     * @param readerPtr     native reader pointer
     * @param tableName     table name for registration with DataFusion
     * @param substraitPlan serialized substrait plan bytes
     * @param runtimePtr    native runtime pointer
     * @return native stream pointer (caller must close via {@link #streamClose})
     */
    public static native long executeQuery(long readerPtr, String tableName, byte[] substraitPlan, long runtimePtr);

    /**
     * Returns the Arrow schema address for the given stream.
     *
     * @param streamPtr native stream pointer
     * @return ArrowSchema C Data Interface address
     */
    public static native long streamGetSchema(long streamPtr);

    /**
     * Loads the next record batch from the stream.
     *
     * @param runtimePtr native runtime pointer
     * @param streamPtr  native stream pointer
     * @return ArrowArray C Data Interface address, or 0 if end-of-stream
     */
    public static native long streamNext(long runtimePtr, long streamPtr);

    /**
     * Closes the native stream and releases associated resources.
     *
     * @param streamPtr native stream pointer
     */
    public static native void streamClose(long streamPtr);
}
