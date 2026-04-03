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

    // ---- Cache management ----

    /**
     * Notifies the native cache manager that new files are available for caching.
     * @param runtimePtr the native runtime pointer
     * @param filePaths absolute paths of the new files
     */
    public static native void cacheManagerAddFiles(long runtimePtr, String[] filePaths);

    /**
     * Notifies the native cache manager that files have been deleted and should be evicted.
     * @param runtimePtr the native runtime pointer
     * @param filePaths absolute paths of the deleted files
     */
    public static native void cacheManagerRemoveFiles(long runtimePtr, String[] filePaths);

    // ---- Tree query execution ----

    /**
     * Executes a boolean tree query asynchronously. The tree bytes and bridge context ID
     * are passed to Rust. Rust deserializes the tree, creates JniTreeShardSearcher per
     * collector leaf, builds TreeIndexedTableProvider, and returns a stream pointer.
     *
     * @param treeBytes        Serialized boolean tree (from IndexFilterTree.serialize())
     * @param bridgeContextId  Context ID registered with FilterTreeCallbackBridge
     * @param segmentMaxDocs   Max doc count per segment (long[])
     * @param parquetPaths     One parquet file path per segment (String[])
     * @param tableName        Table name for DataFusion registration
     * @param substraitBytes   Serialized substrait plan bytes
     * @param numPartitions    Number of DataFusion partitions
     * @param indexLeafCount   Number of collector leaves in the tree
     * @param isQueryPlanExplainEnabled Whether to enable query plan explain
     * @param runtimePtr       Pointer to the DataFusion runtime
     * @param listener         ActionListener to receive the stream pointer (Long)
     */
    public static native void executeTreeQueryAsync(
        byte[] treeBytes,
        long bridgeContextId,
        long[] segmentMaxDocs,
        String[] parquetPaths,
        String tableName,
        byte[] substraitBytes,
        int numPartitions,
        int indexLeafCount,
        boolean isQueryPlanExplainEnabled,
        long runtimePtr,
        org.opensearch.core.action.ActionListener<Long> listener
    );

    // ---- Test helpers ----

    /**
     * Converts a SQL query to serialized Substrait plan bytes (test only).
     * Registers the table from the reader, plans the SQL, and returns the substrait bytes.
     * @param readerPtr the native reader pointer
     * @param tableName the table name to register
     * @param sql the SQL query string
     * @param runtimePtr the native runtime pointer
     * @return serialized substrait plan bytes
     */
    public static native byte[] sqlToSubstrait(long readerPtr, String tableName, String sql, long runtimePtr);

    // ---- Logger ----

    /**
     * Initializes the Rust-to-Java logging bridge.
     */
    public static native void initLogger();
}
