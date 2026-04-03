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

    private static volatile boolean loaded = false;

    static {
        loadNativeLibrary();
    }

    private NativeBridge() {}

    private static synchronized void loadNativeLibrary() {
        if (loaded) return;
        try {
            // Try java.library.path first
            System.loadLibrary("opensearch_datafusion_jni");
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            // Fall back to loading from classpath resources
            try {
                loadFromResources();
                loaded = true;
            } catch (Exception ex) {
                throw new ExceptionInInitializerError(
                    "Failed to load native library opensearch_datafusion_jni: " + e.getMessage()
                    + ". Also failed to load from resources: " + ex.getMessage());
            }
        }
    }

    private static void loadFromResources() throws java.io.IOException {
        String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
        String libName;
        if (os.contains("mac")) {
            libName = "libopensearch_datafusion_jni.dylib";
        } else if (os.contains("win")) {
            libName = "opensearch_datafusion_jni.dll";
        } else {
            libName = "libopensearch_datafusion_jni.so";
        }

        String resourcePath = "/native/" + libName;
        try (java.io.InputStream in = NativeBridge.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new java.io.FileNotFoundException("Native library not found in resources: " + resourcePath);
            }
            java.io.File tempFile = java.io.File.createTempFile("opensearch_datafusion_jni", libName.substring(libName.lastIndexOf('.')));
            tempFile.deleteOnExit();
            try (java.io.OutputStream out = new java.io.FileOutputStream(tempFile)) {
                in.transferTo(out);
            }
            System.load(tempFile.getAbsolutePath());
        }
    }

    // ---- Tokio runtime management ----

    /**
     * Initializes the Tokio runtime manager with dedicated CPU and IO thread pools.
     * Must be called once at node startup before any query execution.
     * @param cpuThreads number of CPU threads for the dedicated executor
     */
    public static native void initTokioRuntimeManager(int cpuThreads);

    /**
     * Shuts down the Tokio runtime manager and all associated thread pools.
     */
    public static native void shutdownTokioRuntimeManager();

    // ---- DataFusion runtime ----

    /**
     * Creates a global DataFusion runtime with the given resource limits.
     * @param memoryLimit the maximum memory in bytes
     * @param cacheManagerPtr the native cache manager pointer (0 for no cache)
     * @param spillDir the directory path for spill files
     * @param spillLimit the maximum spill size in bytes
     */
    public static native long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit);

    /**
     * Closes the global DataFusion runtime.
     * @param ptr the native runtime pointer
     */
    public static native void closeGlobalRuntime(long ptr);

    // ---- Reader management ----

    /**
     * Creates a native DataFusion reader.
     * @param path the directory path containing data files
     * @param files the array of file names to read
     */
    public static native long createDatafusionReader(String path, String[] files);

    /**
     * Closes the native DataFusion reader.
     * @param ptr the native reader pointer
     */
    public static native void closeDatafusionReader(long ptr);

    // ---- Query execution ----

    /**
     * Executes a substrait plan asynchronously against the given reader.
     * The result stream pointer is delivered via the ActionListener callback.
     *
     * @param readerPtr the native reader pointer
     * @param tableName the target table name
     * @param substraitPlan the serialized substrait plan bytes
     * @param runtimePtr the native runtime pointer
     * @param listener callback receiving the stream pointer (Long) or error
     */
    public static native void executeQueryAsync(
        long readerPtr,
        String tableName,
        byte[] substraitPlan,
        long runtimePtr,
        org.opensearch.core.action.ActionListener<Long> listener
    );

    // ---- Stream operations ----

    /**
     * Returns the Arrow schema address for the given stream.
     * Synchronous — schema is cached on the stream and fast to access.
     *
     * @param streamPtr the native stream pointer
     * @param listener callback receiving the ArrowSchema C Data Interface address
     */
    public static native void streamGetSchema(long streamPtr, org.opensearch.core.action.ActionListener<Long> listener);

    /**
     * Loads the next record batch from the stream asynchronously.
     *
     * @param runtimePtr the native runtime pointer
     * @param streamPtr the native stream pointer
     * @param listener callback receiving ArrowArray C Data Interface address, or 0 if end-of-stream
     */
    public static native void streamNext(long runtimePtr, long streamPtr, org.opensearch.core.action.ActionListener<Long> listener);

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

    /**
     * Deliberately panics in native code (test only). Used to verify panic catching.
     * @param message the panic message
     */
    public static native void testPanic(String message);

    // ---- Logger ----

    /**
     * Initializes the Rust-to-Java logging bridge.
     */
    public static native void initLogger();
}
