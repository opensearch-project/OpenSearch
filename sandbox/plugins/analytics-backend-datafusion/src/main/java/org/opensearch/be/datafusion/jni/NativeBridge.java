/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion.jni;

/**
 * JNI bridge to the Rust DataFusion engine.
 * Native library: analytics_datafusion_jni (.so / .dylib)
 */
public class NativeBridge {

    static {
        NativeLibraryLoader.load("analytics_datafusion_jni");
    }

    /**
     * Creates a Tokio async runtime. Returns a pointer (as long) to the runtime.
     */
    public static native long createRuntime();

    /**
     * Destroys a Tokio runtime previously created by {@link #createRuntime()}.
     */
    public static native void destroyRuntime(long runtimePtr);

    /**
     * Executes a SQL query against a parquet file and streams results via callback.
     *
     * For each result batch, calls callback.onBatch(schemaAddr, arrayAddr) with
     * heap-allocated Arrow C Data Interface pointers. After all batches, calls
     * callback.onComplete().
     *
     * @param runtimePtr  Tokio runtime pointer from createRuntime()
     * @param parquetPath Path to the parquet file
     * @param sql         SQL query (table is registered as "t")
     * @param callback    BatchCallback receiving Arrow FFI pointers
     */
    /**
     * Creates a test parquet file with category (string) + amount (int) data.
     * 8 rows: A/100, B/200, A/300, C/150, B/250, A/400, C/350, D/175
     */
    public static native void createTestParquet(String path);

    public static native void executeAndStream(long runtimePtr, String parquetPath, String sql, Object callback);
}
