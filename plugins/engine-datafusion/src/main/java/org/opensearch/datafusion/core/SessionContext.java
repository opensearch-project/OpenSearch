/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.core;

/**
 * Session context for datafusion
 */
public class SessionContext implements AutoCloseable {

    private final long context;
    private final long runtime;

    /**
     * Constructor for SessionContext with custom parquet file.
     * @param tableName table name
     * @param parquetFilePath Path to the parquet file to register
     */
    public SessionContext(String parquetFilePath, String tableName) {
        this.context = createContext();
        this.runtime = createRuntime(parquetFilePath);
        registerParquetTable(this.context, this.runtime, parquetFilePath, tableName);
    }

    /**
     * Creates a new DataFusion session context
     * @return pointer to the native context
     */
    public static native long createContext();

    /**
     * Closes and cleans up a DataFusion session context
     * @param contextPointer pointer to the context to close
     * @return status code
     */
    public static native long closeContext(long contextPointer);

    /**
     * Creates a new DataFusion runtime
     * @param parquetFilePath path to parquet file
     * @return pointer to the native runtime
     */
    private static native long createRuntime(String parquetFilePath);

    /**
     * Closes and cleans up a DataFusion runtime
     * @param runtimePointer pointer to the runtime to close
     * @return status code
     */
    public static native long closeRuntime(long runtimePointer);

    /**
     * Registers a parquet table with the given context and runtime
     * @param contextPointer pointer to the DataFusion context
     * @param runTime pointer to the runtime
     * @param filePath path to the parquet file
     * @param tableName name to register the table as
     */
    public static native void registerParquetTable(long contextPointer, long runTime, String filePath, String tableName);

    /**
     * Get the native context pointer
     * @return the context pointer
     */
    public long getContext() {
        return context;
    }

    /**
     * Get the runtime
     * @return the runtime pointer
     */
    public long getRuntime() {
        return runtime;
    }

    @Override
    public void close() throws Exception {
        closeContext(this.context);
        closeRuntime(this.runtime);
    }
}
