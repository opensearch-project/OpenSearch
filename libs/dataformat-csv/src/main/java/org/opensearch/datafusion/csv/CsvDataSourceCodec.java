/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.csv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.datafusion.spi.DataSourceCodec;
import org.opensearch.datafusion.spi.RecordBatchStream;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Datasource codec implementation for CSV files
 */
public class CsvDataSourceCodec implements DataSourceCodec {

    private static final Logger logger = LogManager.getLogger(CsvDataSourceCodec.class);
    private static final AtomicLong runtimeIdGenerator = new AtomicLong(0);
    private static final AtomicLong sessionIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<Long, Long> sessionContexts = new ConcurrentHashMap<>();

    // JNI library loading
    static {
        try {
            JniLibraryLoader.loadLibrary();
            logger.info("DataFusion JNI library loaded successfully");
        } catch (Exception e) {
            logger.error("Failed to load DataFusion JNI library", e);
            throw new RuntimeException("Failed to initialize DataFusion JNI library", e);
        }
    }

    @Override
    public CompletableFuture<Void> registerDirectory(String directoryPath, List<String> fileNames, long runtimeId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("Registering directory: {} with {} files", directoryPath, fileNames.size());

                // Convert file names to arrays for JNI
                String[] fileArray = fileNames.toArray(new String[0]);

                // Call native method to register directory
                nativeRegisterDirectory("csv_table", directoryPath, fileArray, runtimeId);
                return null;
            } catch (Exception e) {
                logger.error("Failed to register directory: " + directoryPath, e);
                throw new CompletionException("Failed to register directory", e);
            }
        });
    }

    @Override
    public CompletableFuture<Long> createSessionContext(long globalRuntimeEnvId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long sessionId = sessionIdGenerator.incrementAndGet();
                logger.debug("Creating session context with ID: {} for runtime: {}", sessionId, globalRuntimeEnvId);

                // Default configuration
                String[] configKeys = { "batch_size", "target_partitions" };
                String[] configValues = { "1024", "4" };

                // Create native session context
                long nativeContextPtr = nativeCreateSessionContext(configKeys, configValues);
                sessionContexts.put(sessionId, nativeContextPtr);

                logger.info("Created session context with ID: {}", sessionId);
                return sessionId;
            } catch (Exception e) {
                logger.error("Failed to create session context for runtime: " + globalRuntimeEnvId, e);
                throw new CompletionException("Failed to create session context", e);
            }
        });
    }

    @Override
    public CompletableFuture<RecordBatchStream> executeSubstraitQuery(long sessionContextId, byte[] substraitPlanBytes) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("Executing Substrait query for session: {}", sessionContextId);

                Long nativeContextPtr = sessionContexts.get(sessionContextId);
                if (nativeContextPtr == null) {
                    throw new IllegalArgumentException("Invalid session context ID: " + sessionContextId);
                }

                // Execute query and get native stream pointer
                long nativeStreamPtr = nativeExecuteSubstraitQuery(nativeContextPtr, substraitPlanBytes);

                // Create Java wrapper for the native stream
                RecordBatchStream stream = new CsvRecordBatchStream(nativeStreamPtr);

                logger.info("Successfully executed Substrait query for session: {}", sessionContextId);
                return stream;
            } catch (Exception e) {
                logger.error("Failed to execute Substrait query for session: " + sessionContextId, e);
                throw new CompletionException("Failed to execute Substrait query", e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> closeSessionContext(long sessionContextId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("Closing session context: {}", sessionContextId);

                Long nativeContextPtr = sessionContexts.remove(sessionContextId);
                if (nativeContextPtr != null) {
                    nativeCloseSessionContext(nativeContextPtr);
                    logger.info("Successfully closed session context: {}", sessionContextId);
                } else {
                    logger.warn("Session context not found: {}", sessionContextId);
                }

                return null;
            } catch (Exception e) {
                logger.error("Failed to close session context: " + sessionContextId, e);
                throw new CompletionException("Failed to close session context", e);
            }
        });
    }

    // Native method declarations - these will be implemented in the JNI library
    private static native void nativeRegisterDirectory(String tableName, String directoryPath, String[] files, long runtimeId);

    private static native long nativeCreateSessionContext(String[] configKeys, String[] configValues);

    private static native long nativeExecuteSubstraitQuery(long sessionContextPtr, byte[] substraitPlan);

    private static native void nativeCloseSessionContext(long sessionContextPtr);
}
