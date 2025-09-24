/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Service for managing DataFusion contexts and operations - essentially like SearchService
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private final Environment environment;
    private SessionContext defaultSessionContext;

    /**
     * Constructor for DataFusionService.
     * @param environment The OpenSearch environment containing path configurations and settings
     */
    public DataFusionService(Environment environment) {
        super();
        this.environment = environment;
    }

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);

    // in memory contexts, similar to ReaderContext in SearchService, just a ptr to SessionContext for now.
    private final ConcurrentMapLong<SessionContext> contexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    @Override
    protected void doStart() {
        logger.info("Starting DataFusion service");
        try {
            // Test that the native library loads correctly
            String version = DataFusionJNI.getVersion();
            logger.info("DataFusion service started successfully. Version info: {}", version);

            // Create a default context with parquet file path from path.repo setting
            String repoPath = environment.settings().get("path.data").trim().replaceAll("^\\[|]$", "");
            if (repoPath.isEmpty()) {
                throw new RuntimeException(
                    "path.repo setting is required for DataFusion service. "
                        + "Please configure it using -PrepoPath when starting OpenSearch."
                );
            }

            logger.info("DataFusion service started successfully. Repo path: {}", repoPath);

            Path dataPath = Path.of(repoPath);
            Path parquetFile = dataPath.resolve("hits_data.parquet");

            // Check if the parquet file exists
            if (!Files.exists(parquetFile)) {
                throw new RuntimeException(
                    "Parquet file not found at: " + parquetFile + ". Please place your parquet file in the OpenSearch data directory."
                );
            }

            defaultSessionContext = new SessionContext(parquetFile.toString(), "hits");
            contexts.put(defaultSessionContext.getContext(), defaultSessionContext);
            logger.info("Created default DataFusion context with ID: {}", defaultSessionContext.getContext());
        } catch (Exception e) {
            logger.error("Failed to start DataFusion service", e);
            throw new RuntimeException("Failed to initialize DataFusion JNI", e);
        }
    }

    @Override
    protected void doStop() {
        logger.info("Stopping DataFusion service");
        // Close all named contexts
        for (SessionContext ctx : contexts.values()) {
            try {
                ctx.close();
            } catch (Exception e) {
                logger.warn("Error closing DataFusion context", e);
            }
        }
        contexts.clear();
        logger.info("DataFusion service stopped");
    }

    @Override
    protected void doClose() {
        // Ensure all resources are cleaned up
        doStop();
    }

    /**
     * Get a context by id
     * @param id the context id
     * @return the context ID, or null if not found
     */
    SessionContext getContext(long id) {
        return contexts.get(id);
    }

    /**
     * Get default context
     * @return default context
     */
    SessionContext getDefaultContext() {
        return defaultSessionContext;
    }

    /**
     * Close a context
     * @param contextId the context id
     * @return true if the context was found and closed, false otherwise
     */
    public boolean closeContext(long contextId) {
        SessionContext context = contexts.remove(contextId);
        if (context != null) {
            try {
                context.close();
                return true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    /**
     * Get version information
     * @return JSON version string
     */
    public String getVersion() {
        return DataFusionJNI.getVersion();
    }

    /**
     * Execute a Substrait query plan and return a stream pointer for streaming results.
     * Use this for large result sets to avoid memory issues.
     *
     * @param queryPlanIR the Substrait query plan as bytes
     * @return stream pointer (0 if error occurred)
     */
    public long executeSubstraitQueryStream(byte[] queryPlanIR) {
        return nativeExecuteSubstraitQueryStream(defaultSessionContext.getRuntime(), defaultSessionContext.getContext(), queryPlanIR);
    }

    /**
     * Executes a Substrait query plan and returns a stream pointer
     * @param runTime the DataFusion runtime ID
     * @param contextId the DataFusion context ID
     * @param queryPlanIR the Substrait query plan bytes
     * @return pointer to the result stream
     */
    public static native long nativeExecuteSubstraitQueryStream(long runTime, long contextId, byte[] queryPlanIR);
}
