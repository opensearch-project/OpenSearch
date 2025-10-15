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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.datafusion.core.GlobalRuntimeEnv;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service for managing DataFusion contexts and operations - essentially like SearchService
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);
    private final ConcurrentMapLong<DataSourceCodec> sessionEngines = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final DataSourceRegistry dataSourceRegistry;
    private final GlobalRuntimeEnv globalRuntimeEnv;
    private CacheManager cacheManager;

    /**
     * Creates a new DataFusion service instance.
     */
    public DataFusionService(Map<DataFormat, DataSourceCodec> dataSourceCodecs) {
        this.dataSourceRegistry = new DataSourceRegistry(dataSourceCodecs);

        // to verify jni
        String version = DataFusionQueryJNI.getVersionInfo();
        this.globalRuntimeEnv = new GlobalRuntimeEnv();
    }

    public DataFusionService(Map<DataFormat, DataSourceCodec> dataSourceCodecs, ClusterSettings clusterSettings) {
        this.dataSourceRegistry = new DataSourceRegistry(dataSourceCodecs);

        // to verify jni
        String version = DataFusionQueryJNI.getVersionInfo();
        this.globalRuntimeEnv = new GlobalRuntimeEnv(clusterSettings);
        this.cacheManager = globalRuntimeEnv.getCacheManager();
    }

    @Override
    protected void doStart() {
        logger.info("Starting DataFusion service");
        try {
            // Initialize the data source registry
            // Test that at least one data source is available
            if (!dataSourceRegistry.hasCodecs()) {
                logger.warn("No data sources available");
            } else {
                logger.info(
                    "DataFusion service started successfully with {} data sources: {}",
                    dataSourceRegistry.getCodecNames().size(),
                    dataSourceRegistry.getCodecNames()
                );

            }
        } catch (Exception e) {
            logger.error("Failed to start DataFusion service", e);
            throw new RuntimeException("Failed to initialize DataFusion service", e);
        }
    }

    @Override
    protected void doStop() {
        logger.info("Stopping DataFusion service");

        // Close all session contexts
        for (Long sessionId : sessionEngines.keySet()) {
            try {
                closeSessionContext(sessionId).get();
            } catch (Exception e) {
                logger.warn("Error closing session context {}", sessionId, e);
            }
        }
        sessionEngines.clear();
        globalRuntimeEnv.close();
        logger.info("DataFusion service stopped");
    }

    @Override
    protected void doClose() {
        doStop();
    }

    /**
     * Register a directory with list of files to create a runtime environment
     * with listing files cache of DataFusion
     *
     * @param directoryPath path to the directory containing files
     * @param fileNames list of file names in the directory
     * @return runtime environment ID
     */
    public CompletableFuture<Void> registerDirectory(String directoryPath, List<String> fileNames) {
        DataSourceCodec engine = dataSourceRegistry.getDefaultEngine();
        if (engine == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No DataFusion engine available"));
        }

        logger.debug(
            "Registering directory {} with {} files using engine {}",
            directoryPath,
            fileNames.size(),
            engine.getClass().getSimpleName()
        );

        return engine.registerDirectory(directoryPath, fileNames, globalRuntimeEnv.getPointer());
    }

    /**
     * Create a session context
     *
     * @return session context ID
     */
    public CompletableFuture<Long> createSessionContext() {
        long runtimeEnvironmentId = globalRuntimeEnv.getPointer();
        DataSourceCodec codec = dataSourceRegistry.getDefaultEngine();
        if (codec == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Runtime environment not found: " + runtimeEnvironmentId));
        }

        logger.debug(
            "Creating session context for runtime environment {} using engine {}",
            runtimeEnvironmentId,
            codec.getClass().getSimpleName()
        );

        return codec.createSessionContext(runtimeEnvironmentId).thenApply(sessionId -> {
            // Track which engine created this session context
            sessionEngines.put(sessionId, codec);
            logger.debug("Created session context {} with engine {}", sessionId, codec.getClass().getSimpleName());
            return sessionId;
        });
    }

    /**
     * Execute a query accepting substrait plan bytes and run via session context
     *
     * @param sessionContextId the session context ID
     * @param substraitPlanBytes the substrait plan as byte array
     * @return record batch stream containing query results
     */
    public CompletableFuture<RecordBatchStream> executeSubstraitQuery(long sessionContextId, byte[] substraitPlanBytes) {
        DataSourceCodec engine = sessionEngines.get(sessionContextId);
        if (engine == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Session context not found: " + sessionContextId));
        }

        logger.debug(
            "Executing substrait query for session {} with plan size {} bytes using engine {}",
            sessionContextId,
            substraitPlanBytes.length,
            engine.getClass().getSimpleName()
        );

        return engine.executeSubstraitQuery(sessionContextId, substraitPlanBytes);
    }

    public long getRuntimePointer() {
        return globalRuntimeEnv.getPointer();
    }

    public long getTokioRuntimePointer() {
        return globalRuntimeEnv.getTokioRuntimePtr();
    }

    /**
     * Close the session context and clean up resources
     *
     * @param sessionContextId the session context ID to close
     * @return future that completes when cleanup is done
     */
    public CompletableFuture<Void> closeSessionContext(long sessionContextId) {
        DataSourceCodec engine = sessionEngines.remove(sessionContextId);
        if (engine == null) {
            logger.debug("Session context {} not found or already closed", sessionContextId);
            return CompletableFuture.completedFuture(null);
        }

        logger.debug("Closing session context {} using engine {}", sessionContextId, engine.getClass().getSimpleName());

        return engine.closeSessionContext(sessionContextId);
    }

    /**
     * Get version information from available codecs
     * @return JSON version string
     */
    public String getVersion() {
        StringBuilder version = new StringBuilder();
        version.append("{\"codecs\":[");

        boolean first = true;
        for (DataFormat engineName : this.dataSourceRegistry.getCodecNames()) {
            if (!first) {
                version.append(",");
            }
            version.append("{\"name\":\"").append(engineName).append("\"}");
            first = false;
        }

        version.append("]}");
        return version.toString();
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
