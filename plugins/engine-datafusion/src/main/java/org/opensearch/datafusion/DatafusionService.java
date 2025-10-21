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
import org.opensearch.datafusion.core.GlobalRuntimeEnv;
import org.opensearch.execution.search.spi.DataFormatCodec;
import org.opensearch.index.engine.exec.format.DataFormat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service for managing DataFusion contexts and search operations
 */
public class DatafusionService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DatafusionService.class);
    private final ConcurrentMapLong<DataFormatCodec> dataFormatCodecs = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final DataSourceRegistry dataSourceRegistry;
    private final GlobalRuntimeEnv globalRuntimeEnv;

    /**
     * Creates a new DataFusion service instance.
     */
    public DatafusionService(Map<DataFormat, DataFormatCodec> dataSourceCodecs) {
        this.dataSourceRegistry = new DataSourceRegistry(dataSourceCodecs);
        this.globalRuntimeEnv = new GlobalRuntimeEnv();
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
        for (Long sessionId : dataFormatCodecs.keySet()) {
            try {
                closeSessionContext(sessionId).get();
            } catch (Exception e) {
                logger.warn("Error closing session context {}", sessionId, e);
            }
        }
        dataFormatCodecs.clear();
        globalRuntimeEnv.close();
        logger.info("DataFusion service stopped");
    }

    @Override
    protected void doClose() {
        doStop();
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
        DataFormatCodec codec = dataFormatCodecs.remove(sessionContextId);
        if (codec == null) {
            logger.debug("Session context {} not found or already closed", sessionContextId);
            return CompletableFuture.completedFuture(null);
        }

        return codec.closeSessionContext(sessionContextId);
    }
}
