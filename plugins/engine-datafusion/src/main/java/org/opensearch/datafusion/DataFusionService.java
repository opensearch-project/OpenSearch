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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.util.Map;

/**
 * Service for managing DataFusion contexts and operations - essentially like SearchService
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);

    private final DataSourceRegistry dataSourceRegistry;
    private final DataFusionRuntimeEnv runtimeEnv;


    /**
     * Creates a new DataFusion service instance.
     */
    public DataFusionService(Map<DataFormat, DataSourceCodec> dataSourceCodecs, ClusterService clusterService) {
        this.dataSourceRegistry = new DataSourceRegistry(dataSourceCodecs);

        // to verify jni
        String version = NativeBridge.getVersionInfo();
        this.runtimeEnv = new DataFusionRuntimeEnv(clusterService);
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
        runtimeEnv.close();
        logger.info("DataFusion service stopped");
    }

    @Override
    protected void doClose() {
        doStop();
    }


    public long getRuntimePointer() {
        return runtimeEnv.getPointer();
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
}
