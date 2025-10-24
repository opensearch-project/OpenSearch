/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.vectorized.execution.search.spi.ReadEngineConfig;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;

public class DatafusionEngineConfig implements ReadEngineConfig {

    SessionConfig dataFusionSessionConfig;

    DatafusionEngineConfig(
        IndexService indexService,
        ClusterService clusterService
    ) {
        this.dataFusionSessionConfig = new DataFusionSessionConfig(indexService, clusterService);
    }

    @Override
    public SessionConfig getSessionConfig() {
        return dataFusionSessionConfig;
    }

    @Override
    public ReadEngineConfig updateSessionConfig(SessionConfig sessionConfig) {
        this.dataFusionSessionConfig.mergeFrom(sessionConfig);
        return this;
    }
}
