/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.datafusion.DataFusionQueryJNI;
import org.opensearch.index.IndexService;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;

import static org.opensearch.datafusion.DatafusionEngine.CLUSTER_DATAFUSION_BATCH_SIZE;
import static org.opensearch.datafusion.DatafusionEngine.CLUSTER_DATAFUSION_STATISTICS_ENABLED;
import static org.opensearch.datafusion.DatafusionEngine.INDEX_DATAFUSION_STATISTICS_ENABLED;

public class DataFusionSessionConfig implements SessionConfig {

    private final String indexName;

    // This class should have stored it and get's overriden by the Dataformats later on
    private Boolean isCollectStatisticsEnabled;
    private Boolean isPageIndexEnabled;
    private Integer batchSize;


    // Final Object ptr to be constructed lazily
    private Long datafusionSessionConfigPtr;

    DataFusionSessionConfig(IndexService indexService, ClusterService clusterService) {
        this.indexName = indexService.index().getName();
        Settings indexSettings = indexService.getIndexSettings().getSettings();
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        isCollectStatisticsEnabled =  indexSettings.getAsBoolean(
            INDEX_DATAFUSION_STATISTICS_ENABLED.getKey(),
            clusterSettings.get(CLUSTER_DATAFUSION_STATISTICS_ENABLED)
        );

        batchSize = clusterSettings.get(CLUSTER_DATAFUSION_BATCH_SIZE);
    }

    /**
     * Gets or creates the native session config pointer.
     * Call this only after all merging and population is complete.
     */
    @Override
    public long getOrCreateNativePtr() {
        if (datafusionSessionConfigPtr == null) {
            datafusionSessionConfigPtr = DataFusionQueryJNI.createSessionConfigNative(
                isCollectStatisticsEnabled,
                isPageIndexEnabled != null ? isPageIndexEnabled : false,
                batchSize != null ? batchSize : 0
            );
        }
        return datafusionSessionConfigPtr;
    }

    @Override
    public Integer getBatchSize(String indexName) {
        return batchSize;
    }

    @Override
    public Boolean isCollectStatisticsEnabled(String indexName) {
        return isCollectStatisticsEnabled;
    }

    @Override
    public Boolean isPageIndexEnabled(String indexName) {
        return isPageIndexEnabled;
    }

    @Override
    public void mergeFrom(SessionConfig other) {

        if(other.getBatchSize(indexName) != null) {
            batchSize = other.getBatchSize(indexName);
        }

        if(other.isCollectStatisticsEnabled(indexName) != null) {
            isCollectStatisticsEnabled = other.isCollectStatisticsEnabled(indexName);
        }

        if(other.isPageIndexEnabled(indexName) != null) {
            isPageIndexEnabled = other.isPageIndexEnabled(indexName);
        }

    }

}
