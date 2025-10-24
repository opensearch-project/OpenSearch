/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;
import reactor.util.annotation.NonNull;

import static org.opensearch.common.settings.Setting.Property.Dynamic;
import static org.opensearch.common.settings.Setting.Property.IndexScope;
import static org.opensearch.common.settings.Setting.Property.NodeScope;

/*
 For Settings which needs to be set from the ParquetSessionConfig,
 we need to make sure that in the get call, we return the default value from the cluster settings.
 */
public class ParquetSessionConfig implements SessionConfig {

    private ClusterService clusterService;

    public static final Setting<Integer> INDEX_PARQUET_BATCH_SIZE = Setting.intSetting(
        "index.parquet.batch_size",
        1024,
        IndexScope,
        Dynamic
    );

    public static final Setting<Integer> CLUSTER_PARQUET_BATCH_SIZE = Setting.intSetting(
        "parquet.batch_size",
        2048,
        NodeScope,
        Dynamic
    );

    public static final Setting<Boolean> INDEX_PARQUET_PAGE_INDEX_ENABLED = Setting.boolSetting(
        "index.parquet.page_index.enabled",
        true,
        IndexScope,
        Dynamic
    );

    public static final Setting<Boolean> CLUSTER_PARQUET_PAGE_INDEX_ENABLED = Setting.boolSetting(
        "parquet.page_index.enabled",
        true,
        NodeScope,
        Dynamic
    );


    ParquetSessionConfig(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public Settings getIndexSettings(@NonNull final String indexName) {
        return clusterService.state().getMetadata().index(indexName).getSettings();
    }

    @Override
    public Integer getBatchSize(String indexName) {
        return getIndexSettings(indexName)
            .getAsInt(INDEX_PARQUET_BATCH_SIZE.getKey(),
                clusterService.getClusterSettings().getOrNull(CLUSTER_PARQUET_BATCH_SIZE));
    }


    @Override
    public Boolean isPageIndexEnabled(String indexName) {
        return getIndexSettings(indexName)
            .getAsBoolean(INDEX_PARQUET_PAGE_INDEX_ENABLED.getKey(),
                clusterService.getClusterSettings().get(CLUSTER_PARQUET_PAGE_INDEX_ENABLED));
    }



    @Override
    public void mergeFrom(SessionConfig other) {
        throw new UnsupportedOperationException("Can't update the Parquet SessionConfig");
    }

    @Override
    public long getOrCreateNativePtr() {
        return 0;
    }
}
