/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;

public class ParquetSettings {

    private static ParquetSettings INSTANCE;

    private SessionConfig sessionConfig;

    private ParquetSettings() {}

    public static synchronized ParquetSettings state() {
        if (INSTANCE == null) {
            INSTANCE = new ParquetSettings();
        }
        return INSTANCE;
    }

    public void initialize(ClusterService clusterService) {
        this.sessionConfig = new ParquetSessionConfig(clusterService);
    }

    public SessionConfig getSessionConfig() {
        return sessionConfig;
    }

}
