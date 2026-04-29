/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.plugin.stats.BackendStatsProvider;
import org.opensearch.plugin.stats.PluginStats;

/**
 * DataFusion implementation of {@link BackendStatsProvider}.
 *
 * <p>When the Mustang Analytics Plugin lands, it discovers
 * {@code BackendStatsProvider} implementations and iterates over them.
 * DataFusion is already registered via this class.
 */
public class DataFusionBackendStatsProvider implements BackendStatsProvider {

    /** Creates a new {@code DataFusionBackendStatsProvider}. */
    public DataFusionBackendStatsProvider() {}

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public PluginStats getBackendStats() {
        // TODO: Expose only necessary DF metrics to core.
        return null;
    }
}
