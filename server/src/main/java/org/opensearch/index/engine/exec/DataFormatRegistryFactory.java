/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.SearchAnalyticsBackEndPlugin;

import java.io.IOException;
import java.util.List;

@ExperimentalApi
public class DataFormatRegistryFactory {
    private final List<SearchAnalyticsBackEndPlugin> searchPlugins;

    public DataFormatRegistryFactory(List<SearchAnalyticsBackEndPlugin> searchPlugins) {
        this.searchPlugins = searchPlugins;
    }

    /**
     * Called at shard creation time when ShardPath is available.
     */
    public DataFormatRegistry create(ShardPath shardPath) throws IOException {
        return new DataFormatRegistry(searchPlugins, shardPath);
    }

    public boolean hasPlugins() {
        return !searchPlugins.isEmpty();
    }
}
