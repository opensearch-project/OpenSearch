/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

/**
 * SPI adapter that delegates to the real {@link DataFusionPlugin} instance.
 *
 * <p>{@code PluginsService} extension discovery requires either a no-arg constructor
 * or a constructor taking the enclosing plugin. Since {@code DataFusionPlugin}
 * itself needs a {@code Settings} constructor for plugin loading, this thin
 * adapter bridges the two requirements.
 */
public class DataFusionAnalyticsBackend implements AnalyticsBackEndPlugin {

    private final DataFusionPlugin plugin;

    public DataFusionAnalyticsBackend(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return plugin.name();
    }

    @Override
    public EngineBridge<?, ?, ?> bridge(CatalogSnapshot snapshot) {
        return plugin.bridge(snapshot);
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return plugin.operatorTable();
    }
}
