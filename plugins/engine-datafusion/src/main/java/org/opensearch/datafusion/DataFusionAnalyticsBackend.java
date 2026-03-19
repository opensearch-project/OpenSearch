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
 * SPI adapter that delegates to the real {@link DataFusionPlugin} instance,
 * or to a child backend plugin if one was discovered via ExtensiblePlugin.
 */
public class DataFusionAnalyticsBackend implements AnalyticsBackEndPlugin {

    private final DataFusionPlugin plugin;

    public DataFusionAnalyticsBackend(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    private AnalyticsBackEndPlugin delegate() {
        // If a child plugin (e.g. sandbox analytics-backend-datafusion) registered,
        // delegate to it so it can override the bridge implementation.
        if (plugin.getChildBackends().isEmpty() == false) {
            return plugin.getChildBackends().get(0);
        }
        return plugin;
    }

    @Override
    public String name() {
        return plugin.name();
    }

    @Override
    public EngineBridge<?, ?, ?> bridge(CatalogSnapshot snapshot) {
        return delegate().bridge(snapshot);
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return delegate().operatorTable();
    }

    @Override
    public boolean supportsSearchExecEngine() {
        return delegate().supportsSearchExecEngine();
    }
}
