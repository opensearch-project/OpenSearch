/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.plugins.Plugin;

/**
 * DataFusion native execution engine plugin (sandbox stub).
 *
 * @deprecated This sandbox stub is superseded by
 * {@code org.opensearch.datafusion.DataFusionPlugin} in the engine-datafusion plugin,
 * which provides full lifecycle management via {@code DataFusionService},
 * snapshot-based bridge construction, and native JNI execution.
 * This class will be removed once the sandbox analytics-backend-datafusion module is retired.
 */
@Deprecated
public class DataFusionPlugin extends Plugin implements AnalyticsBackEndPlugin {

    /** Creates a new DataFusion plugin. */
    public DataFusionPlugin() {}

    private final DataFusionBridge bridge = new DataFusionBridge();

    @Override
    public String name() {
        return "datafusion";
    }

    /**
     * @deprecated Use {@code org.opensearch.datafusion.DataFusionPlugin#bridge(CatalogSnapshot)} instead.
     */
    @Deprecated
    @Override
    public EngineBridge<?, ?, ?> bridge(CatalogSnapshot snapshot) {
        return bridge;
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return null;
    }
}
