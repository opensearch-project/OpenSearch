/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.be.datafusion.action.PartialPlanAction;
import org.opensearch.be.datafusion.action.TransportPartialPlanAction;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;

/**
 * DataFusion native execution engine plugin.
 * Registers the partial-plan stream transport action for data-node execution.
 */
public class DataFusionPlugin extends Plugin implements AnalyticsBackEndPlugin, ActionPlugin {

    public DataFusionPlugin() {}

    private final DataFusionBridge bridge = new DataFusionBridge();

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineBridge<?, ?, ?> bridge() {
        return bridge;
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return null;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(PartialPlanAction.INSTANCE, TransportPartialPlanAction.class));
    }
}
