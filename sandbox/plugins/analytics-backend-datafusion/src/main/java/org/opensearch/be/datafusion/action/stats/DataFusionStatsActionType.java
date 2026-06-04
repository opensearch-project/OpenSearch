/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.ActionType;

/**
 * Action type for the DataFusion cluster stats transport action.
 *
 * <p>Singleton constant identifying the transport action that collects
 * DataFusion runtime statistics from all (or specific) nodes in the cluster.
 *
 * @opensearch.internal
 */
public class DataFusionStatsActionType extends ActionType<DataFusionStatsNodesResponse> {

    public static final String NAME = "cluster:monitor/_analytics_backend_datafusion/stats";
    public static final DataFusionStatsActionType INSTANCE = new DataFusionStatsActionType();

    private DataFusionStatsActionType() {
        super(NAME, DataFusionStatsNodesResponse::new);
    }
}
