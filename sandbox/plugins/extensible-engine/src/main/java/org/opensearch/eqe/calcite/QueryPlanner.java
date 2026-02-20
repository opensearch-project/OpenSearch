/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.calcite;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.opensearch.cluster.service.ClusterService;

/**
 * Resolves a serialized JSON plan into a Calcite {@link RelNode}.
 *
 * // TODO: Add RBO/CBO and return optimized RelNode
 *
 */
public class QueryPlanner {

    private final OpenSearchSchemaBuilder schemaBuilder;

    public QueryPlanner(ClusterService clusterService) {
        this.schemaBuilder = new OpenSearchSchemaBuilder(clusterService);
    }

    /**
     * Resolve a JSON RelNode plan into a Calcite {@link RelNode}.
     *
     * @param jsonPlan JSON string produced by {@code RelJsonWriter}
     * @return the deserialized Calcite logical plan
     */
    public RelNode resolve(String jsonPlan) {
        CalciteCatalogReader catalogReader = schemaBuilder.buildCatalogReader();
        return CalcitePlanConverter.fromJson(jsonPlan, catalogReader);
    }
}
