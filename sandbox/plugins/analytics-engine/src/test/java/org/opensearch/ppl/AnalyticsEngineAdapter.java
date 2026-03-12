/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl;

import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.SchemaProvider;
import org.opensearch.common.inject.Inject;

public class AnalyticsEngineAdapter {

    public QueryPlanExecutor getExecutor() {
        return executor;
    }

    public SchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public EngineCapabilities getCapabilities() {
        return capabilities;
    }

    QueryPlanExecutor executor;
    SchemaProvider schemaProvider;
    EngineCapabilities capabilities;

    @Inject(optional = true)
    void setEngineComponents(QueryPlanExecutor executor, SchemaProvider schemaProvider, EngineCapabilities engineCapabilities) {
        this.executor = executor;
        this.schemaProvider = schemaProvider;
        this.capabilities = engineCapabilities;
    }
}
