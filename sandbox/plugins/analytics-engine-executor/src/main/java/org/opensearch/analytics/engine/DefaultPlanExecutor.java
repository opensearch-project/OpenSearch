/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.engine;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link QueryPlanExecutor} implementation in the query-engine module.
 * Translates logical fragments (RelNode trees) into OpenSearch query
 * operations and returns result rows.
 *
 * <p>The parameters are typed as {@code Object} because the {@link QueryPlanExecutor}
 * interface lives in {@code server/plugins} which must not depend on Calcite.
 * This implementation casts them to the appropriate Calcite types internally.</p>
 *
 * <p>Currently a stub that logs the received logical plan and returns
 * placeholder rows matching the fragment's field count.</p>
 * // TODO: call this something.. better
 */
public class DefaultPlanExecutor implements QueryPlanExecutor {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);

    public DefaultPlanExecutor(List<AnalyticsBackEndPlugin> backEnds) {}

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // TODO: This is a stub for now, just logs the RelNode fragment.
        RelNode fragment = (RelNode) logicalFragment;
        int fieldCount = fragment.getRowType().getFieldCount();

        // TODO: Implement planner & scheduler components for now this just looks up a back-end and executes
        // We will decide which back-end to invoke based on planner rules / physical operators / compatibility checks etc

        logger.info("[DefaultPlanExecutor] Executing fragment with {} fields: {}", fieldCount, fragment.explain());

        // Stub: return an empty result set.
        // A real implementation would translate the fragment into OpenSearch
        // query operations, execute against OpenSearch shards, and return rows.
        List<Object[]> rows = new ArrayList<>();
        return rows;
    }
}
