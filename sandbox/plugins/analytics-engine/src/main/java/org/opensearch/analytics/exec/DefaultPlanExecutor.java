/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link QueryPlanExecutor} default implementation.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);

    /**
     * Creates a plan executor with the given back-end plugins.
     *
     * @param backEnds registered back-end engine plugins
     */
    public DefaultPlanExecutor(List<AnalyticsBackEndPlugin> backEnds) {
        // TODO: use back-ends
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        RelNode fragment = logicalFragment;
        int fieldCount = fragment.getRowType().getFieldCount();

        logger.debug("[DefaultPlanExecutor] Executing fragment with {} fields: {}", fieldCount, fragment.explain());

        // Stub: return empty result set.
        return new ArrayList<>();
    }
}
