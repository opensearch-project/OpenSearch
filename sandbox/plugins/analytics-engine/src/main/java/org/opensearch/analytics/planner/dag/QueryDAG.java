/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;

/**
 * Root of the query execution DAG. Recursive tree of {@link Stage}s.
 *
 * @param queryId   unique identifier for this query
 * @param rootStage the coordinator root stage
 * @opensearch.internal
 */
public record QueryDAG(String queryId, Stage rootStage) {

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("QueryDAG(queryId=").append(queryId).append(")\n");
        appendStage(sb, rootStage, 0);
        return sb.toString();
    }

    private static void appendStage(StringBuilder sb, Stage stage, int depth) {
        String indent = "  ".repeat(depth);
        sb.append(indent).append("Stage ").append(stage.getStageId());
        if (stage.getExchangeInfo() != null) {
            sb.append(" exchange=").append(stage.getExchangeInfo().distributionType());
        }
        sb.append("\n");
        if (stage.getFragment() != null) {
            for (String line : RelOptUtil.toString(stage.getFragment()).split("\n")) {
                if (!line.isEmpty()) sb.append(indent).append("  ").append(line).append("\n");
            }
        }
        for (Stage child : stage.getChildStages()) {
            appendStage(sb, child, depth + 1);
        }
    }
}
