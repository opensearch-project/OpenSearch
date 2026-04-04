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
 * @param queryId   unique identifier for this query. Currently a random UUID.
 *                  Future: accept a user-provided ID or generate a cluster-wide
 *                  unique ID (e.g., node-local counter + nodeId prefix).
 * @param rootStage the coordinator/root stage
 * @opensearch.internal
 */
public record QueryDAG(String queryId, Stage rootStage) {

    /**
     * Returns a human-readable representation of the entire DAG showing
     * every stage with its fragment indented.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("QueryDAG(queryId=").append(queryId).append(")\n");
        appendStage(builder, rootStage, 0);
        return builder.toString();
    }

    private static void appendStage(StringBuilder builder, Stage stage, int depth) {
        String indent = "  ".repeat(depth);
        builder.append(indent).append("Stage ").append(stage.getStageId());
        if (stage.getExchangeInfo() != null) {
            builder.append(" [exchange=").append(stage.getExchangeInfo()).append("]");
        } else {
            builder.append(" [root]");
        }
        builder.append("\n");

        if (stage.getFragment() != null) {
            String fragmentStr = RelOptUtil.toString(stage.getFragment());
            for (String line : fragmentStr.split("\n")) {
                if (!line.isEmpty()) {
                    builder.append(indent).append("  ").append(line).append("\n");
                }
            }
        } else {
            builder.append(indent).append("  <gather>\n");
        }

        if (!stage.getPlanAlternatives().isEmpty()) {
            builder.append(indent).append("  Alternatives (").append(stage.getPlanAlternatives().size()).append("):\n");
            for (int idx = 0; idx < stage.getPlanAlternatives().size(); idx++) {
                StagePlan plan = stage.getPlanAlternatives().get(idx);
                builder.append(indent).append("  [").append(idx).append("] ");
                String planStr = RelOptUtil.toString(plan.resolvedFragment());
                String[] lines = planStr.split("\n");
                builder.append(lines[0]).append("\n");
                for (int lineIdx = 1; lineIdx < lines.length; lineIdx++) {
                    if (!lines[lineIdx].isEmpty()) {
                        builder.append(indent).append("      ").append(lines[lineIdx]).append("\n");
                    }
                }
            }
        }

        for (Stage child : stage.getChildStages()) {
            appendStage(builder, child, depth + 1);
        }
    }
}
