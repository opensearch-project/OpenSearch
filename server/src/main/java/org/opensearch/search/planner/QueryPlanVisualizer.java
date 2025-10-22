/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Visualizes query plans for debugging and analysis.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class QueryPlanVisualizer {

    /**
     * Converts a query plan to a string representation.
     */
    public static String toString(QueryPlanNode root) {
        if (root == null) {
            return "Empty plan";
        }

        StringBuilder sb = new StringBuilder();
        buildString(root, sb, "", true);
        return sb.toString();
    }

    private static void buildString(QueryPlanNode node, StringBuilder sb, String prefix, boolean isLast) {
        sb.append(prefix);
        sb.append(isLast ? "└── " : "├── ");
        sb.append(node.getType().name());
        sb.append(" [cost=").append(String.format(Locale.ROOT, "%.2f", node.getEstimatedCost().getTotalCost())).append("]");

        // Add additional info based on node type
        QueryPlanProfile profile = node.getProfile();
        Map<String, Object> attrs = profile.getAttributes();
        if (attrs.containsKey("field")) {
            sb.append(" field=").append(attrs.get("field"));
        }
        if (attrs.containsKey("value")) {
            sb.append(" value=").append(attrs.get("value"));
        }

        sb.append("\n");

        List<QueryPlanNode> children = node.getChildren();
        for (int i = 0; i < children.size(); i++) {
            boolean childIsLast = i == children.size() - 1;
            String childPrefix = prefix + (isLast ? "    " : "│   ");
            buildString(children.get(i), sb, childPrefix, childIsLast);
        }
    }

    /**
     * Converts a query plan to a JSON-like map structure.
     */
    public static Map<String, Object> toMap(QueryPlanNode root) {
        if (root == null) {
            return null;
        }

        Map<String, Object> map = new HashMap<>();
        map.put("type", root.getType().name());
        map.put("cost", costBreakdown(root.getEstimatedCost()));

        // Add profile attributes
        QueryPlanProfile profile = root.getProfile();
        Map<String, Object> attrs = profile.getAttributes();
        if (!attrs.isEmpty()) {
            map.put("attributes", attrs);
        }

        // Add children
        List<QueryPlanNode> children = root.getChildren();
        if (!children.isEmpty()) {
            List<Map<String, Object>> childMaps = new ArrayList<>();
            for (QueryPlanNode child : children) {
                childMaps.add(toMap(child));
            }
            map.put("children", childMaps);
        }

        return map;
    }

    private static Map<String, Object> costBreakdown(QueryCost cost) {
        Map<String, Object> breakdown = new HashMap<>();
        breakdown.put("total", cost.getTotalCost());
        breakdown.put("lucene_docs", cost.getLuceneCost());
        breakdown.put("cpu", cost.getCpuCost());
        breakdown.put("memory_bytes", cost.getMemoryCost());
        breakdown.put("io", cost.getIoCost());
        breakdown.put("is_estimate", cost.isEstimate());
        return breakdown;
    }

    /**
     * Analyzes a query plan and returns optimization suggestions.
     */
    public static List<String> analyzePlan(QueryPlanNode root) {
        List<String> suggestions = new ArrayList<>();
        analyzePlanRecursive(root, suggestions, 0);
        return suggestions;
    }

    private static void analyzePlanRecursive(QueryPlanNode node, List<String> suggestions, int depth) {
        // Check for expensive operations
        if (node.getType().isComputationallyExpensive()) {
            double cost = node.getEstimatedCost().getTotalCost();
            if (cost > 10000) {
                suggestions.add(
                    String.format(
                        Locale.ROOT,
                        "Expensive %s query detected (cost=%.0f). Consider optimizing or adding filters.",
                        node.getType().name(),
                        cost
                    )
                );
            }
        }

        // Check for match_all in scoring context
        if (node.getType() == QueryNodeType.MATCH_ALL && depth > 0) {
            suggestions.add("match_all query found in sub-query. Consider removing if not needed.");
        }

        // Check for deeply nested boolean queries
        if (node.getType() == QueryNodeType.BOOLEAN && depth > 3) {
            suggestions.add("Deeply nested boolean query detected. Consider flattening the structure.");
        }

        // Recurse to children
        for (QueryPlanNode child : node.getChildren()) {
            analyzePlanRecursive(child, suggestions, depth + 1);
        }
    }
}
