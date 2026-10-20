/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

/**
 * Shared scaffolding for plan-shape tests: context helpers + assertion + line
 * normalization. Subclasses are organized per operator (Scan/Filter/Project/Sort/
 * Aggregate/Window/Join/Union) plus {@link PlanShapeTests} for combinations.
 *
 * <p>Tests build {@code LogicalXxx} rels directly via {@link BasePlannerRulesTests}
 * helpers — no PPL frontend involved. The assertion compares the After-CBO RelNode
 * tree to a string literal.
 */
abstract class PlanShapeTestBase extends BasePlannerRulesTests {

    /** 1-shard "test_index" with int fields. */
    protected PlannerContext singleShardContext() {
        return buildContext("parquet", 1, intFields());
    }

    /** 2-shard "test_index" — the default for "multi-shard" tests. */
    protected PlannerContext multiShardContext() {
        return buildContext("parquet", 2, intFields());
    }

    /** 3-shard "test_index" — used for nested-stage cases that interact with shard count. */
    protected PlannerContext threeShardContext() {
        return buildContext("parquet", 3, intFields());
    }

    /** Per-index shard counts for join / union cases that need independent table layouts. */
    protected PlannerContext perIndexContext(Map<String, Integer> shardCountByIndex) {
        return buildContextPerIndex("parquet", shardCountByIndex);
    }

    /**
     * Asserts the plan tree matches {@code expected} (whitespace-normalized line by line).
     * On mismatch the assertion message includes the full actual plan so failures surface
     * the entire tree without truncation.
     */
    protected static void assertPlanShape(String expected, RelNode actual) {
        String actualStr = RelOptUtil.toString(actual);
        String normalizedExpected = normalizeLines(expected);
        String normalizedActual = normalizeLines(actualStr);
        assertEquals("Plan shape mismatch — actual:\n" + actualStr, normalizedExpected, normalizedActual);
    }

    private static String normalizeLines(String s) {
        StringBuilder sb = new StringBuilder();
        for (String line : s.split("\n", -1)) {
            int end = line.length();
            while (end > 0 && (line.charAt(end - 1) == ' ' || line.charAt(end - 1) == '\t'))
                end--;
            sb.append(line, 0, end).append('\n');
        }
        while (sb.length() >= 2 && sb.charAt(sb.length() - 1) == '\n' && sb.charAt(sb.length() - 2) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }
}
