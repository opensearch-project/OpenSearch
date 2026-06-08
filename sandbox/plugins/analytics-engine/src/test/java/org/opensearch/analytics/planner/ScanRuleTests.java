/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Set;

/**
 * Tests for table scan rule: backend assignment and field storage resolution.
 */
public class ScanRuleTests extends BasePlannerRulesTests {

    public void testScanResolvesBackendAndFieldStorage() {
        PlannerContext context = buildContext("parquet", intFields());
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode result = unwrapExchange(runPlanner(stubScan(table), context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        assertPipelineViableBackends(result, List.of(OpenSearchTableScan.class), Set.of(MockDataFusionBackend.NAME));
        OpenSearchTableScan scan = (OpenSearchTableScan) result;
        assertEquals(2, scan.getOutputFieldStorage().size());
        assertEquals("status", scan.getOutputFieldStorage().get(0).getFieldName());
        assertEquals("size", scan.getOutputFieldStorage().get(1).getFieldName());
    }
}
