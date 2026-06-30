/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchTableScan}
 * standing alone. Multi-shard scans gather to coord via ER; single-shard scans satisfy the
 * locality-agnostic root SINGLETON demand directly, no ER inserted.
 */
public class ScanPlanShapeTests extends PlanShapeTestBase {

    public void testBareScan_1shard() {
        RelNode plan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testBareScan_2shard() {
        RelNode plan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
