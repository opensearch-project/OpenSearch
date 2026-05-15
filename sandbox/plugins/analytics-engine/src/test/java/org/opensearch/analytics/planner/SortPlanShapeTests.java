/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchSort}.
 *
 * <p>Two flavors:
 * <ul>
 *   <li><b>Collated Sort</b> (ORDER BY) — needs SINGLETON input. Volcano's
 *       {@code OpenSearchSortSplitRule} fires {@code convert(input, COORDINATOR)} which
 *       inserts an ER under the Sort. Sort runs at coord.</li>
 *   <li><b>Pure-LIMIT Sort</b> (no collation, just {@code fetch}) — partition-local
 *       fetch is correct. ER goes above the Sort to gather to coord.</li>
 * </ul>
 */
public class SortPlanShapeTests extends PlanShapeTestBase {

    /**
     * 1-shard collated Sort: data already lives on one node, so an ideal planner would
     * keep the Sort at the shard and ER above. Today we still insert ER under and run
     * Sort at coord — same row count moves either way, so this is a benign suboptimality.
     */
    @AwaitsFix(bugUrl = "Optimization: collated Sort over 1-shard SHARD+SINGLETON should stay at shard.")
    public void testCollatedSort_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ -1);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testCollatedSort_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeSort(scan, /* fetch */ -1);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testPureLimit_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeLimit(scan, 10);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testPureLimit_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeLimit(scan, 10);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * 1-shard sort + LIMIT pushdown. Today the LIMIT runs at coord (above the inner Sort
     * which itself runs at coord). Optimal: the entire {@code Sort + fetch} chain stays
     * at the shard and only the top-K rows are transported.
     */
    @AwaitsFix(bugUrl = "Optimization: top-K (collated Sort + outer fetch) over 1-shard SHARD+SINGLETON should stay at shard.")
    public void testSortPlusLimit_1shard() {
        RelNode result = runPlanner(buildSortPlusLimit(), singleShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testSortPlusLimit_2shard() {
        RelNode result = runPlanner(buildSortPlusLimit(), multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** PPL frontend emits two LogicalSort nodes for {@code | sort x | head 10}: an outer
     *  pure-fetch and an inner collated. Constructed by hand here for the same shape. */
    private RelNode buildSortPlusLimit() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        return makeLimit(innerSort, 10);
    }

    public void testCollatedSort_2shard_descending() {
        // sort -status — DESC NULLS LAST is Calcite's default for DESC.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[DESC], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
