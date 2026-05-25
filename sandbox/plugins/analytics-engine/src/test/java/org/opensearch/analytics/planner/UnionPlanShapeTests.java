/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchUnion}.
 *
 * <p>Co-location fast path applies when every arm is a 1-shard scan of the same table —
 * Union runs at the shard, no per-arm ER, single ER above for the root demand. Otherwise
 * each arm is gathered to {@code COORDINATOR+SINGLETON} via {@code TraitDef.convert}.
 */
public class UnionPlanShapeTests extends PlanShapeTestBase {

    public void testUnion_sameTable_1shard() {
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("test_index", "status", "size")), stubScan(mockTable("test_index", "status", "size"))),
            /* all */ true
        );
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 1));
        assertPlanShape("""
            OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testUnion_sameTable_2shard() {
        // 2-shard inputs fail the co-location predicate (shardCount > 1). General path:
        // each arm gathered to coord, Union at coord.
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("test_index", "status", "size")), stubScan(mockTable("test_index", "status", "size"))),
            /* all */ true
        );
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 2));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_differentTables_1shard() {
        // Different tables. tableIds differ → general path.
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("left_idx", "status", "size")), stubScan(mockTable("right_idx", "status", "size"))),
            /* all */ true
        );
        RelNode result = runPlanner(union, unionContextTwoIndices(Map.of("left_idx", 1, "right_idx", 1)));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_differentTables_2shard() {
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("left_idx", "status", "size")), stubScan(mockTable("right_idx", "status", "size"))),
            /* all */ true
        );
        RelNode result = runPlanner(union, unionContextTwoIndices(Map.of("left_idx", 2, "right_idx", 2)));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_threeArms_sameTable_1shard() {
        RelNode union = LogicalUnion.create(
            List.of(
                stubScan(mockTable("test_index", "status", "size")),
                stubScan(mockTable("test_index", "status", "size")),
                stubScan(mockTable("test_index", "status", "size"))
            ),
            /* all */ true
        );
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 1));
        assertPlanShape("""
            OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testUnion_threeArms_mixedTables_1shard() {
        // a, b, a — three 1-shard arms but two distinct tableIds → general path.
        RelNode union = LogicalUnion.create(
            List.of(
                stubScan(mockTable("a", "status", "size")),
                stubScan(mockTable("b", "status", "size")),
                stubScan(mockTable("a", "status", "size"))
            ),
            /* all */ true
        );
        RelNode result = runPlanner(union, unionContextTwoIndices(Map.of("a", 1, "b", 1)));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[a]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[b]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[a]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testUnion_oneEmptyArm_collapsed() {
        // Empty Values arm should be dropped — UnionRule collapses to a single non-empty
        // arm (just the scan) when only one input remains.
        RelDataType rowType = typeFactory.builder()
            .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("size", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();
        RelNode emptyArm = LogicalValues.createEmpty(cluster, rowType);
        RelNode union = LogicalUnion.create(List.of(stubScan(mockTable("test_index", "status", "size")), emptyArm), /* all */ true);
        RelNode result = runPlanner(union, unionContextSingleIndex("test_index", 1));
        assertPlanShape("""
            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    // ── Context helpers ───────────────────────────────────────────────────────

    private PlannerContext unionContextSingleIndex(String indexName, int shardCount) {
        return buildContextPerIndex("parquet", Map.of(indexName, shardCount), intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    private PlannerContext unionContextTwoIndices(Map<String, Integer> shardsByIndex) {
        return buildContextPerIndex("parquet", shardsByIndex, intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    /** Mock DF backend with EngineCapability.UNION — needed because UNION is opt-in. */
    private static final class UnionCapableBackend extends MockDataFusionBackend {
        @Override
        protected Set<EngineCapability> supportedEngineCapabilities() {
            Set<EngineCapability> caps = new HashSet<>(super.supportedEngineCapabilities());
            caps.add(EngineCapability.UNION);
            return caps;
        }
    }
}
