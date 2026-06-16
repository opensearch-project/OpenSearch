/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rules.OpenSearchSortPushdownRewriter}.
 *
 * <p>Fires for non-aggregate {@code ORDER BY .. LIMIT}: a copy of the collated Sort
 * (with its effective fetch) is inserted below the ER so shards ship local top-N.
 */
public class SortPushdownPlanShapeTests extends PlanShapeTestBase {

    private RelNode fetchLiteralSort(RelNode input, int fetch) {
        return LogicalSort.create(
            input,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    /** A collation-free LIMIT — PPL {@code head N} / SQL {@code LIMIT N} with no {@code ORDER BY}. */
    private RelNode bareLimit(RelNode input, int fetch) {
        return LogicalSort.create(
            input,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    /**
     * Bare {@code head N} (no ORDER BY): a collation-free fetch must still be pushed below the ER so
     * each shard caps locally at N rows. Without this, every shard streams its <em>entire</em> scan to
     * the coordinator, which then trims to N — fetching every row defeats the limit. A limit without an
     * order is always safe to push: the coordinator's N-row result is a subset of the union of each
     * shard's N-row result, regardless of which rows each shard keeps.
     */
    public void testBareLimit_2shard_pushed() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(bareLimit(scan, 10), multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Bare LIMIT, single shard: no ER to push below — coordinator keeps the only Sort. */
    public void testBareLimit_singleShard_notPushed() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(bareLimit(scan, 10), singleShardContext());
        assertEquals(1, RelOptUtil.toString(result).lines().filter(l -> l.contains("OpenSearchSort")).count());
    }

    /** Bare LIMIT with OFFSET: shard fetch widens to offset+fetch, offset stays on the coordinator. */
    public void testBareLimitOffset_2shard_widenedFetch() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.EMPTY,
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        String p = RelOptUtil.toString(runPlanner(plan, multiShardContext()));
        assertEquals("coord + shard Sort", 2, p.lines().filter(l -> l.contains("OpenSearchSort")).count());
        assertTrue("coordinator keeps offset=5", p.contains("offset=[5]"));
        int erIdx = p.indexOf("ExchangeReducer");
        int widenedIdx = p.indexOf("fetch=[15]");
        assertTrue("shard Sort widened to offset+fetch=15 below ER", erIdx >= 0 && widenedIdx > erIdx);
        assertFalse("shard Sort must not carry an offset", p.substring(widenedIdx).contains("offset="));
    }

    /** Bare LIMIT over UNION ALL: the collation-free fetch is pushed below each arm's ER. */
    public void testBareLimitUnionAll_2shard_pushedIntoBothArms() {
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("test_index", "status", "size")), stubScan(mockTable("test_index", "status", "size"))),
            true
        );
        String p = RelOptUtil.toString(runPlanner(bareLimit(union, 10), unionContext("test_index", 2)));
        assertEquals("coordinator + one Sort per arm, plan:\n" + p, 3, p.lines().filter(l -> l.contains("OpenSearchSort")).count());
        assertEquals("one ER per arm, plan:\n" + p, 2, p.lines().filter(l -> l.contains("ExchangeReducer")).count());
        String[] lines = p.split("\n", -1);
        for (int i = 0; i + 1 < lines.length; i++) {
            if (lines[i].contains("ExchangeReducer")) {
                assertTrue("a Sort must be pushed below each arm ER, plan:\n" + p, lines[i + 1].contains("OpenSearchSort"));
            }
        }
    }

    /** SQL shape: single Sort(collation, fetch) → identical Sort pushed below ER.
     *  (A LateMaterialization node wraps the top for scans with stored fields — incidental.) */
    public void testSqlSortLimit_2shard_pushed() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(fetchLiteralSort(scan, 10), multiShardContext());
        assertPlanShape(
            """
                OpenSearchLateMaterialization(aboveAnchorPhysicalFields=[[status, size]], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** PPL shape: outer pure-fetch over inner collated → shard Sort gets inner collation + outer fetch. */
    public void testPplSortHead_2shard_pushed() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode innerSort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode outerLimit = LogicalSort.create(
            innerSort,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(outerLimit, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[10], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** SQL shape with OFFSET: shard Sort drops the offset and widens fetch to offset+fetch; coordinator keeps offset. */
    public void testSqlSortLimitOffset_2shard_widenedFetch() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        String p = org.apache.calcite.plan.RelOptUtil.toString(runPlanner(plan, multiShardContext()));
        assertEquals("coord + shard Sort", 2, p.lines().filter(l -> l.contains("OpenSearchSort")).count());
        assertTrue("coordinator keeps offset=5", p.contains("offset=[5]"));
        int erIdx = p.indexOf("ExchangeReducer");
        int widenedIdx = p.indexOf("fetch=[15]");
        assertTrue("shard Sort widened to offset+fetch=15 below ER", erIdx >= 0 && widenedIdx > erIdx);
        assertFalse("shard Sort must not carry an offset", p.substring(widenedIdx).contains("offset="));
    }

    /** UNION ALL: the collated sort is pushed below each arm's ER (split at each point). */
    public void testUnionAll_2shard_pushedIntoBothArms() {
        RelNode union = LogicalUnion.create(
            List.of(stubScan(mockTable("test_index", "status", "size")), stubScan(mockTable("test_index", "status", "size"))),
            true
        );
        String p = RelOptUtil.toString(runPlanner(fetchLiteralSort(union, 10), unionContext("test_index", 2)));
        assertEquals("coordinator + one Sort per arm, plan:\n" + p, 3, p.lines().filter(l -> l.contains("OpenSearchSort")).count());
        assertEquals("one ER per arm, plan:\n" + p, 2, p.lines().filter(l -> l.contains("ExchangeReducer")).count());
        String[] lines = p.split("\n", -1);
        for (int i = 0; i + 1 < lines.length; i++) {
            if (lines[i].contains("ExchangeReducer")) {
                assertTrue("a Sort must be pushed below each arm ER, plan:\n" + p, lines[i + 1].contains("OpenSearchSort"));
            }
        }
    }

    /** Sort over a Join is not pushed — joins are explicitly out of scope. */
    public void testJoin_notPushed() {
        RexNode cond = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = org.apache.calcite.rel.logical.LogicalJoin.create(
            stubScan(mockTable("left_idx", "status", "size")),
            stubScan(mockTable("right_idx", "status", "size")),
            List.of(),
            cond,
            java.util.Set.of(),
            org.apache.calcite.rel.core.JoinRelType.INNER
        );
        String p = RelOptUtil.toString(
            runPlanner(fetchLiteralSort(join, 10), perIndexContext(java.util.Map.of("left_idx", 1, "right_idx", 1)))
        );
        assertEquals("no Sort pushed below a join, plan:\n" + p, 1, p.lines().filter(l -> l.contains("OpenSearchSort")).count());
    }

    /** Collated Sort with no fetch → no transport bound → not pushed. */
    public void testNoFetch_notPushed() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertEquals(1, RelOptUtil.toString(result).lines().filter(l -> l.contains("OpenSearchSort")).count());
    }

    /** Single shard has no ER → nothing to push below. */
    public void testSingleShard_notPushed() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode result = runPlanner(fetchLiteralSort(scan, 10), singleShardContext());
        assertEquals(1, RelOptUtil.toString(result).lines().filter(l -> l.contains("OpenSearchSort")).count());
    }

    /** Aggregate path (Sort over grouped COUNT) is owned by the TopK rewriter — this rewriter must not add a Sort. */
    public void testAggregatePath_notTouched() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode agg = org.apache.calcite.rel.logical.LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(countStarCall())
        );
        RelNode result = runPlanner(fetchLiteralSort(agg, 10), multiShardContext());
        // Only the coordinator Sort — no per-partition Sort from this rewriter (oversampling default 0 disables TopK too).
        assertEquals(1, RelOptUtil.toString(result).lines().filter(l -> l.contains("OpenSearchSort")).count());
    }

    /** Context whose DataFusion backend declares EngineCapability.UNION (mirrors PlanShapeTests). */
    private PlannerContext unionContext(String indexName, int shardCount) {
        return buildContextPerIndex(
            "parquet",
            java.util.Map.of(indexName, shardCount),
            intFields(),
            List.of(new UnionCapableBackend(), LUCENE)
        );
    }

    private static final class UnionCapableBackend extends MockDataFusionBackend {
        @Override
        protected java.util.Set<org.opensearch.analytics.spi.EngineCapability> supportedEngineCapabilities() {
            java.util.Set<org.opensearch.analytics.spi.EngineCapability> caps = new java.util.HashSet<>(
                super.supportedEngineCapabilities()
            );
            caps.add(org.opensearch.analytics.spi.EngineCapability.UNION);
            return caps;
        }
    }
}
