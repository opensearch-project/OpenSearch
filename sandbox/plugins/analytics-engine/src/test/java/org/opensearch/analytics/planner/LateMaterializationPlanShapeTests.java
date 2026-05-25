/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Plan-shape tests for the QTF (late-materialization) post-CBO rewriter. Drive SQL through
 * the full planner ({@link PlannerImpl#runAllOptimizations}) and assert the structural
 * post-conditions QTF must produce — wrapper presence, Scan rowType narrowing with
 * {@code ___row_id} appended, ER output declaring {@code ___ugsi}, wrapper output
 * stripping both helpers and exposing keepBelow + fetched cols, outer Project RexInputRefs
 * remapped to wrapper indices.
 *
 * <p>Each test declares the SQL plus QTF expectations; {@link Expect}'s static factories
 * compose into one {@code assertQtfFired(...)} call. Tests that should not fire QTF call
 * {@code assertQtfDeclined(...)}.
 */
public class LateMaterializationPlanShapeTests extends BasePlannerRulesTests {

    // ── Tests that fire QTF ────────────────────────────────────────────

    public void testQtfFires_simpleSortProject() {
        // Pure-project column (URL) above the anchor; sort key EventDate retained below.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("EventDate", "URL"),
            Expect.outerProjectExprIndices(1, 0)
        );
    }

    public void testQtfFires_withWhere() {
        // Filter col (CounterID) joins keepBelow; URL stays a fetch col.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("CounterID", "EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("CounterID", "EventDate", "URL"),
            Expect.outerProjectExprIndices(2, 1)
        );
    }

    public void testQtfFires_sortColAlsoProjected() {
        // EventDate is both sort key and projected — surfaces via wrapper passthrough, not via fetch.
        assertQtfFired(
            "SELECT EventDate, URL FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("EventDate", "URL"),
            Expect.outerProjectExprIndices(0, 1)
        );
    }

    public void testQtfFires_singleShard() {
        // Single shard: no ER, so no ___ugsi declared. Narrowed Scan + wrapper still apply.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10",
            1,
            Expect.scanCols("EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(false),
            Expect.wrapperOutput("EventDate", "URL"),
            Expect.outerProjectExprIndices(1, 0)
        );
    }

    public void testQtfFires_descendingSort() {
        // DESC collation: anchor identification + direction preservation.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate DESC LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true),
            Expect.collationDirections(RelFieldCollation.Direction.DESCENDING)
        );
    }

    public void testQtfFires_multiKeySort() {
        // Multi-key sort — both keys land in keepBelow, neither fetched.
        assertQtfFired(
            "SELECT URL, EventDate, CounterID FROM hits ORDER BY EventDate, CounterID LIMIT 10",
            2,
            Expect.scanCols("CounterID", "EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true)
        );
    }

    public void testQtfFires_filterColAlsoProjected() {
        // Filter col (CounterID) is also projected — lands in keepBelow once, surfaces via wrapper.
        assertQtfFired(
            "SELECT URL, CounterID FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("CounterID", "EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("CounterID", "EventDate", "URL")
        );
    }

    public void testQtfFires_offset() {
        // OFFSET preserved on the rebuilt anchor Sort.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10 OFFSET 5",
            2,
            Expect.scanCols("EventDate"),
            Expect.fetchList("URL"),
            Expect.erHasUgsi(true)
        );
    }

    // ── Tests that decline QTF ─────────────────────────────────────────

    public void testQtfDeclined_pureLimit() {
        assertQtfDeclined("SELECT URL FROM hits LIMIT 10", 2);
    }

    public void testQtfDeclined_emptyFetchedSet() {
        assertQtfDeclined("SELECT EventDate FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_aggregateBelowSort() {
        assertQtfDeclined("SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID ORDER BY CounterID LIMIT 10", 2);
    }

    public void testQtfDeclined_windowInOuterProject() {
        // ROW_NUMBER not wired into WindowFunction.fromSqlKind — use SUM() OVER ().
        assertQtfDeclined("SELECT URL, SUM(ParamPrice) OVER () AS sp FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_expressionProjectBelowAnchor() {
        // Non-passthrough Project below the anchor — QTF rewriter bails (TODO follow-up).
        assertQtfDeclined("SELECT URL FROM hits ORDER BY (CounterID + 1) LIMIT 10", 2);
    }

    // ── Composable assert API ──────────────────────────────────────────

    /**
     * Runs SQL through the planner; asserts QTF fired (wrapper present), then runs each
     * supplied {@link Expect} against the optimized plan.
     */
    private void assertQtfFired(String sql, int shardCount, Expect... expectations) {
        RelNode optimized = optimize(sql, shardCount);
        String planText = RelOptUtil.toString(optimized);
        Inspector ctx = new Inspector(optimized);
        if (ctx.wrapper == null) {
            fail("Expected QTF wrapper in plan for SQL: " + sql + "\nPlan:\n" + planText);
        }
        for (Expect e : expectations) {
            e.check(ctx, sql, planText);
        }
    }

    private void assertQtfDeclined(String sql, int shardCount) {
        RelNode optimized = optimize(sql, shardCount);
        Inspector ctx = new Inspector(optimized);
        if (ctx.wrapper != null) {
            fail("QTF should NOT have fired for SQL: " + sql + "\nPlan:\n" + RelOptUtil.toString(optimized));
        }
    }

    /** Composable post-condition. Static factory per check, all sharing the {@link Inspector} context. */
    private abstract static class Expect {
        abstract void check(Inspector ctx, String sql, String planText);

        /** Scan rowType (post-narrowing) carries exactly these original cols + {@code ___row_id} at the end. */
        static Expect scanCols(String... expectedNamesInOrder) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<String> actual = fieldNames(ctx.scan.getRowType().getFieldList());
                    List<String> expected = new ArrayList<>(Arrays.asList(expectedNamesInOrder));
                    expected.add(OpenSearchLateMaterialization.ROW_ID_FIELD);
                    if (!expected.equals(actual)) {
                        fail("Scan rowType mismatch.\n  expected: " + expected + "\n  actual:   " + actual + "\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                }
            };
        }

        /** Wrapper carries exactly these field names in the fetch list (in order). */
        static Expect fetchList(String... expectedNamesInOrder) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<String> actual = fieldNames(ctx.wrapper.getFetchList());
                    List<String> expected = Arrays.asList(expectedNamesInOrder);
                    if (!expected.equals(actual)) {
                        fail("Wrapper fetchList mismatch.\n  expected: " + expected + "\n  actual:   " + actual + "\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                }
            };
        }

        /** Wrapper output rowType is exactly these names — helpers MUST be stripped. */
        static Expect wrapperOutput(String... expectedNamesInOrder) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<String> actual = fieldNames(ctx.wrapper.getRowType().getFieldList());
                    List<String> expected = Arrays.asList(expectedNamesInOrder);
                    if (!expected.equals(actual)) {
                        fail("Wrapper output rowType mismatch.\n  expected: " + expected + "\n  actual:   " + actual + "\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                    if (actual.contains(OpenSearchLateMaterialization.ROW_ID_FIELD)
                        || actual.contains(OpenSearchLateMaterialization.UGSI_FIELD)) {
                        fail("Wrapper output leaked helper col(s) " + actual + "\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                }
            };
        }

        /** ER carries (or does not carry) {@code ___ugsi} as its last column. */
        static Expect erHasUgsi(boolean expected) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    if (expected) {
                        if (ctx.er == null) fail("Expected ER in plan but none found.\nSQL: " + sql + "\nPlan:\n" + plan);
                        List<String> erFields = fieldNames(ctx.er.getRowType().getFieldList());
                        if (!OpenSearchLateMaterialization.UGSI_FIELD.equals(erFields.get(erFields.size() - 1))) {
                            fail("ER output missing ___ugsi as last col. Fields: " + erFields + "\nSQL: " + sql + "\nPlan:\n" + plan);
                        }
                    } else {
                        if (ctx.er != null) fail("Did not expect ER in plan (single-shard).\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                }
            };
        }

        /** Outer Project (immediately above wrapper) RexInputRef indices, in expression order. */
        static Expect outerProjectExprIndices(int... expectedIndices) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    if (ctx.outerProject == null) {
                        fail("No outer Project above wrapper.\nSQL: " + sql + "\nPlan:\n" + plan);
                        return;
                    }
                    int[] actual = new int[ctx.outerProject.getProjects().size()];
                    for (int i = 0; i < actual.length; i++) {
                        if (ctx.outerProject.getProjects().get(i) instanceof RexInputRef ref) {
                            actual[i] = ref.getIndex();
                        } else {
                            fail("Outer Project expr [" + i + "] is not a RexInputRef: " + ctx.outerProject.getProjects().get(i)
                                + "\nSQL: " + sql + "\nPlan:\n" + plan);
                            return;
                        }
                    }
                    if (!Arrays.equals(expectedIndices, actual)) {
                        fail("Outer Project RexInputRef indices mismatch.\n  expected: " + Arrays.toString(expectedIndices)
                            + "\n  actual:   " + Arrays.toString(actual) + "\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                }
            };
        }

        /** Anchor Sort collation directions in order. */
        static Expect collationDirections(RelFieldCollation.Direction... expected) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<RelFieldCollation> fc = ctx.anchor.getCollation().getFieldCollations();
                    if (fc.size() != expected.length) {
                        fail("Anchor collation size mismatch. expected=" + expected.length + " actual=" + fc.size()
                            + "\nSQL: " + sql + "\nPlan:\n" + plan);
                    }
                    for (int i = 0; i < expected.length; i++) {
                        if (fc.get(i).getDirection() != expected[i]) {
                            fail("Anchor collation[" + i + "] direction mismatch. expected=" + expected[i]
                                + " actual=" + fc.get(i).getDirection() + "\nSQL: " + sql + "\nPlan:\n" + plan);
                        }
                    }
                }
            };
        }
    }

    /**
     * Single-pass walker that captures the QTF-relevant nodes from an optimized plan:
     * the wrapper, anchor (= wrapper's input Sort), scan, ER (between anchor and scan if
     * any), and outer Project (parent of wrapper, if any).
     */
    private static final class Inspector {
        OpenSearchLateMaterialization wrapper;
        OpenSearchSort anchor;
        OpenSearchTableScan scan;
        OpenSearchExchangeReducer er;
        OpenSearchProject outerProject;

        Inspector(RelNode root) {
            new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof OpenSearchLateMaterialization w && wrapper == null) {
                        wrapper = w;
                        if (parent instanceof OpenSearchProject p) outerProject = p;
                        if (w.getInput() instanceof OpenSearchSort s) anchor = s;
                    }
                    if (node instanceof OpenSearchExchangeReducer r && er == null) er = r;
                    if (node instanceof OpenSearchTableScan t && scan == null) scan = t;
                    super.visit(node, ordinal, parent);
                }
            }.go(root);
        }
    }

    private static List<String> fieldNames(List<RelDataTypeField> fields) {
        List<String> out = new ArrayList<>(fields.size());
        for (RelDataTypeField f : fields) out.add(f.getName());
        return out;
    }

    private RelNode optimize(String sql, int shardCount) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith(ClickBench.INDEX, ClickBench.BASIC_FIELDS, "parquet", shardCount);
        PlannerContext context = new PlannerContext(
            new CapabilityRegistry(List.of(DATAFUSION, LUCENE), FieldStorageResolver::new),
            state,
            false
        );
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);
        return PlannerImpl.runAllOptimizations(parsed, context);
    }
}
