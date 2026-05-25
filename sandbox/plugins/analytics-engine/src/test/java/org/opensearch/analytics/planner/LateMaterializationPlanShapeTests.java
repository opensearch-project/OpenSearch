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
 * Plan-shape tests for the QTF (late-materialization) post-CBO rewriter, v2.
 *
 * <p>Each test drives a SQL string (Calcite dialect) through the full planner and asserts
 * the structural post-conditions QTF must produce:
 * <ul>
 *   <li>Scan rowType narrowed to {@code BelowAnchorPhysicalFields + ___row_id}.</li>
 *   <li>ER output declares {@code ___ugsi} as the last field. Single-shard plans do not trigger
 *       QTF (no gather to skip).</li>
 *   <li>Wrapper output rowType = {@code AboveAnchorPhysicalFields}, in
 *       {@code TopmostOperatorAboveAnchor} order. Helpers stripped.</li>
 *   <li>Outer Project's RexInputRefs are remapped to wrapper-output indices.</li>
 * </ul>
 *
 * <p>Skip predicate: QTF fires iff
 * {@code AboveAnchorPhysicalFields - BelowAnchorPhysicalFields} is non-empty.
 */
public class LateMaterializationPlanShapeTests extends BasePlannerRulesTests {

    // ── Tests that fire QTF ────────────────────────────────────────────

    public void testQtfFires_simpleSortProject() {
        // SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10
        // AboveAnchorPhysicalFields = [URL, EventDate] (topmost Project, in SELECT order)
        // BelowAnchorPhysicalFields = {EventDate}
        // FetchOnly = {URL} → fire
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "EventDate"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL", "EventDate"),
            Expect.outerProjectExprIndices(0, 1)
        );
    }

    public void testQtfFires_withWhere() {
        // SELECT URL, EventDate FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10
        // AboveAnchorPhysicalFields = [URL, EventDate]
        // BelowAnchorPhysicalFields = {CounterID, EventDate}
        // FetchOnly = {URL} → fire
        assertQtfFired(
            "SELECT URL, EventDate FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("CounterID", "EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "EventDate"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL", "EventDate"),
            Expect.outerProjectExprIndices(0, 1)
        );
    }

    public void testQtfFires_sortColAlsoProjected() {
        // SELECT EventDate, URL FROM hits ORDER BY EventDate LIMIT 10
        // Wrapper output is in topmost-op (SELECT) order, NOT scan order.
        // EventDate is in BOTH BelowAnchor and AboveAnchor — refetched per v2 (no passthrough trick).
        assertQtfFired(
            "SELECT EventDate, URL FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("EventDate", "URL"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("EventDate", "URL"),
            Expect.outerProjectExprIndices(0, 1)
        );
    }

    public void testQtfDeclined_singleShard() {
        // Single shard: CBO inserts no ExchangeReducer below the anchor (the scan's
        // SOURCE(SINGLETON) already satisfies the parent Sort's demand). QTF's win comes from
        // avoiding cross-node materialization of fetch-only columns through the gather; with
        // no gather there's nothing to save, so the rewriter declines.
        assertQtfDeclined("SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10", 1);
    }

    public void testQtfFires_descendingSort() {
        // DESC collation preserved through anchor rebuild.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate DESC LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "EventDate"),
            Expect.erHasUgsi(true),
            Expect.collationDirections(RelFieldCollation.Direction.DESCENDING)
        );
    }

    public void testQtfFires_multiKeySort() {
        // SELECT URL, EventDate, CounterID FROM hits ORDER BY EventDate, CounterID LIMIT 10
        // BelowAnchor = {EventDate, CounterID}, AboveAnchor = [URL, EventDate, CounterID]
        // FetchOnly = {URL} → fire. All three referenced fields end up in fetch list (v2 refetches).
        assertQtfFired(
            "SELECT URL, EventDate, CounterID FROM hits ORDER BY EventDate, CounterID LIMIT 10",
            2,
            Expect.scanCols("CounterID", "EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "EventDate", "CounterID"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL", "EventDate", "CounterID")
        );
    }

    public void testQtfFires_filterColAlsoProjected() {
        // SELECT URL, CounterID FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10
        // BelowAnchor = {CounterID, EventDate}, AboveAnchor = [URL, CounterID]
        // FetchOnly = {URL} → fire. CounterID refetched.
        assertQtfFired(
            "SELECT URL, CounterID FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("CounterID", "EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "CounterID"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL", "CounterID")
        );
    }

    public void testQtfFires_offset() {
        // OFFSET preserved on the rebuilt anchor Sort.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10 OFFSET 5",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "EventDate"),
            Expect.erHasUgsi(true)
        );
    }

    public void testQtfFires_compositeExpressionWithDedup() {
        // SELECT URL || '-' || URL AS combined, UPPER(URL) AS upper_url FROM hits ORDER BY EventDate LIMIT 10
        // Both above-Project outputs derive from URL. dependsOn walk + LinkedHashSet dedup
        // collapses to AboveAnchorPhysicalFields = [URL].
        // BelowAnchor = {EventDate}; FetchOnly = {URL} → fire.
        // Outer Project carries RexCall expressions, not bare InputRefs — outerProjectExprIndices skipped.
        assertQtfFired(
            "SELECT URL || '-' || URL AS combined, UPPER(URL) AS upper_url FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("URL"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL")
        );
    }

    public void testQtfFires_compositeExpressionMultiCol() {
        // SELECT URL || '-' || Title AS combined FROM hits ORDER BY EventDate LIMIT 10
        // dependsOn walk yields {URL, Title} in evaluation order.
        // BelowAnchor = {EventDate}; FetchOnly = {URL, Title} → fire.
        assertQtfFired(
            "SELECT URL || '-' || Title AS combined FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "Title"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL", "Title")
        );
    }

    public void testQtfFires_starProjection() {
        // SELECT * FROM hits ORDER BY EventDate LIMIT 10
        // Topmost Project's outputs are passthrough refs to every physical field.
        // AboveAnchor = [CounterID, UserID, URL, Title, EventDate, AdvEngineID, ParamPrice].
        // BelowAnchor = {EventDate}. FetchOnly = everything except EventDate → fire.
        assertQtfFired(
            "SELECT * FROM hits ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("CounterID", "UserID", "URL", "Title", "EventDate", "AdvEngineID", "ParamPrice"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("CounterID", "UserID", "URL", "Title", "EventDate", "AdvEngineID", "ParamPrice")
        );
    }

    public void testQtfFires_outerFilter() {
        // SELECT URL, EventDate FROM hits WHERE EventDate > '2020-01-01' ORDER BY EventDate LIMIT 10
        // Filter on EventDate is below-Filter (predicate pushdown), so EventDate stays in BelowAnchor.
        // AboveAnchor = [URL, EventDate]; FetchOnly = {URL} → fire.
        assertQtfFired(
            "SELECT URL, EventDate FROM hits WHERE EventDate > DATE '2020-01-01' ORDER BY EventDate LIMIT 10",
            2,
            Expect.scanCols("EventDate"),
            Expect.aboveAnchorPhysicalFields("URL", "EventDate"),
            Expect.erHasUgsi(true),
            Expect.wrapperOutput("URL", "EventDate")
        );
    }

    // ── Tests that decline QTF ─────────────────────────────────────────

    public void testQtfDeclined_pureLimit() {
        // SELECT URL FROM hits LIMIT 10
        // No ORDER BY → no anchor → no rewrite.
        assertQtfDeclined("SELECT URL FROM hits LIMIT 10", 2);
    }

    public void testQtfDeclined_skipPredicate_sortColIsOnlyProjection() {
        // SELECT EventDate FROM hits ORDER BY EventDate LIMIT 10
        // AboveAnchor = {EventDate}; BelowAnchor = {EventDate}; FetchOnly = {} → skip.
        // Refetching EventDate by row id when it already rode through the reduce would be a wash.
        assertQtfDeclined("SELECT EventDate FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_skipPredicate_filterColIsOnlyProjection() {
        // SELECT CounterID FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10
        // AboveAnchor = {CounterID}; BelowAnchor = {CounterID, EventDate}; FetchOnly = {} → skip.
        assertQtfDeclined("SELECT CounterID FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_aggregateBelowSort() {
        // SELECT CounterID, COUNT(*) FROM hits GROUP BY CounterID ORDER BY CounterID LIMIT 10
        // Aggregate sits below the anchor — not in BELOW_INTERMEDIATE_ALLOWED, declined.
        assertQtfDeclined("SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID ORDER BY CounterID LIMIT 10", 2);
    }

    public void testQtfDeclined_aggregateAboveAnchor() {
        // Aggregate above the anchor — explicitly excluded from ABOVE_ALLOWED in v2.
        // (Above-Aggregate group/aggCall remap under a moving wrapper rowType is a follow-up.)
        // SELECT inner.CounterID, COUNT(*) FROM (SELECT CounterID FROM hits ORDER BY CounterID LIMIT 100) AS inner GROUP BY inner.CounterID
        assertQtfDeclined(
            "SELECT inner_q.CounterID, COUNT(*) AS c "
                + "FROM (SELECT CounterID FROM hits ORDER BY CounterID LIMIT 100) AS inner_q "
                + "GROUP BY inner_q.CounterID",
            2
        );
    }

    public void testQtfDeclined_windowInOuterProject() {
        // SELECT URL, SUM(ParamPrice) OVER () FROM hits ORDER BY EventDate LIMIT 10
        // RexOver in the above-Project — declined (window's global frame needs SINGLETON input;
        // the wrapper would break that).
        assertQtfDeclined("SELECT URL, SUM(ParamPrice) OVER () AS sp FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_expressionProjectBelowAnchor() {
        // SELECT URL FROM hits ORDER BY (CounterID + 1) LIMIT 10
        // The sort key (CounterID + 1) gets materialized via a derived below-Project.
        // Today's algorithm declines (TODO in passthroughMap — derived-pushup not yet supported).
        assertQtfDeclined("SELECT URL FROM hits ORDER BY (CounterID + 1) LIMIT 10", 2);
    }

    // ── Composable assert API ──────────────────────────────────────────

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

    private abstract static class Expect {
        abstract void check(Inspector ctx, String sql, String planText);

        /** Scan rowType (post-narrowing) carries exactly these original cols + {@code ___row_id}. */
        static Expect scanCols(String... expectedNamesInOrder) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<String> actual = fieldNames(ctx.scan.getRowType().getFieldList());
                    List<String> expected = new ArrayList<>(Arrays.asList(expectedNamesInOrder));
                    expected.add(OpenSearchLateMaterialization.ROW_ID_FIELD);
                    if (!expected.equals(actual)) {
                        fail(
                            "Scan rowType mismatch.\n  expected: "
                                + expected
                                + "\n  actual:   "
                                + actual
                                + "\nSQL: "
                                + sql
                                + "\nPlan:\n"
                                + plan
                        );
                    }
                }
            };
        }

        /**
         * Wrapper carries exactly these field names as its {@code AboveAnchorPhysicalFields}
         * (i.e. fetch list, in order).
         */
        static Expect aboveAnchorPhysicalFields(String... expectedNamesInOrder) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<String> actual = fieldNames(ctx.wrapper.getAboveAnchorPhysicalFields());
                    List<String> expected = Arrays.asList(expectedNamesInOrder);
                    if (!expected.equals(actual)) {
                        fail(
                            "Wrapper aboveAnchorPhysicalFields mismatch.\n  expected: "
                                + expected
                                + "\n  actual:   "
                                + actual
                                + "\nSQL: "
                                + sql
                                + "\nPlan:\n"
                                + plan
                        );
                    }
                }
            };
        }

        /**
         * Wrapper output rowType is exactly these names (= {@code AboveAnchorPhysicalFields} in
         * topmost-op order). Helpers must be absent.
         */
        static Expect wrapperOutput(String... expectedNamesInOrder) {
            return new Expect() {
                @Override
                void check(Inspector ctx, String sql, String plan) {
                    List<String> actual = fieldNames(ctx.wrapper.getRowType().getFieldList());
                    List<String> expected = Arrays.asList(expectedNamesInOrder);
                    if (!expected.equals(actual)) {
                        fail(
                            "Wrapper output rowType mismatch.\n  expected: "
                                + expected
                                + "\n  actual:   "
                                + actual
                                + "\nSQL: "
                                + sql
                                + "\nPlan:\n"
                                + plan
                        );
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

        /**
         * Outer Project (immediately above wrapper) RexInputRef indices, in expression order.
         * Skip this assertion for projects whose expressions are not bare InputRefs (e.g.
         * derived expressions like {@code UPPER(URL)}).
         */
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
                            fail(
                                "Outer Project expr ["
                                    + i
                                    + "] is not a RexInputRef: "
                                    + ctx.outerProject.getProjects().get(i)
                                    + "\nSQL: "
                                    + sql
                                    + "\nPlan:\n"
                                    + plan
                            );
                            return;
                        }
                    }
                    if (!Arrays.equals(expectedIndices, actual)) {
                        fail(
                            "Outer Project RexInputRef indices mismatch.\n  expected: "
                                + Arrays.toString(expectedIndices)
                                + "\n  actual:   "
                                + Arrays.toString(actual)
                                + "\nSQL: "
                                + sql
                                + "\nPlan:\n"
                                + plan
                        );
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
                        fail(
                            "Anchor collation size mismatch. expected="
                                + expected.length
                                + " actual="
                                + fc.size()
                                + "\nSQL: "
                                + sql
                                + "\nPlan:\n"
                                + plan
                        );
                    }
                    for (int i = 0; i < expected.length; i++) {
                        if (fc.get(i).getDirection() != expected[i]) {
                            fail(
                                "Anchor collation["
                                    + i
                                    + "] direction mismatch. expected="
                                    + expected[i]
                                    + " actual="
                                    + fc.get(i).getDirection()
                                    + "\nSQL: "
                                    + sql
                                    + "\nPlan:\n"
                                    + plan
                            );
                        }
                    }
                }
            };
        }
    }

    /** Walks an optimized plan once, capturing the QTF-relevant nodes. */
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
        for (RelDataTypeField f : fields)
            out.add(f.getName());
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
