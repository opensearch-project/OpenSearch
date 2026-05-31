/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Sarg;
import org.opensearch.analytics.planner.SqlPlannerTestFixture;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.cluster.ClusterState;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link CanMatchFilterExtractor}. Mix of SQL-driven fixtures (real
 * Calcite output for ordinary comparisons) and direct RexNode construction (deterministic
 * Sarg cases without depending on which Calcite rewrite happens to emit them).
 *
 * <p>Empirical baseline (Calcite without rule-driven rewrites): {@code col > N} stays
 * as a direct {@code >} call rather than collapsing to {@code SEARCH(col, Sarg[(N..)])}
 * — so the dual-bound extraction path is the hot one, and the SEARCH/Sarg path is
 * exercised only when the planner upstream of us has run a rule that folds into a Sarg.
 */
public class CanMatchFilterExtractorTests extends OpenSearchTestCase {

    // ── SQL-driven happy paths ───────────────────────────────────────────

    public void testGreaterThanBumpsOpenLowerBound() {
        // Strict `> 5` for an integer column → smallest matching value is 6.
        // The bump enables pruning row groups with stat range [5, 5].
        assertSingleRange(extractFromSql("SELECT * FROM idx WHERE status > 5"), "status", 6L, Long.MAX_VALUE);
    }

    public void testGreaterThanOrEqual() {
        assertSingleRange(extractFromSql("SELECT * FROM idx WHERE status >= 5"), "status", 5L, Long.MAX_VALUE);
    }

    public void testLessThanBumpsOpenUpperBound() {
        // Strict `< 100` for an integer column → largest matching value is 99.
        assertSingleRange(extractFromSql("SELECT * FROM idx WHERE status < 100"), "status", Long.MIN_VALUE, 99L);
    }

    public void testLessThanOrEqual() {
        assertSingleRange(extractFromSql("SELECT * FROM idx WHERE status <= 100"), "status", Long.MIN_VALUE, 100L);
    }

    public void testReversedSidesGreaterThan() {
        // 5 < status → equivalent to status > 5 after sides swap. Strict → bumped.
        assertSingleRange(extractFromSql("SELECT * FROM idx WHERE 5 < status"), "status", 6L, Long.MAX_VALUE);
    }

    public void testReversedSidesLessThan() {
        // 100 > status → equivalent to status < 100. Strict → bumped.
        assertSingleRange(extractFromSql("SELECT * FROM idx WHERE 100 > status"), "status", Long.MIN_VALUE, 99L);
    }

    public void testCombinedAndRangeProducesTwoFilters() {
        // AND(>($0,5), <($0,100)) — both strict, both bumped.
        List<CanMatchFilter> filters = extractFromSql("SELECT * FROM idx WHERE status > 5 AND status < 100");
        assertEquals(2, filters.size());
        CanMatchFilter lo = byColumnAndMin(filters, "status", 6L);
        CanMatchFilter hi = byColumnAndMax(filters, "status", 99L);
        assertEquals(Long.MAX_VALUE, lo.getMaxValue());
        assertEquals(Long.MIN_VALUE, hi.getMinValue());
    }

    public void testBetweenProducesTwoFilters() {
        // BETWEEN folds to AND(>=, <=) at parse time. Two filters, same as the >/< case.
        List<CanMatchFilter> filters = extractFromSql("SELECT * FROM idx WHERE status BETWEEN 10 AND 20");
        assertEquals(2, filters.size());
        assertEquals(10L, byColumnAndMin(filters, "status", 10L).getMinValue());
        assertEquals(20L, byColumnAndMax(filters, "status", 20L).getMaxValue());
    }

    public void testMultipleColumnsProducesOneFilterEach() {
        List<CanMatchFilter> filters = extractFromSql("SELECT * FROM idx WHERE status > 5 AND size < 100");
        assertEquals(2, filters.size());
        CanMatchFilter statusFilter = filters.stream().filter(f -> f.getColumnName().equals("status")).findFirst().orElseThrow();
        CanMatchFilter sizeFilter = filters.stream().filter(f -> f.getColumnName().equals("size")).findFirst().orElseThrow();
        assertEquals(6L, statusFilter.getMinValue()); // strict, bumped
        assertEquals(Long.MAX_VALUE, statusFilter.getMaxValue());
        assertEquals(Long.MIN_VALUE, sizeFilter.getMinValue());
        assertEquals(99L, sizeFilter.getMaxValue()); // strict, bumped
    }

    // ── SQL-driven negative cases ────────────────────────────────────────

    public void testDisjunctionProducesNoFilter() {
        // OR is not a recognised extraction path. Pinning the conservative behaviour.
        List<CanMatchFilter> filters = extractFromSql("SELECT * FROM idx WHERE status > 5 OR status < -5");
        assertTrue("OR must not produce a filter: " + filters, filters.isEmpty());
    }

    public void testNotEqualProducesNoFilter() {
        // Calcite uses <> for inequality.
        List<CanMatchFilter> filters = extractFromSql("SELECT * FROM idx WHERE status <> 5");
        assertTrue(filters.isEmpty());
    }

    public void testIsNullProducesNoFilter() {
        assertTrue(extractFromSql("SELECT * FROM idx WHERE status IS NULL").isEmpty());
    }

    public void testInListProducesNoFilter() {
        // IN (1, 2, 3) — extractor doesn't model set membership, conservatively skipped.
        assertTrue(extractFromSql("SELECT * FROM idx WHERE status IN (1, 2, 3)").isEmpty());
    }

    public void testEqualityProducesNoFilter() {
        // The current extractor only handles range kinds; equality isn't an extraction path.
        assertTrue(extractFromSql("SELECT * FROM idx WHERE status = 5").isEmpty());
    }

    public void testStringColumnComparisonProducesNoFilter() {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", Map.of("name", Map.of("type", "keyword")));
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT * FROM idx WHERE name > 'foo'", state);
        assertTrue(CanMatchFilterExtractor.extract(parsed).isEmpty());
    }

    // ── Decimal / double literal handling (C1 — bug-reveal) ─────────────

    public void testDoubleColumnWithFractionalLiteralIsRejected() {
        // C1: `price > 9.99` must not produce a filter — extracting truncates 9.99→9 and
        // would incorrectly eliminate row groups where price ∈ [9, 9.99].
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", Map.of("price", Map.of("type", "double")));
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT * FROM idx WHERE price > 9.99", state);
        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(parsed);
        assertTrue("fractional literal must not produce a filter (truncation hazard): " + filters, filters.isEmpty());
    }

    // ── Direct Sarg construction (C2 — bug-reveal + nullAs coverage) ────

    public void testSargWithUnknownNullAsExtracts() {
        RelNode filter = buildFilterWithSarg(RexUnknownAs.UNKNOWN, Range.greaterThan(BigDecimal.valueOf(5)));
        assertSingleRange(CanMatchFilterExtractor.extract(filter), "status", 6L, Long.MAX_VALUE);
    }

    public void testSargWithFalseNullAsExtracts() {
        // C2: `nullAs = FALSE` is semantically equivalent to UNKNOWN for WHERE purposes —
        // null rows are excluded either way. Extractor must still produce a filter.
        RelNode filter = buildFilterWithSarg(RexUnknownAs.FALSE, Range.greaterThan(BigDecimal.valueOf(5)));
        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(filter);
        assertEquals("nullAs=FALSE must still extract: " + filters, 1, filters.size());
        assertEquals(6L, filters.get(0).getMinValue());
        assertEquals(Long.MAX_VALUE, filters.get(0).getMaxValue());
    }

    public void testSargWithTrueNullAsIsSkipped() {
        // nullAs=TRUE makes null match the predicate. Without a null-count check, an
        // all-nulls row group would be incorrectly dropped. Stay conservative — skip.
        RelNode filter = buildFilterWithSarg(RexUnknownAs.TRUE, Range.greaterThan(BigDecimal.valueOf(5)));
        assertTrue(CanMatchFilterExtractor.extract(filter).isEmpty());
    }

    public void testSargWithFractionalUpperBoundDoesNotOverPrune() {
        // C1 (Sarg path): a Sarg of (-∞, 9.99) on an integer column means "values strictly
        // less than 9.99" — integers 9 and below match. Truncating 9.99→9 then bumping
        // for OPEN gives [MIN, 8], which incorrectly drops row groups with stat range
        // [9, 9]. Either reject the fractional endpoint or only bump for integer-typed
        // bounds. Either way, the filter must not over-prune integer 9.
        RelNode filter = buildFilterWithSarg(RexUnknownAs.UNKNOWN, Range.lessThan(new BigDecimal("9.99")));
        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(filter);
        if (filters.isEmpty()) return; // acceptable: fail-open, no pruning
        assertEquals(1, filters.size());
        long hi = filters.get(0).getMaxValue();
        assertTrue("upper bound " + hi + " must not exclude integer 9 (which matches < 9.99)", hi >= 9);
    }

    public void testMultiRangeSargIsSkipped() {
        // Two disjoint ranges → skipped (single-range only).
        ImmutableRangeSet<BigDecimal> rangeSet = ImmutableRangeSet.<BigDecimal>builder()
            .add(Range.lessThan(BigDecimal.valueOf(0)))
            .add(Range.greaterThan(BigDecimal.valueOf(10)))
            .build();
        RelNode filter = buildFilterWithSarg(RexUnknownAs.UNKNOWN, rangeSet);
        assertTrue(CanMatchFilterExtractor.extract(filter).isEmpty());
    }

    // ── AnnotatedPredicate unwrap ───────────────────────────────────────

    public void testAnnotatedPredicateUnwrap() {
        // Wrap a `status > 5` predicate in AnnotatedPredicate; extractor must unwrap and bump.
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT * FROM idx WHERE status > 5", state);
        Filter filter = findFilter(parsed);
        RexNode original = filter.getCondition();
        AnnotatedPredicate annotated = new AnnotatedPredicate(original.getType(), original, List.of("datafusion"), 1);
        Filter wrapped = (Filter) filter.copy(filter.getTraitSet(), filter.getInput(), annotated);
        assertSingleRange(CanMatchFilterExtractor.extract(wrapped), "status", 6L, Long.MAX_VALUE);
    }

    // ── Misc ─────────────────────────────────────────────────────────────

    public void testFilterAboveRenamingProjectionResolvesToOriginalColumn() {
        // C3: inner SELECT renames status → sev; outer WHERE references sev. Calcite emits:
        //   LogicalFilter(>(sev, 5))
        //     LogicalProject(sev = $0)
        //       LogicalTableScan(idx)
        // The extractor must resolve sev back through the renaming projection to
        // "status" — otherwise the data node looks up the wrong column and pruning
        // silently fails (no match in parquet stats → Unknown → fail-open).
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql(
            "SELECT * FROM (SELECT status AS sev FROM idx) WHERE sev > 5",
            state
        );
        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(parsed);
        assertEquals(1, filters.size());
        assertEquals("alias must resolve back through renaming projection", "status", filters.get(0).getColumnName());
        assertEquals(6L, filters.get(0).getMinValue());
    }

    public void testFilterAboveComputedProjectionIsSkipped() {
        // Computed projection (sev = status + 1) can't be reverse-mapped to a single
        // source column — extractor must fail open (no filter).
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql(
            "SELECT * FROM (SELECT status + 1 AS sev FROM idx) WHERE sev > 5",
            state
        );
        List<CanMatchFilter> filters = CanMatchFilterExtractor.extract(parsed);
        assertTrue("computed projection must not produce a filter: " + filters, filters.isEmpty());
    }

    public void testNonFilterPlanReturnsEmpty() {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT status FROM idx", state);
        assertTrue(CanMatchFilterExtractor.extract(parsed).isEmpty());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static Map<String, Map<String, Object>> intFields() {
        return Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer"));
    }

    private static List<CanMatchFilter> extractFromSql(String sql) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);
        return CanMatchFilterExtractor.extract(parsed);
    }

    private static void assertSingleRange(List<CanMatchFilter> filters, String column, long lo, long hi) {
        assertEquals("expected exactly one filter, got: " + filters, 1, filters.size());
        assertEquals(column, filters.get(0).getColumnName());
        assertEquals("min for " + column, lo, filters.get(0).getMinValue());
        assertEquals("max for " + column, hi, filters.get(0).getMaxValue());
    }

    private static CanMatchFilter byColumnAndMin(List<CanMatchFilter> filters, String column, long min) {
        return filters.stream().filter(f -> f.getColumnName().equals(column) && f.getMinValue() == min).findFirst().orElseThrow();
    }

    private static CanMatchFilter byColumnAndMax(List<CanMatchFilter> filters, String column, long max) {
        return filters.stream().filter(f -> f.getColumnName().equals(column) && f.getMaxValue() == max).findFirst().orElseThrow();
    }

    private static Filter findFilter(RelNode plan) {
        if (plan instanceof Filter f) return f;
        for (RelNode input : plan.getInputs()) {
            Filter found = findFilter(input);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Builds a LogicalFilter with a SEARCH(status, Sarg[range; nullAs]) condition by
     * starting from a real Calcite-produced filter (so the cluster, types, and input
     * row type are valid) and substituting the condition. Uses the input's actual column
     * type (matching nullability) to satisfy {@code Filter.isValid}.
     */
    private static RelNode buildFilterWithSarg(RexUnknownAs nullAs, Range<BigDecimal> range) {
        return buildFilterWithSarg(nullAs, ImmutableRangeSet.of(range));
    }

    private static RelNode buildFilterWithSarg(RexUnknownAs nullAs, ImmutableRangeSet<BigDecimal> ranges) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith("idx", intFields());
        RelNode parsed = SqlPlannerTestFixture.parseSql("SELECT * FROM idx WHERE status > 0", state);
        Filter base = findFilter(parsed);
        RelOptCluster cluster = base.getCluster();
        RexBuilder rb = cluster.getRexBuilder();
        int statusIdx = base.getInput().getRowType().getFieldNames().indexOf("status");
        RelDataType colType = base.getInput().getRowType().getFieldList().get(statusIdx).getType();
        RexInputRef inputRef = rb.makeInputRef(colType, statusIdx);
        Sarg<BigDecimal> sarg = Sarg.of(nullAs, ranges);
        RexNode sargLit = rb.makeSearchArgumentLiteral(sarg, colType);
        RexNode search = rb.makeCall(SqlStdOperatorTable.SEARCH, inputRef, sargLit);
        return LogicalFilter.create(base.getInput(), search);
    }
}
