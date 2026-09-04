/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.UnsupportedFunctionException;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link OpenSearchNestedFieldRewriter}: how {@code ITEM($arrayCol,'field')} filter
 * predicates become {@code NESTED_ANY_MATCH($arrayCol,'<json>')} calls, and which shapes it
 * deliberately leaves alone for the marking phase to reject.
 *
 * <p>Input schema (column index → type):
 * <ul>
 *   <li>{@code $0 events}  — {@code ARRAY<ROW<name:VARCHAR, count:INTEGER>>}</li>
 *   <li>{@code $1 links}   — {@code ARRAY<ROW<traceId:VARCHAR>>}</li>
 *   <li>{@code $2 traceId} — {@code VARCHAR} (a plain row-level scalar)</li>
 * </ul>
 */
public class OpenSearchNestedFieldRewriterTests extends BasePlannerRulesTests {

    private static final int EVENTS = 0;
    private static final int LINKS = 1;
    private static final int TRACE_ID = 2;

    // ---- rewrite shapes ----

    public void testSimpleLeafEqualityRewrittenToNestedAnyMatch() {
        RelNode scan = nestedScan();
        RexNode condition = eq(eventsName(), str("exception"));

        RexCall call = asNestedAnyMatch(rewrittenCondition(scan, condition));
        assertEquals(EVENTS, arrayColOf(call));
        assertEquals("{\"op\":\"=\",\"args\":[{\"field\":\"name\"},{\"lit\":\"exception\"}]}", jsonOf(call));
    }

    public void testNumericComparisonEmitsFieldAndOp() {
        RelNode scan = nestedScan();
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, eventsCount(), intLit(0));

        String json = jsonOf(asNestedAnyMatch(rewrittenCondition(scan, condition)));
        assertTrue(json, json.contains("\"op\":\">\""));
        assertTrue(json, json.contains("\"field\":\"count\""));
    }

    public void testSameFamilyCastOnNestedLeafIsUnwrapped() {
        // `cast(events.count as bigint) > 0` — int->bigint stays in the NUMERIC family, so the implicit
        // cast is unwrapped and the leaf predicate still rewrites.
        RelNode scan = nestedScan();
        RexNode castToBigint = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.BIGINT), eventsCount());
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, castToBigint, intLit(0));

        String json = jsonOf(asNestedAnyMatch(rewrittenCondition(scan, condition)));
        assertTrue(json, json.contains("\"field\":\"count\""));
    }

    public void testBooleanLeafEqualityRewritten() {
        // `where events.isError = true` — the comparison form builds a proper op-node and rewrites.
        RelNode scan = nestedScan();
        RexNode condition = eq(item(EVENTS, eventsArrayType(), "isError", SqlTypeName.BOOLEAN), rexBuilder.makeLiteral(true));

        String json = jsonOf(asNestedAnyMatch(rewrittenCondition(scan, condition)));
        assertTrue(json, json.contains("\"op\":\"=\""));
        assertTrue(json, json.contains("\"field\":\"isError\""));
    }

    public void testSameArrayConjunctsFusedIntoOneCall() {
        RelNode scan = nestedScan();
        RexNode condition = makeAnd(
            eq(eventsName(), str("exception")),
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, eventsCount(), intLit(0))
        );

        // Fusion: one element must satisfy both conjuncts, so a single call carries an AND tree.
        RexCall call = asNestedAnyMatch(rewrittenCondition(scan, condition));
        String json = jsonOf(call);
        assertTrue(json, json.contains("\"op\":\"AND\""));
        assertTrue(json, json.contains("\"field\":\"name\""));
        assertTrue(json, json.contains("\"field\":\"count\""));
    }

    public void testNestedAndParentConjunctSplit() {
        RelNode scan = nestedScan();
        RexNode condition = makeAnd(eq(eventsName(), str("exception")), eq(traceIdRef(), str("t1")));

        RexCall top = asCall(rewrittenCondition(scan, condition), SqlKind.AND);
        assertEquals(2, top.getOperands().size());
        // Nested conjunct becomes a NESTED_ANY_MATCH call; parent scalar stays a plain equal.
        assertSame(OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_OP, ((RexCall) top.getOperands().get(0)).getOperator());
        assertEquals(SqlKind.EQUALS, top.getOperands().get(1).getKind());
    }

    public void testMultipleArraysEmitOneCallEach() {
        RelNode scan = nestedScan();
        RexNode condition = makeAnd(eq(eventsName(), str("exception")), eq(linksTraceId(), str("t1")));

        // Independent existentials: one call per array, ANDed at row level.
        RexCall top = asCall(rewrittenCondition(scan, condition), SqlKind.AND);
        assertEquals(2, top.getOperands().size());
        assertEquals(EVENTS, arrayColOf(asNestedAnyMatch(top.getOperands().get(0))));
        assertEquals(LINKS, arrayColOf(asNestedAnyMatch(top.getOperands().get(1))));
    }

    public void testOrSplitBetweenArrayAndParent() {
        RelNode scan = nestedScan();
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, eq(eventsName(), str("exception")), eq(traceIdRef(), str("t1")));

        RexCall top = asCall(rewrittenCondition(scan, condition), SqlKind.OR);
        assertEquals(2, top.getOperands().size());
        assertSame(OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_OP, ((RexCall) top.getOperands().get(0)).getOperator());
        assertEquals(SqlKind.EQUALS, top.getOperands().get(1).getKind());
    }

    // ---- unsupported shapes (no ITEM-on-array → unchanged; ITEM-on-array we can't rewrite → 400) ----

    public void testFilterWithoutItemOnArrayUnchanged() {
        RelNode scan = nestedScan();
        RexNode condition = eq(traceIdRef(), str("t1"));

        assertUnchanged(scan, condition);
    }

    public void testArithmeticOnNestedLeafNotRewritten() {
        // `events.count + 1 > 5` — arithmetic isn't in the JSON grammar the Rust rule consumes,
        // so the rewriter rejects it with a 400 rather than emit a call that would error at execution (Finding A).
        RelNode scan = nestedScan();
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, eventsCount(), intLit(1));
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, plus, intLit(5));

        assertRejected(scan, condition);
    }

    public void testNullLiteralNotRewritten() {
        // `events.name = NULL` — a NULL literal is not a valid element predicate; rejected with a 400 (Finding B).
        RelNode scan = nestedScan();
        RexNode condition = eq(eventsName(), rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        assertRejected(scan, condition);
    }

    public void testCrossArrayComparisonNotRewritten() {
        // `events.name = links.traceId` — a correlation across two arrays, unsupported; rejected with a 400.
        RelNode scan = nestedScan();
        RexNode condition = eq(eventsName(), linksTraceId());

        assertRejected(scan, condition);
    }

    public void testCrossArrayOrNotRewritten() {
        // `events.name='a' OR links.traceId='t'` — OR across two arrays (independent existentials the
        // OR-split doesn't model); rejected with a 400 rather than emitting a raw ITEM-on-array.
        RelNode scan = nestedScan();
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, eq(eventsName(), str("a")), eq(linksTraceId(), str("t")));

        assertRejected(scan, condition);
    }

    public void testArrayWithinArrayNotRewritten() {
        // `events.spans.name = 'x'` — array-within-array; the Rust consumer can't lower multi-level
        // descent, so the rewriter rejects it with a 400 (see class doc "Not covered").
        RelDataType innerElem = typeFactory.createStructType(List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("name"));
        RelDataType spansArray = typeFactory.createArrayType(innerElem, -1);
        RelDataType outerElem = typeFactory.createStructType(List.of(spansArray), List.of("spans"));
        RelDataType eventsArray = typeFactory.createArrayType(outerElem, -1);
        RelDataType rowType = typeFactory.builder().add("events", eventsArray).build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("nested_index"));
        when(table.getRowType()).thenReturn(rowType);
        RelNode scan = stubScan(table);

        RexNode arrayRef = rexBuilder.makeInputRef(eventsArray, 0);
        RexNode innerItem = rexBuilder.makeCall(spansArray, SqlStdOperatorTable.ITEM, List.of(arrayRef, rexBuilder.makeLiteral("spans")));
        RexNode outerItem = rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            SqlStdOperatorTable.ITEM,
            List.of(innerItem, rexBuilder.makeLiteral("name"))
        );
        RexNode condition = eq(outerItem, str("x"));

        assertRejected(scan, condition);
    }

    // ---- IN / BETWEEN / LIKE: coverage depends on how Calcite normalized the predicate ----

    public void testInExpandedToOrIsRewritten() {
        // `events.name IN ('a','b')` lowered to OR-of-equals → covered.
        RelNode scan = nestedScan();
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, eq(eventsName(), str("a")), eq(eventsName(), str("b")));

        String json = jsonOf(asNestedAnyMatch(rewrittenCondition(scan, condition)));
        assertTrue(json, json.contains("\"op\":\"OR\""));
    }

    public void testBetweenExpandedToRangeIsRewritten() {
        // `events.count BETWEEN 1 AND 10` lowered to `>= AND <=` → covered (fused into one call).
        RelNode scan = nestedScan();
        RexNode ge = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, eventsCount(), intLit(1));
        RexNode le = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, eventsCount(), intLit(10));

        String json = jsonOf(asNestedAnyMatch(rewrittenCondition(scan, makeAnd(ge, le))));
        assertTrue(json, json.contains("\"op\":\">=\""));
        assertTrue(json, json.contains("\"op\":\"<=\""));
    }

    public void testSearchSargOnLeafIsNotRewritten() {
        // The other lowering: an IN / range kept as a SEARCH(Sarg) node is outside the allowlist → rejected with a 400.
        RelNode scan = nestedScan();
        Sarg<BigDecimal> sarg = Sarg.of(
            RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.<BigDecimal>builder()
                .add(Range.singleton(BigDecimal.valueOf(1)))
                .add(Range.singleton(BigDecimal.valueOf(2)))
                .build()
        );
        RexNode ref = eventsCount();
        RexNode search = rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, ref, rexBuilder.makeSearchArgumentLiteral(sarg, ref.getType()));
        assertEquals(SqlKind.SEARCH, search.getKind());

        assertRejected(scan, search);
    }

    public void testLikeOnLeafIsNotRewritten() {
        // LIKE is never in the allowlist regardless of normalization → rejected with a 400.
        RelNode scan = nestedScan();
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, eventsName(), str("exc%"));

        assertRejected(scan, condition);
    }

    public void testBareBooleanNestedLeafNotRewritten() {
        // `where events.isError` — a bare boolean nested leaf, no comparison. It builds to a rootless
        // {"field":...} tree with no "op", which the Rust consumer can't lower (the placeholder would
        // survive to execution → 500). The rewriter rejects it with a clean 400 instead;
        // `where events.isError = true` (above) still works.
        RelNode scan = nestedScan();
        RexNode condition = item(EVENTS, eventsArrayType(), "isError", SqlTypeName.BOOLEAN);

        assertRejected(scan, condition);
    }

    public void testCrossFamilyCastOnNestedLeafNotRewritten() {
        // `cast(events.name as int) = 5` — the cast crosses VARCHAR→INTEGER, which would change the
        // comparison. We can't represent it in the element tree, so reject with a 400 rather than
        // silently dropping the cast and comparing the string field to an int.
        RelNode scan = nestedScan();
        RexNode castToInt = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), eventsName());
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, castToInt, intLit(5));

        assertRejected(scan, condition);
    }

    public void testRejectionNamesTheOffendingArrayNotTheFirst() {
        // `events.name='exception' AND links.traceId LIKE 't%'` — the unsupported predicate (LIKE) is on
        // `links`, though `events` is referenced first. The 400 must name the offending array, `links`.
        RelNode scan = nestedScan();
        RexNode condition = makeAnd(
            eq(eventsName(), str("exception")),
            rexBuilder.makeCall(SqlStdOperatorTable.LIKE, linksTraceId(), str("t%"))
        );

        UnsupportedFunctionException e = expectThrows(
            UnsupportedFunctionException.class,
            () -> OpenSearchNestedFieldRewriter.rewrite(makeFilter(scan, condition))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("links"));
        assertFalse(e.getMessage(), e.getMessage().contains("events"));
    }

    public void testCalciteLowersInToSearch() {
        // Anchors the boundary to reality: Calcite's own IN lowering yields a SEARCH(Sarg),
        // i.e. the not-covered branch above — not OR-of-equals.
        RexNode in = rexBuilder.makeIn(eventsName(), List.of(str("a"), str("b")));
        assertEquals(SqlKind.SEARCH, in.getKind());
    }

    // ---- operator sanity ----

    public void testNestedAnyMatchOperatorMetadata() {
        assertEquals("NESTED_ANY_MATCH", OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_OP.getName());
        assertEquals(SqlKind.OTHER_FUNCTION, OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_OP.getKind());
    }

    // ---- projection: fields events.name / numeric leaf / parent + leaf ----

    public void testLeafProjectionRewrittenToNestedProject() {
        RelNode scan = nestedScan();
        RexCall np = asNestedProject(rewrittenProject(scan, List.of(eventsName())).get(0));
        assertEquals(SqlTypeName.ARRAY, np.getType().getSqlTypeName());
        assertEquals(EVENTS, arrayColOf(np));
        assertEquals("{\"field\":\"name\"}", jsonOf(np));
    }

    public void testNumericLeafProjectionIsArrayOfInteger() {
        RelNode scan = nestedScan();
        RexCall np = asNestedProject(rewrittenProject(scan, List.of(eventsCount())).get(0));
        assertEquals(SqlTypeName.ARRAY, np.getType().getSqlTypeName());
        assertEquals(SqlTypeName.INTEGER, np.getType().getComponentType().getSqlTypeName());
    }

    public void testParentAndLeafProjection() {
        RelNode scan = nestedScan();
        List<RexNode> out = rewrittenProject(scan, List.of(traceIdRef(), eventsName()));
        assertEquals(SqlKind.INPUT_REF, out.get(0).getKind()); // parent scalar untouched
        assertSame(OpenSearchNestedFieldRewriter.NESTED_PROJECT_OP, ((RexCall) out.get(1)).getOperator());
    }

    public void testUnsupportedProjectionRejected() {
        // UPPER(events.name) — nested leaf wrapped in a function; unsupported in projection → 400.
        RelNode scan = nestedScan();
        RexNode wrapped = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, eventsName());
        LogicalProject p = LogicalProject.create(scan, List.of(), List.of(wrapped), (List<String>) null);
        expectThrows(UnsupportedFunctionException.class, () -> OpenSearchNestedFieldRewriter.rewrite(p));
    }

    public void testNonNestedProjectionUnchanged() {
        RelNode scan = nestedScan();
        LogicalProject p = LogicalProject.create(scan, List.of(), List.of(traceIdRef()), (List<String>) null);
        assertSame(p, OpenSearchNestedFieldRewriter.rewrite(p));
    }

    public void testMapKeyProjectionRewritten() {
        // events.attributes.<key> where `attributes` is a MAP → a map-value path.
        RelDataType mapType = typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR)
        );
        RelDataType elem = typeFactory.createStructType(
            List.of(mapType, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            List.of("attributes", "name")
        );
        RelDataType eventsArr = typeFactory.createArrayType(elem, -1);
        RelNode scan = arrayScan(eventsArr);

        RexNode arrayRef = rexBuilder.makeInputRef(eventsArr, 0);
        RexNode innerItem = rexBuilder.makeCall(mapType, SqlStdOperatorTable.ITEM, List.of(arrayRef, rexBuilder.makeLiteral("attributes")));
        RexNode mapVal = rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            SqlStdOperatorTable.ITEM,
            List.of(innerItem, rexBuilder.makeLiteral("http.method"))
        );

        String json = jsonOf(asNestedProject(rewrittenProject(scan, List.of(mapVal)).get(0)));
        assertTrue(json, json.contains("\"field\":\"attributes\""));
        assertTrue(json, json.contains("\"key\":\"http.method\""));
    }

    public void testMultiLevelProjectionRejected() {
        // events.spans.name — inner `spans` is an ARRAY, not a MAP → not a map key → 400, not a 500.
        RelDataType innerElem = typeFactory.createStructType(List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("name"));
        RelDataType spansArr = typeFactory.createArrayType(innerElem, -1);
        RelDataType elem = typeFactory.createStructType(List.of(spansArr), List.of("spans"));
        RelDataType eventsArr = typeFactory.createArrayType(elem, -1);
        RelNode scan = arrayScan(eventsArr);

        RexNode arrayRef = rexBuilder.makeInputRef(eventsArr, 0);
        RexNode innerItem = rexBuilder.makeCall(spansArr, SqlStdOperatorTable.ITEM, List.of(arrayRef, rexBuilder.makeLiteral("spans")));
        RexNode leaf = rexBuilder.makeCall(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            SqlStdOperatorTable.ITEM,
            List.of(innerItem, rexBuilder.makeLiteral("name"))
        );
        LogicalProject p = LogicalProject.create(scan, List.of(), List.of(leaf), (List<String>) null);
        expectThrows(UnsupportedFunctionException.class, () -> OpenSearchNestedFieldRewriter.rewrite(p));
    }

    public void testWholeMapProjectionRewritten() {
        // events.attributes where `attributes` is a MAP → single-ITEM whole-map projection:
        // {"field":"attributes"} with no key, returning ARRAY<MAP> (not a scalar leaf).
        RelDataType mapType = typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR)
        );
        RelDataType elem = typeFactory.createStructType(
            List.of(mapType, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            List.of("attributes", "name")
        );
        RelDataType eventsArr = typeFactory.createArrayType(elem, -1);
        RelNode scan = arrayScan(eventsArr);

        RexNode arrayRef = rexBuilder.makeInputRef(eventsArr, 0);
        RexNode wholeMap = rexBuilder.makeCall(mapType, SqlStdOperatorTable.ITEM, List.of(arrayRef, rexBuilder.makeLiteral("attributes")));

        RexCall np = asNestedProject(rewrittenProject(scan, List.of(wholeMap)).get(0));
        assertEquals("{\"field\":\"attributes\"}", jsonOf(np));
        assertEquals(SqlTypeName.ARRAY, np.getType().getSqlTypeName());
        assertEquals(SqlTypeName.MAP, np.getType().getComponentType().getSqlTypeName());
    }

    public void testCastWrappedLeafProjectionRewritten() {
        // Calcite may wrap the ITEM in a CAST; extractProjectPath unwraps it (parity with the filter path).
        RelNode scan = nestedScan();
        RexNode cast = rexBuilder.makeAbstractCast(typeFactory.createSqlType(SqlTypeName.VARCHAR), eventsName());
        RexCall np = asNestedProject(rewrittenProject(scan, List.of(cast)).get(0));
        assertEquals("{\"field\":\"name\"}", jsonOf(np));
    }

    public void testCrossFamilyCastOnNestedLeafProjectionRejected() {
        // cast(events.name as int) — a cross-family cast changes the value, so the projection is
        // rejected with a 400 instead of becoming ARRAY<INTEGER> over a string leaf.
        RelNode scan = nestedScan();
        RexNode cast = rexBuilder.makeAbstractCast(typeFactory.createSqlType(SqlTypeName.INTEGER), eventsName());
        LogicalProject p = LogicalProject.create(scan, List.of(), List.of(cast), (List<String>) null);
        expectThrows(UnsupportedFunctionException.class, () -> OpenSearchNestedFieldRewriter.rewrite(p));
    }

    public void testNonExistentNestedLeafProjectionRejected() {
        // fields events.bogus — 'bogus' isn't a field of the event struct, so the projection is
        // rejected with a 400 rather than a get_field that 500s.
        RelNode scan = nestedScan();
        RexNode bogus = item(EVENTS, eventsArrayType(), "bogus", SqlTypeName.VARCHAR);
        LogicalProject p = LogicalProject.create(scan, List.of(), List.of(bogus), (List<String>) null);
        expectThrows(UnsupportedFunctionException.class, () -> OpenSearchNestedFieldRewriter.rewrite(p));
    }

    // ---- helpers ----

    /** A scan whose only column ($0) is the given array type. */
    private RelNode arrayScan(RelDataType eventsArr) {
        RelDataType rowType = typeFactory.builder().add("events", eventsArr).build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("nested_index"));
        when(table.getRowType()).thenReturn(rowType);
        return stubScan(table);
    }

    /** Runs the rewriter over a project and returns its (rewritten) expressions. */
    private List<RexNode> rewrittenProject(RelNode scan, List<RexNode> exprs) {
        LogicalProject p = LogicalProject.create(scan, List.of(), exprs, (List<String>) null);
        RelNode result = OpenSearchNestedFieldRewriter.rewrite(p);
        assertTrue("rewrite must yield a LogicalProject", result instanceof LogicalProject);
        return ((LogicalProject) result).getProjects();
    }

    private RexCall asNestedProject(RexNode node) {
        assertTrue("expected a RexCall, got " + node, node instanceof RexCall);
        RexCall call = (RexCall) node;
        assertSame(OpenSearchNestedFieldRewriter.NESTED_PROJECT_OP, call.getOperator());
        return call;
    }

    /** Runs the rewriter over a filter and returns its (rewritten) condition. */
    private RexNode rewrittenCondition(RelNode scan, RexNode condition) {
        RelNode result = OpenSearchNestedFieldRewriter.rewrite(makeFilter(scan, condition));
        assertTrue("rewrite must yield a LogicalFilter", result instanceof LogicalFilter);
        return ((LogicalFilter) result).getCondition();
    }

    /** Asserts the rewriter leaves the filter's condition structurally identical (no ITEM-on-array). */
    private void assertUnchanged(RelNode scan, RexNode condition) {
        RelNode result = OpenSearchNestedFieldRewriter.rewrite(makeFilter(scan, condition));
        assertTrue(result instanceof LogicalFilter);
        assertEquals(condition, ((LogicalFilter) result).getCondition());
    }

    /** Asserts an ITEM-on-array predicate the rewriter can't handle is rejected with a 400. */
    private void assertRejected(RelNode scan, RexNode condition) {
        LogicalFilter filter = makeFilter(scan, condition);
        expectThrows(UnsupportedFunctionException.class, () -> OpenSearchNestedFieldRewriter.rewrite(filter));
    }

    private RexCall asNestedAnyMatch(RexNode node) {
        assertTrue("expected a RexCall, got " + node, node instanceof RexCall);
        RexCall call = (RexCall) node;
        assertSame(OpenSearchNestedFieldRewriter.NESTED_ANY_MATCH_OP, call.getOperator());
        return call;
    }

    private RexCall asCall(RexNode node, SqlKind kind) {
        assertEquals(kind, node.getKind());
        return (RexCall) node;
    }

    private static String jsonOf(RexCall nestedAnyMatch) {
        return ((RexLiteral) nestedAnyMatch.getOperands().get(1)).getValueAs(String.class);
    }

    private static int arrayColOf(RexCall nestedAnyMatch) {
        return ((RexInputRef) nestedAnyMatch.getOperands().get(0)).getIndex();
    }

    // -- RexNode builders --

    private RexNode eq(RexNode left, RexNode right) {
        return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
    }

    private RexNode str(String value) {
        return rexBuilder.makeLiteral(value);
    }

    private RexNode intLit(int value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), typeFactory.createSqlType(SqlTypeName.INTEGER));
    }

    private RexNode traceIdRef() {
        return rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), TRACE_ID);
    }

    private RexNode eventsName() {
        return item(EVENTS, eventsArrayType(), "name", SqlTypeName.VARCHAR);
    }

    private RexNode eventsCount() {
        return item(EVENTS, eventsArrayType(), "count", SqlTypeName.INTEGER);
    }

    private RexNode linksTraceId() {
        return item(LINKS, linksArrayType(), "traceId", SqlTypeName.VARCHAR);
    }

    /** Builds {@code ITEM($arrayCol,'field') : leafType} using the explicit-return-type overload. */
    private RexNode item(int arrayCol, RelDataType arrayType, String field, SqlTypeName leafType) {
        return rexBuilder.makeCall(
            typeFactory.createSqlType(leafType),
            SqlStdOperatorTable.ITEM,
            List.of(rexBuilder.makeInputRef(arrayType, arrayCol), rexBuilder.makeLiteral(field))
        );
    }

    // -- schema --

    private RelDataType eventsArrayType() {
        RelDataType element = typeFactory.createStructType(
            List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
            List.of("name", "count")
        );
        return typeFactory.createArrayType(element, -1);
    }

    private RelDataType linksArrayType() {
        RelDataType element = typeFactory.createStructType(List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("traceId"));
        return typeFactory.createArrayType(element, -1);
    }

    private RelNode nestedScan() {
        RelDataType rowType = typeFactory.builder()
            .add("events", eventsArrayType())
            .add("links", linksArrayType())
            .add("traceId", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("nested_index"));
        when(table.getRowType()).thenReturn(rowType);
        return stubScan(table);
    }
}
