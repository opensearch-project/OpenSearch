/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source source license.
 */

package org.opensearch.analytics.planner.rules;

import com.fasterxml.jackson.databind.json.JsonMapper;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.UnsupportedFunctionException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre-marking relational rewriter that turns {@code ITEM($arrayCol,'field')} references in
 * {@link LogicalFilter} conditions into {@link #NESTED_ANY_MATCH_OP} scalar-function calls, each
 * encoding the per-element predicate tree as a JSON string literal.
 *
 * <p>The rewrite is an allowlist, not exhaustive: {@link ExprTreeBuilder} accepts only the shapes
 * below. An {@code ITEM}-on-array predicate that doesn't match is rejected with a 400
 * ({@link UnsupportedFunctionException}) rather than left to error at execution.
 *
 * <p><b>Covered</b> (rewritten):
 * <ul>
 *   <li>Leaf comparisons {@code = != > >= < <=} (e.g. {@code events.name="x"}, {@code events.count>0}).</li>
 *   <li>Boolean connectives over them: {@code AND}, {@code OR}, {@code NOT}.</li>
 *   <li>Null checks on a leaf: {@code isnotnull}/{@code isnull} → {@code EXISTS}/{@code NOT_EXISTS}.</li>
 *   <li>Same-array field-to-field comparison (e.g. {@code events.a = events.b}).</li>
 *   <li>Multiple leaves on one array — fused into a single call so one element must satisfy all of
 *       them (matching vanilla OpenSearch's nested block-join semantics).</li>
 *   <li>Multiple arrays combined with {@code AND} — one call each (independent existentials).</li>
 *   <li>A nested predicate combined with parent/scalar conjuncts (row-level AND, or OR-split).</li>
 * </ul>
 *
 * <p><b>Not covered</b> (rejected with a 400 {@link UnsupportedFunctionException}):
 * <ul>
 *   <li>Arithmetic on a leaf (e.g. {@code events.count + 1 > 5}).</li>
 *   <li>A NULL literal (e.g. {@code events.x = null} — use {@code isnull}).</li>
 *   <li>Cross-array correlation (e.g. {@code events.x = links.y}).</li>
 *   <li>Multiple arrays combined with {@code OR} (e.g. {@code events.x = 1 OR links.y = 2}).</li>
 *   <li>Array-within-array (multi-level nested) descent (e.g. {@code spans.events.name}).</li>
 *   <li>Map-value paths (e.g. {@code events.attributes.foo}).</li>
 *   <li>Any other operator or function ({@code LIKE}, {@code CIDRMATCH}, string/date functions, …).
 *       {@code IN}/{@code BETWEEN} are covered only if Calcite pre-expanded them to the operators
 *       above; a {@code SEARCH}/{@code Sarg} form is not.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class OpenSearchNestedFieldRewriter {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchNestedFieldRewriter.class);

    /** Thread-safe once built; reused for every expr-tree serialization. */
    private static final JsonMapper JSON = JsonMapper.builder().build();

    /**
     * Synthetic scalar function: {@code nested_any_match(arrayCol, '<json expr tree>') → BOOLEAN}.
     *
     * <p>Emitted by the filter rewrite for a predicate on a single array column. The second argument
     * is a JSON string holding the per-element predicate tree (node shapes in {@link ExprTreeBuilder});
     * the Rust {@code NestedAnyMatchRewriteRule} turns this call into a native {@code array_any_match}
     * HOF before execution.
     *
     * <p>Top-level AND conjuncts on the same array are fused into one call, so a single element must
     * satisfy the whole condition — matching vanilla OpenSearch's joint-element semantics.
     */
    // TODO(native-array_any_match): once DataFusion round-trips a Substrait HOF+lambda, emit
    // array_any_match(col, e -> ...) as a RexLambda here instead of this op + JSON; then ExprTreeBuilder/JSON go away.
    public static final SqlFunction NESTED_ANY_MATCH_OP = new SqlFunction(
        "NESTED_ANY_MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /**
     * Synthetic scalar function: {@code nested_project(arrayCol, '<path json>') → ARRAY<leaf>}.
     *
     * <p>Emitted by the project rewrite for a nested sub-path access (`fields events.name`). The
     * second argument is a JSON path — {@code {"field":"name"}} for a struct leaf / whole map, or
     * {@code {"field":"attributes","key":"<k>"}} for a map value. The Rust {@code NestedProjectRewriteRule}
     * rewrites this to a native {@code array_transform} HOF before execution.
     *
     * <p>Return type is set explicitly per call to {@code ARRAY<leafType>} (grain-preserving: one
     * per-row array of the sub-path across elements). {@link ReturnTypes#ARG0} here is a harmless
     * default — every {@code makeCall} passes the explicit array type.
     */
    // TODO(native-array_transform): once DataFusion round-trips a Substrait HOF+lambda, emit
    // array_transform(col, e -> get_field(e,'field')) as a RexLambda here instead of this op + JSON.
    public static final SqlFunction NESTED_PROJECT_OP = new SqlFunction(
        "NESTED_PROJECT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private OpenSearchNestedFieldRewriter() {}

    /** Rewrites every {@code ITEM}-on-array filter condition in the tree to use {@link #NESTED_ANY_MATCH_OP}. */
    public static RelNode rewrite(RelNode root) {
        RelNode result = root.accept(new NestedShuttle());
        if (result != root) {
            LOGGER.debug("OpenSearchNestedFieldRewriter: rewrote nested filter predicate");
        }
        return result;
    }

    private static final class NestedShuttle extends RelShuttleImpl {
        @Override
        public RelNode visit(LogicalFilter filter) {
            LogicalFilter visited = (LogicalFilter) super.visitChildren(filter);
            return rewriteFilter(visited);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            LogicalProject visited = (LogicalProject) super.visitChildren(project);
            return rewriteProject(visited);
        }
    }

    private static RelNode rewriteFilter(LogicalFilter filter) {
        RelNode input = filter.getInput();
        int arrayCol = firstArrayColReferenced(List.of(filter.getCondition()), input.getRowType());
        if (arrayCol < 0) {
            return filter;
        }
        RelOptCluster cluster = filter.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        int[] offendingArrayCol = { -1 };
        RexNode rewrittenCondition = tryRewriteToNestedAnyMatch(
            filter.getCondition(),
            arrayCol,
            input.getRowType(),
            rexBuilder,
            offendingArrayCol
        );
        if (rewrittenCondition != null) {
            LOGGER.debug("OpenSearchNestedFieldRewriter: filter rewritten to nested_any_match (no unnest, row count preserved)");
            return LogicalFilter.create(input, rewrittenCondition);
        }

        // There is an ITEM-on-array predicate (arrayCol >= 0) we couldn't rewrite — arithmetic,
        // cross-array correlation, multi-level nesting, a map-value path, LIKE/SEARCH, etc. Reject
        // with a 400 rather than leave a raw ITEM-on-array in the plan: marking treats NESTED as
        // filter-capable, so it would delegate the raw predicate to DataFusion and fail at execution
        // with an opaque 500. A clear UnsupportedFunctionException is the honest, actionable error.
        int reportedCol = offendingArrayCol[0] >= 0 ? offendingArrayCol[0] : arrayCol;
        String field = input.getRowType().getFieldList().get(reportedCol).getName();
        throw new UnsupportedFunctionException(
            "nested predicate on '" + field + "'",
            "in this form; supported on a nested leaf: comparisons, AND/OR/NOT, isnull/isnotnull"
        );
    }

    // ── Projection: fields events.name / events.attributes / events.attributes.<key> ──
    // A nested sub-path is ITEM-over-array; rewrite it to NESTED_PROJECT($col, <path>), which the
    // Rust rule lowers to array_transform(col, e -> get_field(e, path)) → ARRAY<leaf> (grain
    // preserved: one per-row array of the sub-path across the row's elements).

    private static RelNode rewriteProject(LogicalProject project) {
        RelNode input = project.getInput();
        RelDataType inputRowType = input.getRowType();
        if (firstArrayColReferenced(project.getProjects(), inputRowType) < 0) {
            return project;
        }
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        List<RexNode> newProjects = new ArrayList<>(project.getProjects().size());
        boolean changed = false;
        for (RexNode expr : project.getProjects()) {
            RexNode rewritten = rewriteProjectExpr(expr, inputRowType, rexBuilder);
            if (rewritten != expr) {
                changed = true;
            }
            newProjects.add(rewritten);
        }
        if (!changed) {
            return project;
        }
        LOGGER.debug("OpenSearchNestedFieldRewriter: project rewritten to nested_project (row count preserved)");
        return LogicalProject.create(input, project.getHints(), newProjects, project.getRowType().getFieldNames());
    }

    private static RexNode rewriteProjectExpr(RexNode expr, RelDataType inputRowType, RexBuilder rexBuilder) {
        ProjectPath path = extractProjectPath(expr, inputRowType);
        if (path != null) {
            return buildNestedProjectCall(path, expr.getType(), inputRowType, rexBuilder);
        }
        // References a nested array via ITEM but not as a clean sub-path (e.g. UPPER(events.name),
        // events.count + 1) — unsupported in projection; reject with a 400, not a runtime error.
        if (firstArrayColReferenced(List.of(expr), inputRowType) >= 0) {
            throw new UnsupportedFunctionException(
                "nested projection",
                "in this form; project a nested leaf, map, or map key directly (e.g. `fields events.name`)"
            );
        }
        return expr;
    }

    /** A nested sub-path in a projection: array column index + struct field, plus an optional map key. */
    private record ProjectPath(int arrayCol, String field, String mapKey) {
    }

    /** Extracts a clean {@code ITEM}(-on-{@code ITEM})-over-array sub-path, or {@code null}. */
    private static ProjectPath extractProjectPath(RexNode expr, RelDataType inputRowType) {
        // Only unwrap a same-family CAST (e.g. char/varchar, or a numeric widening). A cross-family
        // cast like cast(events.name as int) changes the value, so return null and let the caller
        // reject it with a 400 rather than drop the cast. Same rule as the filter path.
        while (expr instanceof RexCall cast && cast.getKind() == SqlKind.CAST && cast.getOperands().size() == 1) {
            RexNode castOperand = cast.getOperands().get(0);
            var castFamily = cast.getType().getFamily();
            if (castFamily == null || !castFamily.equals(castOperand.getType().getFamily())) {
                return null;
            }
            expr = castOperand;
        }
        if (!(expr instanceof RexCall outer) || !"ITEM".equals(outer.getOperator().getName()) || outer.getOperands().size() != 2) {
            return null;
        }
        if (!(outer.getOperands().get(1) instanceof RexLiteral k1) || k1.getTypeName() != SqlTypeName.CHAR) {
            return null;
        }
        String outerKey = k1.getValueAs(String.class);
        RexNode arg0 = outer.getOperands().get(0);
        // Single ITEM: ITEM($arrayCol, 'field') — a struct leaf or whole map. The field must exist in
        // the element struct; if not, return null so we reject with a 400 instead of a get_field 500.
        if (arg0 instanceof RexInputRef ref && isArrayCol(ref.getIndex(), inputRowType)) {
            if (!elementHasField(ref.getIndex(), outerKey, inputRowType)) {
                return null;
            }
            return new ProjectPath(ref.getIndex(), outerKey, null);
        }
        // Double ITEM: ITEM(ITEM($arrayCol,'mapField'), 'mapKey') — a map value. Only valid when
        // 'mapField' is actually a MAP in the element struct; otherwise (e.g. an inner ARRAY =
        // array-within-array, `events.spans.name`) it is NOT a map key — return null so the caller
        // rejects it with a 400 instead of emitting a map_extract that 500s at execution.
        if (arg0 instanceof RexCall inner
            && "ITEM".equals(inner.getOperator().getName())
            && inner.getOperands().size() == 2
            && inner.getOperands().get(0) instanceof RexInputRef ref2
            && isArrayCol(ref2.getIndex(), inputRowType)
            && inner.getOperands().get(1) instanceof RexLiteral k2
            && k2.getTypeName() == SqlTypeName.CHAR
            && isMapField(ref2.getIndex(), k2.getValueAs(String.class), inputRowType)) {
            return new ProjectPath(ref2.getIndex(), k2.getValueAs(String.class), outerKey);
        }
        return null;
    }

    private static boolean isArrayCol(int index, RelDataType inputRowType) {
        return index < inputRowType.getFieldCount()
            && inputRowType.getFieldList().get(index).getType().getSqlTypeName() == SqlTypeName.ARRAY;
    }

    /** True if the array element struct has a field named {@code field}. */
    private static boolean elementHasField(int arrayCol, String field, RelDataType inputRowType) {
        RelDataType elementType = inputRowType.getFieldList().get(arrayCol).getType().getComponentType();
        return elementType != null && elementType.isStruct() && elementType.getField(field, true, false) != null;
    }

    /** True if {@code field} of the array element's struct is a MAP (so {@code field[key]} is a map value). */
    private static boolean isMapField(int arrayCol, String field, RelDataType inputRowType) {
        RelDataType elementType = inputRowType.getFieldList().get(arrayCol).getType().getComponentType();
        if (elementType == null || !elementType.isStruct()) {
            return false;
        }
        RelDataTypeField f = elementType.getField(field, true, false);
        return f != null && f.getType().getSqlTypeName() == SqlTypeName.MAP;
    }

    private static RexNode buildNestedProjectCall(ProjectPath path, RelDataType leafType, RelDataType inputRowType, RexBuilder rexBuilder) {
        Map<String, Object> pathMap = new LinkedHashMap<>();
        pathMap.put("field", path.field());
        if (path.mapKey() != null) {
            pathMap.put("key", path.mapKey());
        }
        String json;
        try {
            json = JSON.writeValueAsString(pathMap);
        } catch (Exception e) {
            throw new UnsupportedFunctionException("nested projection", "path could not be serialized");
        }
        RexNode arrayRef = rexBuilder.makeInputRef(inputRowType.getFieldList().get(path.arrayCol()).getType(), path.arrayCol());
        RexNode pathLit = rexBuilder.makeLiteral(json);
        // Grain-preserving: one ARRAY<leaf> per row (nullable — a null row yields a null array).
        RelDataType arrayType = rexBuilder.getTypeFactory().createArrayType(leafType, -1);
        RelDataType returnType = rexBuilder.getTypeFactory().createTypeWithNullability(arrayType, true);
        return rexBuilder.makeCall(returnType, NESTED_PROJECT_OP, List.of(arrayRef, pathLit));
    }

    private static RexNode tryRewriteToNestedAnyMatch(
        RexNode condition,
        int arrayCol,
        RelDataType inputRowType,
        RexBuilder rexBuilder,
        int[] offendingArrayCol
    ) {
        if (condition.getKind() == SqlKind.OR) {
            RexNode orSplit = tryOrSplitRewrite(condition, arrayCol, inputRowType, rexBuilder, offendingArrayCol);
            if (orSplit != null) {
                return orSplit;
            }
        }
        List<RexNode> conjuncts = condition.getKind() == SqlKind.AND ? ((RexCall) condition).getOperands() : List.of(condition);

        // Group conjuncts by the array they reference (supports predicates spanning multiple arrays).
        // Different arrays are independent existentials, so each gets its own NESTED_ANY_MATCH;
        // same-array conjuncts fuse into one call (per group below) so a single element satisfies all
        // of them. Pure-parent (scalar) conjuncts stay at row level. LinkedHashMap keeps array order
        // deterministic. Example:
        // events.name="a" AND links.traceId="t"
        // -> AND( NESTED_ANY_MATCH($events, {name="a"}), NESTED_ANY_MATCH($links, {traceId="t"}) )
        LinkedHashMap<Integer, List<RexNode>> conjunctsByArray = new LinkedHashMap<>();
        List<RexNode> parentConjuncts = new ArrayList<>();
        for (RexNode conjunct : conjuncts) {
            int col = firstArrayColReferenced(List.of(conjunct), inputRowType);
            if (col < 0) {
                parentConjuncts.add(conjunct);
            } else {
                conjunctsByArray.computeIfAbsent(col, k -> new ArrayList<>()).add(conjunct);
            }
        }
        if (conjunctsByArray.isEmpty()) {
            return null;
        }

        List<RexNode> anyMatchCalls = new ArrayList<>(conjunctsByArray.size());
        for (Map.Entry<Integer, List<RexNode>> entry : conjunctsByArray.entrySet()) {
            int col = entry.getKey();
            ExprTreeBuilder builder = new ExprTreeBuilder(col, inputRowType);
            List<Map<String, Object>> arrayTrees = new ArrayList<>();
            for (RexNode conjunct : entry.getValue()) {
                Map<String, Object> tree = builder.build(conjunct);
                if (tree == null || !tree.containsKey("op")) {
                    // Reject when the conjunct is unrepresentable (a cross-array comparison like
                    // `events.x = links.y` or a map-value path), OR when it builds to a rootless
                    // field/lit node with no "op" — a bare boolean leaf like `where events.isError`,
                    // which the Rust consumer can't lower. Record the offending array so the 400 names
                    // it. Returning null (rather than emitting a rootless tree) keeps this a clean 400
                    // instead of a placeholder that survives to execution as a 500.
                    offendingArrayCol[0] = col;
                    return null;
                }
                arrayTrees.add(tree);
            }
            Map<String, Object> combinedTree = arrayTrees.size() == 1 ? arrayTrees.get(0) : opNode("AND", arrayTrees);
            RexNode anyMatchCall = buildAnyMatchExprCall(combinedTree, col, inputRowType, rexBuilder);
            if (anyMatchCall == null) {
                return null;
            }
            anyMatchCalls.add(anyMatchCall);
        }

        // AND the per-array existential calls together with any pure-parent conjuncts, at row level.
        List<RexNode> allConjuncts = new ArrayList<>(anyMatchCalls.size() + parentConjuncts.size());
        allConjuncts.addAll(anyMatchCalls);
        allConjuncts.addAll(parentConjuncts);
        if (allConjuncts.size() == 1) {
            return allConjuncts.get(0);
        }
        return rexBuilder.makeCall(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN), SqlStdOperatorTable.AND, allConjuncts);
    }

    private static RexNode tryOrSplitRewrite(
        RexNode condition,
        int arrayCol,
        RelDataType inputRowType,
        RexBuilder rexBuilder,
        int[] offendingArrayCol
    ) {
        List<RexNode> operands = ((RexCall) condition).getOperands();
        ExprTreeBuilder builder = new ExprTreeBuilder(arrayCol, inputRowType);
        List<RexNode> arrayOperands = new ArrayList<>();
        List<RexNode> parentOperands = new ArrayList<>();
        for (RexNode operand : operands) {
            if (builder.containsItemOnArray(operand)) {
                arrayOperands.add(operand);
            } else {
                parentOperands.add(operand);
            }
        }
        if (arrayOperands.isEmpty() || parentOperands.isEmpty()) {
            return null;
        }
        // A parent operand that references a *different* array is an independent existential the
        // OR-split doesn't model; leave the filter unchanged rather than emit a raw ITEM-on-array.
        for (RexNode operand : parentOperands) {
            if (firstArrayColReferenced(List.of(operand), inputRowType) >= 0) {
                return null;
            }
        }

        List<Map<String, Object>> arrayTrees = new ArrayList<>();
        for (RexNode operand : arrayOperands) {
            Map<String, Object> tree = builder.build(operand);
            if (tree == null || !tree.containsKey("op")) {
                // Rootless field/lit (e.g. a bare boolean leaf) or unrepresentable shape — reject cleanly.
                offendingArrayCol[0] = arrayCol;
                return null;
            }
            arrayTrees.add(tree);
        }
        Map<String, Object> combinedArrayTree = arrayTrees.size() == 1 ? arrayTrees.get(0) : opNode("OR", arrayTrees);
        RexNode anyMatchCall = buildAnyMatchExprCall(combinedArrayTree, arrayCol, inputRowType, rexBuilder);
        if (anyMatchCall == null) {
            return null;
        }

        List<RexNode> allOperands = new ArrayList<>(parentOperands.size() + 1);
        allOperands.add(anyMatchCall);
        allOperands.addAll(parentOperands);
        return rexBuilder.makeCall(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN), SqlStdOperatorTable.OR, allOperands);
    }

    private static RexNode buildAnyMatchExprCall(Map<String, Object> tree, int arrayCol, RelDataType inputRowType, RexBuilder rexBuilder) {
        String json;
        try {
            json = JSON.writeValueAsString(tree);
        } catch (Exception e) {
            LOGGER.warn("OpenSearchNestedFieldRewriter: failed to serialize expr tree", e);
            return null;
        }
        RexNode arrayRef = rexBuilder.makeInputRef(inputRowType.getFieldList().get(arrayCol).getType(), arrayCol);
        RexNode exprLit = rexBuilder.makeLiteral(json);
        return rexBuilder.makeCall(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN),
            NESTED_ANY_MATCH_OP,
            List.of(arrayRef, exprLit)
        );
    }

    /** Builds an ordered {@code {"op":..,"args":..}} node so the serialized JSON key order is deterministic. */
    private static Map<String, Object> opNode(String op, List<?> args) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("op", op);
        node.put("args", args);
        return node;
    }

    /**
     * Finds the first array-column index referenced by an {@code ITEM($arrayCol,'field')} anywhere
     * within the given expressions, or -1 if none.
     */
    private static int firstArrayColReferenced(List<RexNode> exprs, RelDataType inputRowType) {
        ItemFinder finder = new ItemFinder(inputRowType);
        for (RexNode e : exprs) {
            e.accept(finder);
        }
        return finder.arrayCol;
    }

    private static final class ItemFinder extends RexShuttle {
        private final RelDataType inputRowType;
        private int arrayCol = -1;

        ItemFinder(RelDataType inputRowType) {
            this.inputRowType = inputRowType;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (arrayCol < 0) {
                int c = itemArrayCol(call, inputRowType);
                if (c >= 0) {
                    arrayCol = c;
                }
            }
            return super.visitCall(call);
        }
    }

    /**
     * Builds a JSON-serializable expression tree describing the per-element predicate for
     * {@link #NESTED_ANY_MATCH_OP}. Returns {@code null} for unsupported shapes.
     */
    private static final class ExprTreeBuilder {
        private final int arrayCol;
        private final RelDataType inputRowType;

        ExprTreeBuilder(int arrayCol, RelDataType inputRowType) {
            this.arrayCol = arrayCol;
            this.inputRowType = inputRowType;
        }

        Map<String, Object> build(RexNode node) {
            if (node instanceof RexCall itemCall && "ITEM".equals(itemCall.getOperator().getName()) && itemCall.getOperands().size() == 2) {
                RexNode arrayOperand = itemCall.getOperands().get(0);
                RexNode fieldNode = itemCall.getOperands().get(1);
                if (arrayOperand instanceof RexInputRef ref
                    && fieldNode instanceof RexLiteral lit
                    && lit.getTypeName() == SqlTypeName.CHAR) {
                    if (ref.getIndex() != arrayCol) {
                        return null;
                    }
                    return Map.of("field", lit.getValueAs(String.class));
                }
                return null;
            }

            if (node instanceof RexLiteral lit) {
                Object value;
                if (lit.getTypeName() == SqlTypeName.CHAR || lit.getTypeName() == SqlTypeName.VARCHAR) {
                    value = lit.getValueAs(String.class);
                } else {
                    value = lit.getValueAs(Comparable.class);
                }
                if (value == null) {
                    // NULL literal — `= null` is not a valid element predicate (use isnull()).
                    // Stop rewriting.
                    return null;
                }
                return Map.of("lit", value);
            }

            if (node instanceof RexCall call) {
                if (call.getKind() == SqlKind.CAST) {
                    // Unwrap only a cast that keeps the value's type family (an implicit char<->varchar
                    // or numeric-width cast Calcite inserts). A cast that crosses families (e.g.
                    // cast(name as int)) would change the comparison and we can't represent it in the
                    // element tree, so bail — rewriteFilter then rejects with a 400 rather than silently
                    // dropping the cast.
                    RexNode castOperand = call.getOperands().get(0);
                    var castFamily = call.getType().getFamily();
                    if (castFamily != null && castFamily.equals(castOperand.getType().getFamily())) {
                        return build(castOperand);
                    }
                    return null;
                }
                String opSymbol = opSymbolFor(call);
                if (opSymbol == null) {
                    // Operator we can't represent in the JSON tree — return null; rewriteFilter rejects with a 400.
                    return null;
                }
                List<Object> args = new ArrayList<>(call.getOperands().size());
                for (RexNode operand : call.getOperands()) {
                    Map<String, Object> argTree = build(operand);
                    if (argTree == null) {
                        return null;
                    }
                    args.add(argTree);
                }
                return opNode(opSymbol, args);
            }

            return null;
        }

        boolean containsItemOnArray(RexNode node) {
            if (node instanceof RexCall call) {
                if ("ITEM".equals(call.getOperator().getName())
                    && call.getOperands().size() == 2
                    && itemArrayCol(call, inputRowType) == arrayCol) {
                    return true;
                }
                for (RexNode op : call.getOperands()) {
                    if (containsItemOnArray(op)) return true;
                }
            }
            return false;
        }

        // Only the operators the Rust NestedAnyMatchRewriteRule can consume — comparisons and
        // boolean connectives. Anything else (e.g. arithmetic) returns null so build() bails and
        // marking rejects the filter, rather than emitting JSON the consumer can't lower.
        private static String opSymbolFor(RexCall call) {
            return switch (call.getKind()) {
                case AND -> "AND";
                case OR -> "OR";
                case NOT -> "NOT";
                case GREATER_THAN -> ">";
                case GREATER_THAN_OR_EQUAL -> ">=";
                case LESS_THAN -> "<";
                case LESS_THAN_OR_EQUAL -> "<=";
                case EQUALS -> "=";
                case NOT_EQUALS -> "!=";
                case IS_NOT_NULL -> "EXISTS";
                case IS_NULL -> "NOT_EXISTS";
                default -> null;
            };
        }
    }

    /** Returns the array-column index if {@code call} is {@code ITEM($N,'field')} with {@code $N} an ARRAY column; else -1. */
    private static int itemArrayCol(RexCall call, RelDataType inputRowType) {
        if (!"ITEM".equals(call.getOperator().getName()) || call.getOperands().size() != 2) {
            return -1;
        }
        RexNode fieldNode = call.getOperands().get(1);
        if (!(fieldNode instanceof RexLiteral lit) || lit.getTypeName() != SqlTypeName.CHAR) {
            return -1;
        }
        RexNode arrayRef = call.getOperands().get(0);
        while (arrayRef instanceof RexCall innerCall
            && "ITEM".equals(innerCall.getOperator().getName())
            && innerCall.getOperands().size() == 2
            && innerCall.getOperands().get(1) instanceof RexLiteral innerLit
            && innerLit.getTypeName() == SqlTypeName.CHAR) {
            arrayRef = innerCall.getOperands().get(0);
        }
        if (!(arrayRef instanceof RexInputRef ref)) {
            return -1;
        }
        int colIndex = ref.getIndex();
        if (colIndex >= inputRowType.getFieldCount()) {
            return -1;
        }
        RelDataType colType = inputRowType.getFieldList().get(colIndex).getType();
        return colType.getSqlTypeName() == SqlTypeName.ARRAY ? colIndex : -1;
    }
}
