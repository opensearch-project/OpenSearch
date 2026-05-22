/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;

import java.util.List;

/**
 * Pre-isthmus pass that rewrites {@code CAST(<IpType> AS VARCHAR)} and
 * {@code CAST(<BinaryType> AS VARCHAR)} in the output {@link Project}'s direct
 * slots to {@code ip_to_string(<col>)} / {@code binary_to_base64(<col>)} UDF
 * calls. Without this rewrite, DataFusion's built-in {@code cast(binary, utf8)}
 * kernel buffer-reinterprets the 16-byte ipv6-mapped buffer as Latin-1 — emitting
 * garbage strings (or NULL when the bytes aren't valid UTF-8) for queries like
 * {@code | eval s = cast(host as STRING)}.
 *
 * <p>Direct structural clone of {@link DatetimeOutputCastRewriter}: same
 * {@code rewrite(RelNode)} entry, same scope rules (output Project only, only
 * direct slots, only {@link SqlKind#CAST} — {@code SAFE_CAST} is not produced
 * by PPL {@code cast()}), same Project subclass-preservation via
 * {@link Project#copy}.
 *
 * <p>The two emitted UDF calls bind through the analytics-engine's existing
 * Substrait extension catalog (see {@code opensearch_scalar_functions.yaml}).
 * Their Rust implementations live in
 * {@code analytics-backend-datafusion/rust/src/udf/ip_to_string.rs} and
 * {@code binary_to_base64.rs}.
 *
 * @opensearch.internal
 */
final class IpBinaryOutputCastRewriter {

    /**
     * UDF call emitted in place of {@code CAST(<IpType> AS VARCHAR)}. Bound to the
     * Substrait extension {@code "ip_to_string"} via
     * {@link DataFusionFragmentConvertor}'s function mapping table; resolved to
     * the Rust UDF in {@code rust/src/udf/ip_to_string.rs}.
     */
    static final SqlOperator IP_TO_STRING_OP = new SqlFunction(
        "ip_to_string",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM
    );

    /**
     * UDF call emitted in place of {@code CAST(<BinaryType> AS VARCHAR)}. Bound to
     * the Substrait extension {@code "binary_to_base64"} via
     * {@link DataFusionFragmentConvertor}'s function mapping table; resolved to
     * the Rust UDF in {@code rust/src/udf/binary_to_base64.rs}.
     */
    static final SqlOperator BINARY_TO_BASE64_OP = new SqlFunction(
        "binary_to_base64",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM
    );

    private IpBinaryOutputCastRewriter() {}

    /**
     * Rewrite every {@code CAST(<IpType|BinaryType> AS VARCHAR)} in any
     * {@link Project} in the tree (output Project, intermediate eval Project,
     * inner casts in CASE/COALESCE expressions, etc.). PPL syntax allows users
     * to write {@code cast(host as STRING)} anywhere — unlike
     * {@link DatetimeOutputCastRewriter} which targets only the rule-emitted
     * output cast — so the scope here is broader.
     *
     * <p>Walks the RelNode tree via {@link RelShuttleImpl}, and within each
     * {@link Project} walks every RexNode subtree via {@link RexShuttle} so
     * casts buried inside CASE/COALESCE/UDF args are also rewritten.
     *
     * <p>Returns {@code root} unchanged when no slot matches.
     */
    static RelNode rewrite(RelNode root) {
        return root.accept(new ProjectShuttle());
    }

    /**
     * Visits every Project in the plan, rewriting any matching CAST in its
     * project list (recursively). Other RelNode types pass through unchanged.
     */
    private static final class ProjectShuttle extends RelShuttleImpl {
        @Override
        public RelNode visit(LogicalProject project) {
            // Recurse into children first so nested Projects (subqueries, eval
            // chains) get rewritten before this one.
            RelNode visited = super.visit(project);
            if (!(visited instanceof Project p)) {
                return visited;
            }
            return rewriteProject(p);
        }
    }

    /**
     * Returns a new {@link Project} with every IP/Binary cast (at any nesting
     * depth in any expression) rewritten, or returns {@code project} unchanged
     * when no rewrite fired.
     */
    private static Project rewriteProject(Project project) {
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        CastRewriteShuttle shuttle = new CastRewriteShuttle(rexBuilder);
        List<RexNode> newProjects = shuttle.apply(project.getProjects());
        if (!shuttle.changed) {
            return project;
        }
        return project.copy(project.getTraitSet(), project.getInput(), newProjects, project.getRowType());
    }

    /**
     * RexShuttle that rewrites {@code CAST(<IpType|BinaryType> AS VARCHAR)} at
     * any depth into the corresponding UDF call. Other RexNodes pass through
     * via {@link RexShuttle}'s default deep-copy behaviour.
     */
    private static final class CastRewriteShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;
        boolean changed;

        CastRewriteShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            // Recurse into operands first so nested casts (e.g. inside CASE)
            // get rewritten bottom-up.
            RexCall visited = (RexCall) super.visitCall(call);
            // Match both CAST and SAFE_CAST. PPL's `cast(... as STRING)` lowers via
            // `rexBuilder.makeCast(targetType, expr, true /* matchNullability */, true /* safe */)`
            // — the `safe` flag produces a SAFE_CAST RexKind, not a CAST.
            SqlKind kind = visited.getKind();
            if (kind != SqlKind.CAST && kind != SqlKind.SAFE_CAST) {
                return visited;
            }
            if (visited.getType().getSqlTypeName() != SqlTypeName.VARCHAR) {
                return visited;
            }
            RexNode source = visited.getOperands().get(0);
            if (source.getType() instanceof IpType) {
                changed = true;
                return rexBuilder.makeCall(visited.getType(), IP_TO_STRING_OP, List.of(source));
            }
            if (source.getType() instanceof BinaryType) {
                changed = true;
                return rexBuilder.makeCall(visited.getType(), BINARY_TO_BASE64_OP, List.of(source));
            }
            return visited;
        }
    }
}
