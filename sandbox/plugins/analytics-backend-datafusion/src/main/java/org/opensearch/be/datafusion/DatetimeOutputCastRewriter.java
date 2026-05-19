/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-isthmus pass that rewrites the engine-output cast emitted by
 * {@code DatetimeOutputCastRule} from {@code CAST(<TIMESTAMP> AS VARCHAR)} to
 * {@code to_char(<TIMESTAMP>, '%Y-%m-%d %H:%M:%S')} so the DataFusion runtime
 * emits PPL's documented space-separator format instead of Arrow's ISO-8601
 * {@code T}-separator.
 *
 * <p>Issue: <a href="https://github.com/opensearch-project/sql/issues/5420">opensearch-project/sql#5420</a>.
 *
 * <p>Background: {@code DatetimeOutputCastRule} (api/spec/datetime) wraps every
 * datetime field at the OUTERMOST {@link LogicalProject} in
 * {@code CAST(... AS VARCHAR)} so the unified planner never has to know which
 * backend serializes datetimes. Calcite's reference planner emits ANSI
 * {@code "2024-01-15 12:00:00"}; DataFusion's Arrow CAST kernel emits
 * {@code "2024-01-15T12:00:00"}. The session config
 * {@code datafusion.format.timestamp_format} only affects the CLI display
 * pipeline, not the Arrow cast kernel — verified by the issue's reporter.
 *
 * <p>Scope is intentionally narrow:
 * <ul>
 *   <li>Only the output {@link Project} is inspected. {@code DatetimeOutputCastRule}
 *       wraps the input in exactly one final {@link Project}; the unified planner
 *       may then wrap that Project in a single {@link Sort} (system query-size
 *       limit). The rewriter therefore inspects either the root {@link Project}
 *       or, when the root is a {@link Sort}, the {@link Project} sitting
 *       directly beneath it. Any deeper {@link Project} (whether user-authored
 *       or optimizer-generated) carries expressions that must round-trip
 *       verbatim.</li>
 *   <li>Only direct project slots — {@code project.getProjects().get(i)} — are
 *       inspected. Nested casts inside {@code CASE}/{@code COALESCE}/UDF args
 *       were authored by the user query and must round-trip verbatim.</li>
 *   <li>Only {@code CAST(... AS VARCHAR)} matches the rule's output shape;
 *       {@code CAST(... AS CHAR(n))} is user-authored and has different
 *       length/padding semantics that {@code to_char} does not preserve.</li>
 *   <li>Only {@link SqlTypeName#TIMESTAMP} sources are rewritten. PPL's
 *       {@code DATE} (no clock) and {@code TIME} (no calendar) cast cleanly
 *       through Arrow already, and {@link SqlTypeName#TIMESTAMP_WITH_LOCAL_TIME_ZONE}
 *       depends on the DataFusion session timezone — emitting a literal space
 *       format there could silently lie about the instant. Defer until a
 *       concrete failing case lands.</li>
 *   <li>The rewriter assumes {@code DatetimeUdtNormalizeRule} (also a
 *       postAnalysisRule, ordered before {@code DatetimeOutputCastRule}) has
 *       already normalized {@code ExprUDT.EXPR_TIMESTAMP} → standard
 *       {@code SqlTypeName.TIMESTAMP}, so we only need to match the standard
 *       SqlTypeName here.</li>
 * </ul>
 *
 * <p>The Substrait emit path is wired in {@code DataFusionFragmentConvertor}:
 * {@code SqlLibraryOperators.TO_CHAR} is mapped to the Substrait extension
 * name {@code to_char} declared in {@code opensearch_scalar_functions.yaml},
 * which DataFusion resolves to its native {@code to_char} scalar function.
 *
 * @opensearch.internal
 */
final class DatetimeOutputCastRewriter {

    /**
     * PPL's documented timestamp output format (space separator). Mirrors the
     * format used by Calcite's reference planner so the analytics-engine path
     * matches per-row output exactly. The trailing {@code %.f} is chrono's
     * variable-length fractional-second specifier — a leading dot followed by
     * 0-9 digits, omitted when the value has no sub-second precision. This
     * matches PPL's legacy formatting for {@code date} and {@code date_nanos}
     * fields where the displayed precision tracks the source value (e.g.
     * {@code "2024-01-15 10:30:01.23456789"} for a date_nanos with 8 fractional
     * digits, {@code "2025-08-01 03:47:41"} for a whole-second value).
     */
    static final String PPL_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S%.f";

    private DatetimeOutputCastRewriter() {}

    /**
     * Rewrite the engine-output {@code CAST(<TIMESTAMP> AS VARCHAR)} slots in the
     * output {@link Project}. Returns {@code root} unchanged when the output
     * Project cannot be located (e.g. raw scan / aggregate fragment) or when no
     * slot matches.
     *
     * <p>The output Project is located in one of two shapes:
     * <ol>
     *   <li>{@code root} is itself a {@link Project} — the rule's output sits at
     *       the root.</li>
     *   <li>{@code root} is a {@link Sort} (the unified planner's
     *       {@code LogicalSystemLimit} system query-size cap) and its input is a
     *       {@link Project} — rewrite that Project's slots and rebuild the Sort
     *       on top of the rewritten Project.</li>
     * </ol>
     *
     * <p>Matches any {@link Project} subclass — {@code DatetimeOutputCastRule}
     * emits a {@link LogicalProject}, but engine-side optimizer rules
     * (e.g. {@code OpenSearchProjectRule}) may have already converted the
     * matched Project to a custom {@link Project} subclass (e.g.
     * {@code OpenSearchProject}). {@link Project#copy} on the matched subclass
     * round-trips back to the same subclass, so any subclass-specific state
     * (viable backends, traits) is preserved.
     *
     * <p>The traversal is intentionally NOT recursive: only the output Project
     * is rewritten — any deeper {@link Project} carries expressions that must
     * round-trip verbatim.
     */
    static RelNode rewrite(RelNode root) {
        if (root instanceof Project project) {
            Project rewritten = rewriteOutputProject(project);
            return rewritten == project ? root : rewritten;
        }
        if (root instanceof Sort sort && sort.getInput() instanceof Project project) {
            Project rewritten = rewriteOutputProject(project);
            if (rewritten == project) {
                return root;
            }
            return sort.copy(sort.getTraitSet(), rewritten, sort.getCollation(), sort.offset, sort.fetch);
        }
        return root;
    }

    /**
     * Returns a new {@link Project} (same subclass as {@code project}) with
     * engine-output cast slots rewritten, or returns {@code project} unchanged
     * when no slot matched.
     */
    private static Project rewriteOutputProject(Project project) {
        List<RexNode> oldProjects = project.getProjects();
        List<RexNode> newProjects = new ArrayList<>(oldProjects.size());
        boolean changed = false;
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        for (RexNode expr : oldProjects) {
            RexNode rewritten = rewriteDirectOutputCast(expr, rexBuilder);
            if (rewritten != expr) {
                changed = true;
            }
            newProjects.add(rewritten);
        }
        if (!changed) {
            return project;
        }
        return project.copy(project.getTraitSet(), project.getInput(), newProjects, project.getRowType());
    }

    /**
     * Returns a {@code to_char(<expr>, format)} call when {@code expr} is the
     * exact shape {@code CAST(<TIMESTAMP> AS VARCHAR)} produced by
     * {@code DatetimeOutputCastRule}; otherwise returns {@code expr} unchanged.
     *
     * <p>Note: deliberately not recursive — see class-level scope notes.
     */
    private static RexNode rewriteDirectOutputCast(RexNode expr, RexBuilder rexBuilder) {
        if (!(expr instanceof RexCall call) || call.getKind() != SqlKind.CAST) {
            return expr;
        }
        RexNode source = call.getOperands().get(0);
        SqlTypeName sourceType = source.getType().getSqlTypeName();
        SqlTypeName targetType = call.getType().getSqlTypeName();
        if (sourceType != SqlTypeName.TIMESTAMP) {
            return expr;
        }
        // VARCHAR-only: DatetimeOutputCastRule emits CAST(... AS VARCHAR) (length-unspecified).
        // CHAR(n) is user-authored and has length/padding semantics that to_char does not preserve.
        if (targetType != SqlTypeName.VARCHAR) {
            return expr;
        }
        RelDataType formatType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
        RexNode formatLiteral = rexBuilder.makeLiteral(PPL_TIMESTAMP_FORMAT, formatType, true);
        return rexBuilder.makeCall(call.getType(), SqlLibraryOperators.TO_CHAR, List.of(source, formatLiteral));
    }
}
