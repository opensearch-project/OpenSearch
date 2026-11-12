/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchProject;

import java.util.List;

/**
 * Whether a join permits reducing one of its inputs to the other side's keys.
 *
 * <p>Shared by both runtime-filter families — the broadcast one derives a can-match value set, the
 * shuffle one plants a probe predicate — because the question is the same and getting it wrong drops
 * rows either way.
 *
 * @opensearch.internal
 */
public final class RuntimeFilterEligibility {

    private RuntimeFilterEligibility() {}

    /**
     * Whether the join's output can be reduced by filtering input {@code probeIdx} to the other
     * side's keys.
     *
     * <p>Sound only when a probe row with no match cannot appear in the output. That fails two ways:
     * an outer join preserves its row-generating side, so filtering that side would drop rows which
     * belong in the result null-extended; and an ANTI join's output is exactly the probe rows that do
     * <em>not</em> match, so filtering to the keys that do match is backwards.
     *
     * <p>Calcite states "preserved" indirectly — nulls are generated on the side that is
     * <em>not</em> preserved, so input 0 is preserved precisely when nulls can appear on input 1. The
     * resulting permission set (INNER and SEMI on either side, RIGHT for the left input, LEFT for the
     * right input) is the same one Spark encodes as {@code canPruneLeft} / {@code canPruneRight},
     * which is a useful independent check on the derivation.
     */
    public static boolean canFilterProbeSide(JoinRelType joinType, int probeIdx) {
        if (joinType == JoinRelType.ANTI) {
            return false;
        }
        boolean probeSideIsPreserved = probeIdx == 0 ? joinType.generatesNullsOnRight() : joinType.generatesNullsOnLeft();
        return !probeSideIsPreserved;
    }

    /**
     * Calcite types a shuffle runtime filter can be built and probed on.
     *
     * <p>The backend's build aggregate and probe predicate both declare exact signatures over the signed
     * integral widths and the two date widths, and nothing else — there is no coercion step that would
     * widen a string or a decimal into one of them. Planting on any other type is therefore not a
     * degraded filter but an <em>unresolvable</em> one, and because the probe predicate goes into the main
     * query plan rather than the pre-pass, it fails the whole query. That is the one outcome this feature
     * must never produce: a join that worked with the setting off must not fail with it on.
     *
     * <p>Refusing is always safe, so the list is deliberately narrow rather than complete. Notably
     * TIMESTAMP is excluded even though a date is accepted: it does not map to one of the date widths the
     * signature names, and being wrong about that costs a working query while being conservative costs
     * one optimization. Widen this only alongside a backend signature that provably accepts the type.
     */
    /**
     * True when no project below {@code root} redefines {@code column}, so the name means the same values at
     * the join as it does at the scan.
     *
     * <p>Both families locate the application point by NAME — the shuffle family plants a predicate at the
     * scan exposing the key, the broadcast family hands the name to the data node as a parquet column. A
     * project can keep a name while changing what it holds ({@code eval id = id + 1}), and then the name
     * matches at both ends while the values do not: the summary was built from the join's view of the key,
     * and the filter would test the scan's raw column against it. Rows whose computed key <em>is</em> in the
     * filter get rejected on the strength of a raw value that is not — a lost result row, silently. A rename
     * in the other direction is harmless by comparison: the name simply stops resolving, and no filter is
     * applied.
     *
     * <p>Only a true pass-through is accepted. The output field must be a bare input reference <em>and</em>
     * the referenced input field must carry the same name; an identity reference alone is not enough,
     * because {@code eval id = other_col} is also a bare reference and also leaves a field called
     * {@code id} whose values come from a different column than the scan's {@code id}.
     */
    public static boolean keyPassesThroughProjectsUnchanged(RelNode root, String column) {
        for (OpenSearchProject project : RelNodeUtils.findNodes(root, OpenSearchProject.class)) {
            List<String> outputNames = project.getRowType().getFieldNames();
            List<String> inputNames = project.getInput().getRowType().getFieldNames();
            List<RexNode> exprs = project.getProjects();
            for (int i = 0; i < outputNames.size() && i < exprs.size(); i++) {
                if (!column.equals(outputNames.get(i))) {
                    continue;
                }
                if (!(exprs.get(i) instanceof RexInputRef ref)) {
                    return false;
                }
                int source = ref.getIndex();
                if (source < 0 || source >= inputNames.size() || !column.equals(inputNames.get(source))) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isSupportedFilterKeyType(RelDataType type) {
        if (type == null) {
            return false;
        }
        return switch (type.getSqlTypeName()) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DATE -> true;
            default -> false;
        };
    }
}
