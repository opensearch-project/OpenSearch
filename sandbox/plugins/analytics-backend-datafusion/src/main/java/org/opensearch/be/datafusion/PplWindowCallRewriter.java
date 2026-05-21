/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlAggFunction;

/**
 * Pre-isthmus pass that rewrites PPL window-based BRAIN expressions onto a flat
 * call chain rooted at {@link DataFusionFragmentConvertor#LOCAL_INTERNAL_PATTERN_WINDOW_OP}
 * so substrait emission sees only concrete types (no {@code MAP<VARCHAR, ANY>}).
 *
 * <p>Substitution rule: every {@link RexOver} whose aggregation operator is the
 * PPL {@code INTERNAL_PATTERN} ({@code SqlKind.OTHER} with name {@code "pattern"})
 * is replaced by a new {@code RexOver} targeting
 * {@link DataFusionFragmentConvertor#LOCAL_INTERNAL_PATTERN_WINDOW_OP} with a
 * {@code VARCHAR} return type. The Rust window UDF returns the matched wildcard
 * pattern string per row directly.
 *
 * <p>{@code OpenSearchProject#liftNestedRexOver} hoists every {@code RexOver} into
 * its own child Project before fragment conversion, so the rewriter only ever
 * sees the RexOver in a "leaf" position — never wrapped inside a deeper scalar
 * expression. The outer Project's {@code PATTERN_PARSER(field, $hoisted, showToken)}
 * call ends up referencing the hoisted column via {@code RexInputRef}; the
 * downstream {@link ItemTypeRebuilder} pass detects the resulting
 * {@code array_element(map_extract(PATTERN_PARSER(field, RexInputRef, showToken),
 * key), 1)} chain and routes it past PATTERN_PARSER entirely.
 *
 * <p>Mirrors {@link PplAggregateCallRewriter} for the aggregate side.
 *
 * @opensearch.internal
 */
final class PplWindowCallRewriter {

    private PplWindowCallRewriter() {}

    static RelNode rewrite(RelNode root) {
        return root.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = super.visit(other);
                RexShuttle shuttle = new WindowRewriteShuttle(visited.getCluster().getRexBuilder());
                RelNode rewritten = visited.accept(shuttle);
                return rewritten == null ? visited : rewritten;
            }
        });
    }

    private static final class WindowRewriteShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;

        WindowRewriteShuttle(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitOver(RexOver over) {
            SqlAggFunction op = over.getAggOperator();
            if (op == DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_WINDOW_OP) {
                return over;
            }
            if (!"PATTERN".equalsIgnoreCase(op.getName())) {
                return super.visitOver(over);
            }
            // NOTE: in-progress. Substituting the RexOver's return type from
            // MAP<VARCHAR, ANY> ARRAY (PPL declared) to VARCHAR (matches our Rust
            // window UDF's actual emission) breaks the parent Project's frozen
            // RexInputRef type — Calcite hoists this RexOver into its own child
            // Project via {@code OpenSearchProject#liftNestedRexOver}, and the
            // parent's RexInputRef captured the original MAP ARRAY type at
            // construction. Rebuilding the operator alone isn't enough; we'd need
            // to walk up and re-create the parent Project's row type and all
            // dependent RexInputRefs. Left as a no-op for now — see PR description
            // / task #39 for the gap analysis.
            return super.visitOver(over);
        }
    }
}
