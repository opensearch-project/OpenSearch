/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Collects, for a raw (pre-marking) logical plan, the set of base-table column names the query
 * actually reads — so cross-index type-conflict validation ({@link IndexResolution}) can be scoped
 * to referenced columns instead of every mapping property. A query that never touches a conflicting
 * field should not be rejected because of it.
 *
 * <p><b>Safety contract.</b> The returned set must be a <em>superset</em> of the columns that reach
 * the scan's emitted schema. Under-counting would let a conflicting column flow into the single
 * Arrow VSR / Substrait {@code base_schema} unvalidated, trading a clean plan-time
 * "incompatible field types" error for an opaque execution-time coercion crash. To guarantee that
 * never happens, this collector:
 * <ul>
 *   <li>propagates used columns <b>top-down</b> (from the root's output down to the scan), the only
 *       direction that cannot under-count a pruning operator;</li>
 *   <li>treats {@code Filter}/{@code Sort} as pass-through (all input columns used if the output is
 *       used), and only {@code Project}/{@code Aggregate} as pruning;</li>
 *   <li>always treats every {@code Aggregate} group key as used (grouping reads it regardless of
 *       whether an ancestor selects it);</li>
 *   <li>returns {@code null} — meaning "validate all fields", the pre-existing behavior — for
 *       anything it cannot analyze precisely: more than one table scan, joins, set operations,
 *       correlations, any other node type, or any unexpected error.</li>
 * </ul>
 * A {@code null} result is always safe (it just preserves the old validate-everything behavior);
 * a non-null result is only ever returned when the whole tree is a single-scan chain of the
 * handled operators.
 *
 * @opensearch.internal
 */
final class ReferencedFieldCollector {

    private ReferencedFieldCollector() {}

    /**
     * Returns the base-table column names read by {@code root}, or {@code null} to signal that the
     * caller should validate all fields (unanalyzable shape or error).
     */
    static Set<String> collect(RelNode root) {
        try {
            Collector c = new Collector();
            // The root's entire output is consumed by the caller of the query.
            c.propagate(root, ImmutableBitSet.range(root.getRowType().getFieldCount()));
            // Only trust the result for a single-scan plan; multi-scan (join/union) fell back to null.
            return c.scanCount == 1 ? c.names : null;
        } catch (RuntimeException e) {
            // Any shape we did not anticipate → validate all (safe, pre-existing behavior).
            return null;
        }
    }

    /** Signals an unanalyzable shape; caught by {@link #collect} and turned into a validate-all fallback. */
    private static final class ValidateAllFieldsException extends RuntimeException {
        ValidateAllFieldsException() {
            super(null, null, false, false);
        }
    }

    private static final class Collector {
        private final Set<String> names = new HashSet<>();
        private int scanCount;

        /** Records that {@code usedOut} output columns of {@code node} are consumed, recursing to the scan. */
        void propagate(RelNode node, ImmutableBitSet usedOut) {
            if (node instanceof TableScan scan) {
                scanCount++;
                if (scanCount > 1) {
                    throw new ValidateAllFieldsException(); // multiple scans → validate all
                }
                List<RelDataTypeField> fields = scan.getRowType().getFieldList();
                for (int col : usedOut) {
                    if (col >= 0 && col < fields.size()) {
                        names.add(fields.get(col).getName());
                    }
                }
                return;
            }
            if (node instanceof Project project) {
                // Only the referenced output expressions matter; collect their input refs.
                ImmutableBitSet.Builder usedIn = ImmutableBitSet.builder();
                List<RexNode> projects = project.getProjects();
                for (int out : usedOut) {
                    if (out >= 0 && out < projects.size()) {
                        collectRefs(projects.get(out), usedIn);
                    }
                }
                propagate(project.getInput(), usedIn.build());
                return;
            }
            if (node instanceof Filter filter) {
                // Pass-through 1:1: every used output column is also a used input column, plus the
                // columns the predicate reads.
                ImmutableBitSet.Builder usedIn = ImmutableBitSet.builder();
                usedIn.addAll(usedOut);
                collectRefs(filter.getCondition(), usedIn);
                propagate(filter.getInput(), usedIn.build());
                return;
            }
            if (node instanceof Sort sort) {
                // Pass-through 1:1, plus the sort keys.
                ImmutableBitSet.Builder usedIn = ImmutableBitSet.builder();
                usedIn.addAll(usedOut);
                sort.getCollation().getFieldCollations().forEach(fc -> usedIn.set(fc.getFieldIndex()));
                propagate(sort.getInput(), usedIn.build());
                return;
            }
            if (node instanceof Aggregate aggregate) {
                // Output = [group keys ..., agg calls ...]. Group keys are always read by the
                // grouping itself; agg-call args are read when that call's output is used.
                ImmutableBitSet.Builder usedIn = ImmutableBitSet.builder();
                usedIn.addAll(aggregate.getGroupSet());
                int groupCount = aggregate.getGroupCount();
                List<AggregateCall> calls = aggregate.getAggCallList();
                for (int out : usedOut) {
                    int callIdx = out - groupCount;
                    if (callIdx >= 0 && callIdx < calls.size()) {
                        AggregateCall call = calls.get(callIdx);
                        call.getArgList().forEach(usedIn::set);
                        if (call.filterArg >= 0) {
                            usedIn.set(call.filterArg);
                        }
                    }
                }
                propagate(aggregate.getInput(), usedIn.build());
                return;
            }
            // Join, Correlate, SetOp (Union/Intersect/Minus), Values, or any node type we do not
            // model precisely → cannot safely scope; validate all.
            throw new ValidateAllFieldsException();
        }

        private static void collectRefs(RexNode expr, ImmutableBitSet.Builder into) {
            expr.accept(new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitInputRef(RexInputRef inputRef) {
                    into.set(inputRef.getIndex());
                    return null;
                }
            });
        }
    }
}
