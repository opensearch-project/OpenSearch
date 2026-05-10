/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import java.util.EnumSet;

/**
 * OpenSearch-aware configuration of Calcite's {@link AggregateReduceFunctionsRule}. Reuses
 * Calcite's tested decomposition for multi-field statistical aggregates (AVG, STDDEV_POP,
 * STDDEV_SAMP, VAR_POP, VAR_SAMP) instead of hand-rolling the same primitive-reduction
 * logic inside our resolver.
 *
 * <p><b>Order</b>: this rule operates on plain {@link LogicalAggregate} so it fires
 * <em>before</em> {@link OpenSearchAggregateRule} marks the aggregate. Running on the
 * un-marked plan keeps Calcite's type inference clean — the reduce rule sees an aggregate
 * whose {@code aggCall.rexList} is empty, so the reduced SUM/COUNT calls get their
 * natural primitive return types (BIGINT for SUM of integer, not AVG's carry-over DOUBLE).
 * The marking rule then converts the already-reduced plan to {@link
 * org.opensearch.analytics.planner.rel.OpenSearchAggregate} with correctly-typed
 * primitive aggregate calls, and the Volcano split rule downstream operates on those
 * primitives.
 *
 * <p><b>Reduction set</b>: narrowed via {@code FUNCTIONS_TO_REDUCE} to AVG only. AVG's
 * reduction uses only SUM, COUNT, DIVIDE, and CAST — all either capability-declared
 * aggregates or baseline scalar operators ({@link OpenSearchProjectRule#BASELINE_SCALAR_OPS}).
 * STDDEV_POP / STDDEV_SAMP / VAR_POP / VAR_SAMP also emit {@code POWER(x, 2)} which is
 * not in the baseline set; extending the set is a one-line change once a backend declares
 * POWER as a project capability.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateReduceRule extends AggregateReduceFunctionsRule {

    private static final EnumSet<SqlKind> FUNCTIONS_TO_REDUCE = EnumSet.of(SqlKind.AVG);

    public OpenSearchAggregateReduceRule() {
        super(LogicalAggregate.class, RelBuilder.proto(Contexts.empty()), FUNCTIONS_TO_REDUCE);
    }
}
