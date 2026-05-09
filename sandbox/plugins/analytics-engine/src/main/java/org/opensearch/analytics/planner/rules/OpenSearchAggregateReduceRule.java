/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;

import java.util.EnumSet;
import java.util.List;

/**
 * OpenSearch-aware subclass of Calcite's {@link AggregateReduceFunctionsRule}. Reuses
 * Calcite's tested decomposition for multi-field statistical aggregates (AVG, STDDEV_POP,
 * STDDEV_SAMP, VAR_POP, VAR_SAMP) instead of hand-rolling the same primitive-reduction
 * logic inside our resolver.
 *
 * <p>Customisations over the stock rule:
 * <ul>
 *   <li>Matches {@link OpenSearchAggregate} (not {@code LogicalAggregate}) so the rule
 *       fires only on marked aggregates, and only when the mode is {@link AggregateMode#SINGLE}
 *       — after Volcano's {@link OpenSearchAggregateSplitRule} has produced PARTIAL/FINAL
 *       pairs, re-firing this rule would be nonsensical (FINAL's arg doesn't refer to a
 *       scan column anymore).</li>
 *   <li>Overrides {@link #newAggregateRel} so the reduced inner aggregate is rebuilt as
 *       an {@link OpenSearchAggregate} preserving {@code mode} and {@code viableBackends}.
 *       Without this override, the stock rule would emit a {@code LogicalAggregate} and
 *       the split rule — which pattern-matches on {@code OpenSearchAggregate} — would
 *       no longer fire on the reduced inner aggregate.</li>
 *   <li>Restricts the reduction set via {@code functionsToReduce} to the specific
 *       aggregate kinds pf4 currently exercises. Extending the set is a one-line change
 *       when new statistical aggregates are onboarded.</li>
 * </ul>
 *
 * <p>Rule ordering: this rule must fire <em>before</em> {@link OpenSearchAggregateSplitRule}
 * produces PARTIAL/FINAL pairs. {@code PlannerImpl} places this rule in the HEP marking
 * phase alongside {@link OpenSearchAggregateRule}; the marking rule converts
 * {@code LogicalAggregate → OpenSearchAggregate} and this rule then reduces the
 * SINGLE-mode aggregate's AVG/STDDEV/VAR calls into primitive SUM/COUNT/SUM_SQ calls
 * wrapped by a scalar {@code Project}. The subsequent Volcano split rule operates on the
 * already-reduced inner aggregate.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateReduceRule extends AggregateReduceFunctionsRule {

    /**
     * SqlKinds this rule reduces. Narrowed to AVG for pf4 scope — AVG's reduction uses only
     * SUM, COUNT, DIVIDE, and CAST, all of which are either capability-declared aggregates
     * or baseline scalars ({@link OpenSearchProjectRule#BASELINE_SCALAR_OPS}).
     *
     * <p>STDDEV_POP / STDDEV_SAMP / VAR_POP / VAR_SAMP reductions emit {@code POWER(x, 2)}
     * for x² which is not in the baseline set — backends would need to declare POWER
     * capability before these can be safely reduced here. Extending the set is a one-line
     * change once STDDEV / VAR land on a backend that declares POWER.
     */
    private static final EnumSet<SqlKind> FUNCTIONS_TO_REDUCE = EnumSet.of(SqlKind.AVG);

    public OpenSearchAggregateReduceRule() {
        super(OpenSearchAggregate.class, RelBuilder.proto(Contexts.empty()), FUNCTIONS_TO_REDUCE);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchAggregate agg = call.rel(0);
        // Only decompose SINGLE aggregates. After OpenSearchAggregateSplitRule runs
        // (in the Volcano phase), PARTIAL/FINAL aggregates have their arg-list rebound
        // to the exchange output and no longer semantically match this rule's contract.
        if (agg.getMode() != AggregateMode.SINGLE) {
            return false;
        }
        return super.matches(call);
    }

    /**
     * Overrides the stock rule's aggregate construction so the reduced inner aggregate is
     * an {@link OpenSearchAggregate} carrying the original {@code mode} and
     * {@code viableBackends}. Without this, the stock rule would emit a bare
     * {@code LogicalAggregate} and subsequent marking / split rules pattern-matching on
     * {@link OpenSearchAggregate} would miss it.
     */
    @Override
    protected void newAggregateRel(RelBuilder relBuilder, Aggregate oldAggRel, List<AggregateCall> newCalls) {
        OpenSearchAggregate src = (OpenSearchAggregate) oldAggRel;
        OpenSearchAggregate reduced = new OpenSearchAggregate(
            src.getCluster(),
            src.getTraitSet(),
            relBuilder.peek(),
            src.getGroupSet(),
            src.getGroupSets(),
            newCalls,
            src.getMode(),
            src.getViableBackends()
        );
        relBuilder.push(reduced);
    }
}
