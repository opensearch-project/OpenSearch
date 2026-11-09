/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file to be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Uncollect;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchCorrelate;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.List;

/** Marks the frontend's {@code mvexpand} Correlate+Uncollect shape for DataFusion execution. */
public class OpenSearchCorrelateRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchCorrelateRule(PlannerContext context) {
        super(operand(Correlate.class, any()), "OpenSearchCorrelateRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Correlate correlate = call.rel(0);
        if (correlate instanceof OpenSearchCorrelate) {
            return false;
        }
        RelNode right = RelNodeUtils.unwrapHep(correlate.getRight());
        if (right instanceof Sort sort) {
            right = RelNodeUtils.unwrapHep(sort.getInput());
        }
        return right instanceof Uncollect;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Correlate correlate = call.rel(0);
        RelNode left = RelNodeUtils.unwrapHep(correlate.getLeft());
        if (!(left instanceof OpenSearchRelNode openSearchLeft)) {
            throw new IllegalStateException("Correlate rule encountered unmarked left child [" + left.getClass().getSimpleName() + "]");
        }
        List<String> expansionBackends = context.getCapabilityRegistry().operatorBackends(EngineCapability.MULTI_VALUE_EXPAND);
        List<String> viableBackends = openSearchLeft.getViableBackends().stream().filter(expansionBackends::contains).toList();
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No viable backend supports multi-value expansion");
        }
        call.transformTo(
            new OpenSearchCorrelate(
                correlate.getCluster(),
                left.getTraitSet(),
                left,
                RelNodeUtils.unwrapHep(correlate.getRight()),
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType(),
                viableBackends
            )
        );
    }
}
