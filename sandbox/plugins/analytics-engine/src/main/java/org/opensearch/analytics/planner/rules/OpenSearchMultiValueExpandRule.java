/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchMultiValueExpand;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;

import java.util.List;

/**
 * HEP marker for {@link OpenSearchMultiValueExpand}. The rel is emitted pre-marking by
 * {@code OpenSearchMultiValueGroupByRewriter} with no viable backends; this rule fills them in from
 * the (already-marked, bottom-up) child.
 *
 * <p>No capability negotiation is needed: the expansion is a per-row unnest that every backend
 * able to scan the LIST column can perform, and it lowers to the backend's
 * {@code MULTI_VALUE_EXPAND} extension at fragment conversion — the same path an explicit
 * {@code mvexpand} takes. Viable backends are therefore exactly the child's.
 *
 * @opensearch.internal
 */
public class OpenSearchMultiValueExpandRule extends RelOptRule {

    @SuppressWarnings("unused")
    private final PlannerContext context;

    public OpenSearchMultiValueExpandRule(PlannerContext context) {
        super(operand(OpenSearchMultiValueExpand.class, operand(RelNode.class, any())), "OpenSearchMultiValueExpandRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchMultiValueExpand expand = call.rel(0);
        // Already marked (a second HEP pass, or a copyResolved product) — nothing to do.
        return expand.getViableBackends().isEmpty();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchMultiValueExpand expand = call.rel(0);
        RelNode child = RelNodeUtils.unwrapHep(call.rel(1));
        if (!(child instanceof OpenSearchRelNode osChild)) {
            throw new IllegalStateException("MultiValueExpand rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }
        List<String> viableBackends = osChild.getViableBackends();
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("MultiValueExpand child has no viable backends: " + child);
        }
        call.transformTo(
            new OpenSearchMultiValueExpand(expand.getCluster(), child.getTraitSet(), child, expand.getFieldIndex(), viableBackends)
        );
    }
}
