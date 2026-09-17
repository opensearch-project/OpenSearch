/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.RuntimeFilterFunction;
import org.opensearch.analytics.spi.RuntimeFilterInstructionNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Run-time half of the shuffle family: attaches a computed filter payload to the stages whose plans carry
 * the matching predicate.
 *
 * <p>The union itself moved to {@link RuntimeFilterMergeSink}, which ORs contributions as they arrive. The
 * buffered variant that used to live here had to be given a byte budget of {@code bloomBytes × shards} up
 * front, and that reservation is what exhausted the native memory pool once a query planted several
 * filters.
 *
 * <p>Separate from {@link ShuffleRuntimeFilters}, which decides at plan time which filter to build and
 * plants the predicate. Nothing here has any say in whether a filter exists — by this point the
 * predicate is already in the plan, and the only question is what value it resolves to.
 *
 * <p>Fails open throughout. An unreadable capture, a size disagreement between two shards, or a build
 * side that produced nothing all yield no payload, and a predicate with no payload keeps every row
 * ({@code os_runtime_filter} returns true for an id it does not know). The cost of any failure here is
 * the optimization, never a row.
 *
 * @opensearch.internal
 */
public final class ShuffleRuntimeFilterPayload {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleRuntimeFilterPayload.class);

    private ShuffleRuntimeFilterPayload() {}

    /**
     * Appends a {@link RuntimeFilterInstructionNode} IN PLACE to every stage whose plan carries a probe
     * predicate for one of these filter ids, and returns how many payloads were attached — payloads rather
     * than stages, so the number is comparable with the pre-pass count that precedes it in the funnel.
     *
     * <p>Stages are located by the predicate in their fragment, not by the stage id the filter was
     * planned against. The shuffle promotion between planting and here may split, copy or renumber a
     * producer, and the invariant that actually matters is narrower than any id: the payload must reach
     * wherever the predicate ended up. Matching on the predicate states exactly that.
     *
     * <p>Must run at the same point as {@code UnifiedDispatch.injectBroadcastsInPlace} and for the same
     * reason — the promotion replaces every stage's plan alternatives, so anything attached earlier is
     * discarded. In place and additive, so it composes with that injection and with the shuffle
     * enrichment that follows.
     */
    public static int attach(Stage stage, Map<Integer, byte[]> payloadsByFilterId) {
        if (stage == null || payloadsByFilterId.isEmpty()) {
            return 0;
        }
        int attached = attachToStage(stage, payloadsByFilterId);
        for (Stage child : stage.getChildStages()) {
            attached += attach(child, payloadsByFilterId);
        }
        return attached;
    }

    private static int attachToStage(Stage stage, Map<Integer, byte[]> payloadsByFilterId) {
        if (stage.getFragment() == null) {
            return 0;
        }
        List<InstructionNode> toAdd = new ArrayList<>();
        for (int filterId : plantedFilterIds(stage.getFragment())) {
            byte[] payload = payloadsByFilterId.get(filterId);
            if (payload != null) {
                toAdd.add(new RuntimeFilterInstructionNode(filterId, payload));
            }
        }
        if (toAdd.isEmpty()) {
            return 0;
        }
        if (stage.getPlanAlternatives().isEmpty()) {
            // An instruction lives on a plan alternative, so a stage with none has nowhere to carry it. That
            // means this ran outside the post-conversion slot it is documented for, and the payload would
            // vanish without a trace; the query is still correct, so say so and move on.
            LOGGER.warn("[runtime-filter] stage {} carries a probe predicate but has no plan to attach {} to", stage.getStageId(), toAdd);
            return 0;
        }
        List<StagePlan> enriched = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            List<InstructionNode> merged = new ArrayList<>(plan.instructions());
            merged.addAll(toAdd);
            enriched.add(plan.withInstructions(merged));
        }
        stage.setPlanAlternatives(enriched);
        LOGGER.debug("[runtime-filter] stage {} gained payload(s) {}", stage.getStageId(), toAdd);
        // Payloads, not stages. The count sits at the end of a funnel whose previous step counts payloads,
        // and since the application point is searched for downward, several filters legitimately land on the
        // SAME leaf — a fact table joined to two dimensions is filtered by both. Returning 1 per stage made
        // the funnel narrow at the last step by exactly the number of extra payloads per stage, which reads
        // as pre-passes computed for nothing.
        return toAdd.size();
    }

    /**
     * Filter ids appearing in a probe predicate anywhere in this fragment, in encounter order.
     *
     * <p>Public because "which runtime filters does this plan carry?" is the question that decides where a
     * payload goes, and it is worth asserting on a real fragment from outside this package.
     */
    public static Set<Integer> plantedFilterIds(RelNode fragment) {
        Set<Integer> ids = new LinkedHashSet<>();
        if (fragment == null) {
            return ids;
        }
        RexVisitorImpl<Void> collector = new RexVisitorImpl<>(/* deep */ true) {
            @Override
            public Void visitCall(RexCall call) {
                if (call.getOperator() == RuntimeFilterFunction.PROBE
                    && call.getOperands().size() == 2
                    && call.getOperands().get(0) instanceof RexLiteral literal) {
                    Integer id = literal.getValueAs(Integer.class);
                    if (id != null) {
                        ids.add(id);
                    }
                }
                return super.visitCall(call);
            }
        };
        for (OpenSearchFilter filter : RelNodeUtils.findNodes(fragment, OpenSearchFilter.class)) {
            RexNode condition = filter.getCondition();
            if (condition != null) {
                condition.accept(collector);
            }
        }
        return ids;
    }
}
