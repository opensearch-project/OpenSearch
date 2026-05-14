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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.List;

/**
 * Converts {@link Sort} → {@link OpenSearchSort}.
 *
 * <p>Validates that the chosen backend supports {@link EngineCapability#SORT}.
 *
 * <p>TODO: for multi-shard Sort+Limit, the split into partial sort
 * per shard + final merge sort at coordinator happens via CBO trait
 * propagation (same as aggregate split).
 *
 * @opensearch.internal
 */
public class OpenSearchSortRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchSortRule(PlannerContext context) {
        super(operand(Sort.class, operand(RelNode.class, any())), "OpenSearchSortRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        RelNode child = call.rel(1);

        if (sort instanceof OpenSearchSort) {
            return;
        }

        if (!(child instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException("Sort rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        // Drop a pure-Fetch Sort over an Aggregate (possibly via a Project chain). Aggregate
        // output cardinality is bounded by group count, so a JOIN_SUBSEARCH_MAXOUT-style 50000
        // limit is structurally never hit. The Sort/Fetch shape here also triggers a DataFusion
        // hang for composite-group keys. Removing it keeps the safety contract intact for
        // unbounded children but avoids the redundant Fetch when the aggregate already bounds
        // output.
        if (sort.getCollation().getFieldCollations().isEmpty() && sort.offset == null && hasAggregateUnderProjects(child)) {
            call.transformTo(child);
            return;
        }

        // Drop a no-fetch outer Sort when an inner OpenSearchSort with fetch already produces
        // the same ordering through a Project chain. With both Sorts present, DataFusion's
        // logical-plan optimizer eliminates the inner Sort as redundant but leaves the Limit,
        // then physical-planning pushes Limit down through CoalescePartitionsExec — so fetch
        // is applied BEFORE sort against the unsorted Aggregate output.
        if (sort.fetch == null && sort.offset == null && !sort.getCollation().getFieldCollations().isEmpty()) {
            OpenSearchSort inner = findInnerSortWithFetchThroughProjects(child);
            if (inner != null && outerCollationMatchesInner(sort, child, inner)) {
                call.transformTo(child);
                return;
            }
        }

        List<String> childViableBackends = openSearchChild.getViableBackends();
        List<String> sortCapable = context.getCapabilityRegistry().operatorBackends(EngineCapability.SORT);

        List<String> viableBackends = childViableBackends.stream().filter(sortCapable::contains).toList();

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports SORT capability among " + childViableBackends);
        }

        // plus(): Calcite's Sort constructor asserts the trait set contains the collation.
        // replace() is a no-op if the slot is missing; plus() appends or overrides.
        call.transformTo(
            new OpenSearchSort(
                sort.getCluster(),
                child.getTraitSet().plus(sort.getCollation()),
                RelNodeUtils.unwrapHep(sort.getInput()),
                sort.getCollation(),
                sort.offset,
                sort.fetch,
                viableBackends
            )
        );
    }

    private static boolean hasAggregateUnderProjects(RelNode node) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        while (current instanceof OpenSearchProject project) {
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        return current instanceof OpenSearchAggregate;
    }

    /** Walks down through OpenSearchProjects, returns the first OpenSearchSort with fetch != null. */
    private static OpenSearchSort findInnerSortWithFetchThroughProjects(RelNode node) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        while (current instanceof OpenSearchProject project) {
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        return current instanceof OpenSearchSort innerSort && innerSort.fetch != null ? innerSort : null;
    }

    /**
     * Checks that the outer Sort's collation, when its field references are remapped down
     * through each intermediate OpenSearchProject's identity-projection exprs, matches the
     * inner Sort's collation field-for-field.
     */
    private static boolean outerCollationMatchesInner(Sort outer, RelNode child, OpenSearchSort inner) {
        List<RelFieldCollation> outerFields = outer.getCollation().getFieldCollations();
        List<RelFieldCollation> innerFields = inner.getCollation().getFieldCollations();
        if (outerFields.size() != innerFields.size()) {
            return false;
        }
        for (int i = 0; i < outerFields.size(); i++) {
            RelFieldCollation outerField = outerFields.get(i);
            int remapped = remapInputIndexThroughProjects(outerField.getFieldIndex(), child, inner);
            if (remapped < 0) {
                return false;
            }
            RelFieldCollation innerField = innerFields.get(i);
            if (remapped != innerField.getFieldIndex()
                || outerField.getDirection() != innerField.getDirection()
                || outerField.nullDirection != innerField.nullDirection) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks `node` down to `inner`, translating `index` through each OpenSearchProject's
     * exprs. Each project's expr at position `i` must be a RexInputRef for the index to
     * remap; non-identity exprs return -1 (unmappable).
     */
    private static int remapInputIndexThroughProjects(int index, RelNode node, OpenSearchSort inner) {
        RelNode current = RelNodeUtils.unwrapHep(node);
        int idx = index;
        while (current instanceof OpenSearchProject project) {
            if (idx < 0 || idx >= project.getProjects().size()) {
                return -1;
            }
            RexNode expr = project.getProjects().get(idx);
            if (!(expr instanceof RexInputRef ref)) {
                return -1;
            }
            idx = ref.getIndex();
            current = RelNodeUtils.unwrapHep(project.getInput());
        }
        return current == inner ? idx : -1;
    }
}
