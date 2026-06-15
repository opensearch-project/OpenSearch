/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.spi.AggregateFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Post-CBO rewriter that inserts a per-partition Sort+Limit between PARTIAL and ER
 * for TopK aggregation queries. Runs before DAGBuilder splits stages.
 *
 * <p>Pattern: Sort(collation) → [Project] → Aggregate(FINAL) → ER → Aggregate(PARTIAL) → Scan
 */
public final class OpenSearchTopKRewriter {

    private OpenSearchTopKRewriter() {}

    public static Optional<RelNode> rewrite(RelNode root, PlannerContext context) {
        SortAboveFinal match = findSortAboveFinal(root);
        if (match == null) return Optional.empty();

        OpenSearchSort sort = match.sort;
        OpenSearchAggregate finalAgg = match.finalAgg;

        if (finalAgg.getGroupSet().isEmpty()) return Optional.empty();

        RelNode erNode = finalAgg.getInput();
        if (!(erNode instanceof OpenSearchExchangeReducer er)) return Optional.empty();
        // TODO: Consider applying TopK even for single-shard topologies in a fast-followup.
        if (er.getInputs().isEmpty()) return Optional.empty();
        RelNode partialNode = er.getInputs().get(0);
        if (!(partialNode instanceof OpenSearchAggregate partial) || partial.getMode() != AggregateMode.PARTIAL) {
            return Optional.empty();
        }

        double factor = resolveOversamplingFactor(context);
        if (factor <= 0.0) return Optional.empty();

        // coordLimit is the rank the coordinator must satisfy after merging shard streams: for
        // `head N from M` it skips M then takes N, so the merged stream needs the global top-(M+N).
        // Non-literal offsets bail to no-pushdown (PPL only emits literal offsets).
        long fetch = (sort.fetch instanceof RexLiteral lit) ? RexLiteral.intValue(lit) : 10_000L;
        long offset;
        if (sort.offset == null) {
            offset = 0L;
        } else if (sort.offset instanceof RexLiteral lit) {
            offset = RexLiteral.intValue(lit);
        } else {
            return Optional.empty();
        }
        long coordLimit = offset + fetch;
        long shardSize = (long) Math.ceil(coordLimit * factor) + coordLimit;
        if (shardSize > Integer.MAX_VALUE) return Optional.empty();

        RexBuilder rb = sort.getCluster().getRexBuilder();
        RexNode shardSizeLiteral = rb.makeLiteral(
            (int) shardSize,
            sort.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER),
            true
        );

        // Remap collation through the intermediate Project (if any) between Sort and FINAL Aggregate.
        // PPL emits measures-first output via a column-swapping Project (e.g. Project(c=$1, key=$0)).
        // The outer Sort's indices reference the Project's output; the shard Sort must reference
        // the PARTIAL Aggregate's output (group-keys first, measures second).
        RelCollation adjustedCollation = sort.getCollation();
        if (match.intermediateProject != null) {
            List<RelFieldCollation> remapped = new ArrayList<>();
            for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
                RexNode expr = match.intermediateProject.getProjects().get(fc.getFieldIndex());
                if (expr instanceof RexInputRef ref) {
                    remapped.add(new RelFieldCollation(ref.getIndex(), fc.getDirection(), fc.nullDirection));
                } else {
                    return Optional.empty();
                }
            }
            adjustedCollation = RelCollations.of(remapped);
        }

        RelNode sortInput = partial;

        if (hasEngineNativeMergeMeasure(partial, adjustedCollation)) {
            RelDataType partialRowType = partial.getRowType();
            int fieldCount = partialRowType.getFieldCount();
            int groupCount = partial.getGroupSet().cardinality();

            List<RexNode> projects = new ArrayList<>();
            List<String> names = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                projects.add(rb.makeInputRef(partialRowType.getFieldList().get(i).getType(), i));
                names.add(partialRowType.getFieldNames().get(i));
            }

            List<RelFieldCollation> newCollations = new ArrayList<>();
            for (RelFieldCollation fc : adjustedCollation.getFieldCollations()) {
                int sortIdx = fc.getFieldIndex();
                if (sortIdx >= groupCount && AggregateFunction.isEngineNativeMerge(partial.getAggCallList().get(sortIdx - groupCount))) {
                    AggregateFunction aggFunc = AggregateFunction.fromSqlAggFunction(
                        partial.getAggCallList().get(sortIdx - groupCount).getAggregation()
                    );
                    RexNode aggNameLiteral = rb.makeLiteral(aggFunc.reduceEvalName());
                    RexNode stateRef = rb.makeInputRef(partialRowType.getFieldList().get(sortIdx).getType(), sortIdx);
                    RexNode reduceCall = rb.makeCall(AggregateFunction.REDUCE_EVAL_OP, aggNameLiteral, stateRef);
                    int newIdx = projects.size();
                    projects.add(reduceCall);
                    names.add("__reduce_eval_" + sortIdx);
                    newCollations.add(new RelFieldCollation(newIdx, fc.getDirection(), fc.nullDirection));
                } else {
                    newCollations.add(fc);
                }
            }

            RelDataType reduceEvalRowType = sort.getCluster()
                .getTypeFactory()
                .createStructType(projects.stream().map(RexNode::getType).toList(), names);
            sortInput = new OpenSearchProject(
                sort.getCluster(),
                partial.getTraitSet(),
                partial,
                projects,
                reduceEvalRowType,
                partial.getViableBackends()
            );
            adjustedCollation = RelCollations.of(newCollations);
        }

        OpenSearchSort shardSort = new OpenSearchSort(
            sort.getCluster(),
            partial.getTraitSet(),
            sortInput,
            adjustedCollation,
            null,
            shardSizeLiteral,
            partial.getViableBackends(),
            true
        );

        RelNode topKSubtree = shardSort;
        if (sortInput != partial) {
            RelDataType sortOutputType = shardSort.getRowType();
            int originalFieldCount = partial.getRowType().getFieldCount();
            List<RexNode> stripProjects = new ArrayList<>();
            List<String> stripNames = new ArrayList<>();
            for (int i = 0; i < originalFieldCount; i++) {
                stripProjects.add(rb.makeInputRef(sortOutputType.getFieldList().get(i).getType(), i));
                stripNames.add(partial.getRowType().getFieldNames().get(i));
            }
            topKSubtree = new OpenSearchProject(
                sort.getCluster(),
                partial.getTraitSet(),
                shardSort,
                stripProjects,
                partial.getRowType(),
                partial.getViableBackends()
            );
        }

        RelNode newER = er.copy(er.getTraitSet(), List.of(topKSubtree));
        RelNode newFinal = finalAgg.copy(finalAgg.getTraitSet(), List.of(newER));

        RelNode result = replaceInTree(root, finalAgg, newFinal);
        return Optional.of(result);
    }

    /**
     * Walks the tree to find the <em>deepest</em> collated Sort above a FINAL aggregate. Recursing
     * before matching is deliberate: PPL {@code ... | sort c | head N} arrives as a collated outer
     * {@code LogicalSystemLimit(fetch=querySizeLimit)} over the user's collated {@code Sort(fetch=N)},
     * and the oversampling must derive the shard fetch from the inner (tighter) N — not the outer
     * size cap — or every shard over-fetches {@code ceil(querySizeLimit * factor) + querySizeLimit}
     * rows. Preferring the deepest match honors the innermost limit.
     */
    private static SortAboveFinal findSortAboveFinal(RelNode node) {
        for (RelNode child : node.getInputs()) {
            SortAboveFinal found = findSortAboveFinal(child);
            if (found != null) return found;
        }
        if (node instanceof OpenSearchSort sort && !sort.getCollation().getFieldCollations().isEmpty()) {
            PathToFinal path = findFinalAgg(sort.getInput());
            if (path != null) return new SortAboveFinal(sort, path.project, path.finalAgg);
        }
        return null;
    }

    private static PathToFinal findFinalAgg(RelNode node) {
        return findFinalAgg(node, null);
    }

    private static PathToFinal findFinalAgg(RelNode node, OpenSearchProject seenProject) {
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) {
            return new PathToFinal(seenProject, agg);
        }
        if (node instanceof OpenSearchProject proj && seenProject == null) {
            return findFinalAgg(proj.getInput(), proj);
        }
        if (node.getInputs().size() == 1) return findFinalAgg(node.getInputs().get(0), seenProject);
        return null;
    }

    private static boolean hasEngineNativeMergeMeasure(OpenSearchAggregate agg, RelCollation collation) {
        int groupCount = agg.getGroupSet().cardinality();
        for (RelFieldCollation fc : collation.getFieldCollations()) {
            int idx = fc.getFieldIndex();
            if (idx >= groupCount && AggregateFunction.isEngineNativeMerge(agg.getAggCallList().get(idx - groupCount))) {
                return true;
            }
        }
        return false;
    }

    /** Replaces oldNode with newNode in the tree (single occurrence). */
    private static RelNode replaceInTree(RelNode root, RelNode oldNode, RelNode newNode) {
        if (root == oldNode) return newNode;
        List<RelNode> children = root.getInputs();
        boolean changed = false;
        RelNode[] newChildren = new RelNode[children.size()];
        for (int i = 0; i < children.size(); i++) {
            newChildren[i] = replaceInTree(children.get(i), oldNode, newNode);
            if (newChildren[i] != children.get(i)) changed = true;
        }
        if (!changed) return root;
        return root.copy(root.getTraitSet(), List.of(newChildren));
    }

    private static double resolveOversamplingFactor(PlannerContext context) {
        return context.getOversamplingFactor();
    }

    private record PathToFinal(OpenSearchProject project, OpenSearchAggregate finalAgg) {
    }

    private record SortAboveFinal(OpenSearchSort sort, OpenSearchProject intermediateProject, OpenSearchAggregate finalAgg) {
    }
}
