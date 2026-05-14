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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Post-CBO rewrite rule for Query-Then-Fetch (QTF) late materialization.
 *
 * <p>Detects the pattern {@code Project -> Sort(with LIMIT) -> [Filter ->] Scan}
 * and narrows the projection to only the columns needed for sort and filter,
 * plus the {@code __row_id__} column. Columns that are only needed for the
 * final output (pureProjectColumns) are deferred to a later fetch phase.
 *
 * <p>The DAGBuilder detects {@code __row_id__} in the shard fragment's output
 * schema and wraps the DAG with a {@code LateMaterializationStageExecution}
 * that handles the fetch phase.
 *
 * <p>This rule only fires when:
 * <ul>
 *   <li>Sort has a fetch (LIMIT) — without limit, all rows are needed anyway</li>
 *   <li>There are columns in the project that are NOT used by sort/filter
 *       (pureProjectColumns is non-empty)</li>
 *   <li>The query does not already project {@code __row_id__} (no user-driven rewrite)</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class LateMaterializationRule extends RelOptRule {

    private static final Logger LOGGER = LogManager.getLogger(LateMaterializationRule.class);
    static final String ROW_ID_COLUMN = "__row_id__";

    /**
     * Pattern: Project -> Sort -> Filter -> Scan (with optional filter).
     * We use a broad match on Project -> Sort -> any, and inspect the subtree
     * in onMatch to handle the optional filter.
     */
    public LateMaterializationRule() {
        super(operand(OpenSearchProject.class, operand(OpenSearchSort.class, operand(RelNode.class, any()))), "LateMaterializationRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchProject project = call.rel(0);
        OpenSearchSort sort = call.rel(1);
        RelNode sortChild = RelNodeUtils.unwrapHep(call.rel(2));

        // Only fire when Sort has a limit (fetch != null)
        if (sort.fetch == null) {
            return;
        }

        // Walk below the sort to find the filter (optional) and scan.
        // Multi-shard plans have an ExchangeReducer between sort and filter/scan:
        // Project -> Sort -> ExchangeReducer -> [Filter ->] Scan
        // Single-shard plans have filter/scan directly:
        // Project -> Sort -> [Filter ->] Scan
        RelNode belowExchange = sortChild;
        if (sortChild instanceof OpenSearchExchangeReducer reducer) {
            belowExchange = RelNodeUtils.unwrapHep(reducer.getInput());
        }

        OpenSearchFilter filter = null;
        OpenSearchTableScan scan;

        if (belowExchange instanceof OpenSearchFilter osFilter) {
            filter = osFilter;
            RelNode filterChild = RelNodeUtils.unwrapHep(osFilter.getInput());
            if (filterChild instanceof OpenSearchTableScan osScan) {
                scan = osScan;
            } else {
                return; // Not the expected shape
            }
        } else if (belowExchange instanceof OpenSearchTableScan osScan) {
            scan = osScan;
        } else {
            return; // Not the expected shape (e.g. aggregate below sort)
        }

        // Find the __row_id__ column index in the sort's input row type.
        // Filter, ExchangeReducer, and Scan all pass through the same row type,
        // so we use the sort's row type (which equals its input's row type).
        RelDataType sortInputRowType = sort.getRowType();
        int rowIdIndex = findFieldIndex(sortInputRowType, ROW_ID_COLUMN);
        if (rowIdIndex < 0) {
            // __row_id__ not in schema — cannot do QTF
            return;
        }

        // Check if the project already outputs __row_id__
        if (projectAlreadyOutputsRowId(project)) {
            return;
        }

        // Collect column indices used by sort collation.
        // Collation field indices reference the sort's input row type.
        Set<Integer> computationIndices = new LinkedHashSet<>();

        for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
            computationIndices.add(fieldCollation.getFieldIndex());
        }

        // Collect column indices used by the filter condition (if present).
        // Filter condition indices also reference the scan's row type, which is
        // identical to the sort's input row type (Filter/ExchangeReducer pass through).
        if (filter != null) {
            collectInputRefs(filter.getCondition(), computationIndices);
        }

        // Collect column indices referenced by the project's expressions.
        // Project expressions reference the sort's output row type (= sort's input row type).
        Set<Integer> projectIndices = new LinkedHashSet<>();
        for (RexNode expr : project.getProjects()) {
            collectInputRefs(expr, projectIndices);
        }

        // pureProjectColumns = columns referenced only by project, not by sort/filter
        Set<Integer> pureProjectIndices = new LinkedHashSet<>(projectIndices);
        pureProjectIndices.removeAll(computationIndices);

        if (pureProjectIndices.isEmpty()) {
            // All project columns are needed for sort/filter — no benefit from QTF
            return;
        }

        LOGGER.info(
            "QTF rewrite: {} pure project columns can be deferred to fetch phase (sort/filter need {} columns)",
            pureProjectIndices.size(),
            computationIndices.size()
        );

        // Build the narrowed column set: sort/filter columns + __row_id__
        Set<Integer> narrowedIndices = new LinkedHashSet<>(computationIndices);
        narrowedIndices.add(rowIdIndex);

        // Build the new projection expressions: one RexInputRef per narrowed column
        // referencing the sort's row type.
        List<RexNode> newProjectExprs = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        for (int idx : narrowedIndices) {
            RelDataTypeField field = sortInputRowType.getFieldList().get(idx);
            newProjectExprs.add(new RexInputRef(idx, field.getType()));
            newFieldNames.add(field.getName());
        }

        // Build the new row type for the narrowed project
        RelDataType newRowType = project.getCluster()
            .getTypeFactory()
            .createStructType(newProjectExprs.stream().map(RexNode::getType).toList(), newFieldNames);

        // Create the new narrowed project, reusing sort as input
        OpenSearchProject narrowedProject = new OpenSearchProject(
            project.getCluster(),
            project.getTraitSet(),
            RelNodeUtils.unwrapHep(project.getInput()),
            newProjectExprs,
            newRowType,
            project.getViableBackends()
        );

        LOGGER.info("QTF rewrite applied: narrowed projection from {} to {} columns", project.getRowType().getFieldCount(), newFieldNames);

        call.transformTo(narrowedProject);
    }

    /**
     * Checks whether the project already outputs __row_id__.
     */
    private boolean projectAlreadyOutputsRowId(OpenSearchProject project) {
        return project.getRowType().getFieldNames().contains(ROW_ID_COLUMN);
    }

    /**
     * Finds the index of a field by name in the row type, or -1 if not found.
     */
    static int findFieldIndex(RelDataType rowType, String fieldName) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Collects all RexInputRef indices from the given expression tree.
     */
    private void collectInputRefs(RexNode node, Set<Integer> indices) {
        node.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                indices.add(inputRef.getIndex());
                return null;
            }
        });
    }
}
