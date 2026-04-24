/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner.rel;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.sql.calcite.plan.Scannable;

import java.util.List;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Boundary node that absorbs supported logical operators into a single scan.
 *
 * <p>Extends {@link TableScan} (NOT {@code LogicalTableScan}) so that
 * {@code UnifiedQueryCompiler}'s inner RelShuttle — which only matches
 * {@code LogicalTableScan} — skips this node. Implements {@link EnumerableRel}
 * so Calcite's Janino code-generation path calls {@link #execute()} at
 * execution time via the stash pattern.
 *
 * <p>The {@code logicalFragment} field holds the absorbed logical subtree
 * (e.g., {@code LogicalFilter → LogicalTableScan}). At execution time,
 * {@code execute()} passes the fragment to the {@link QueryPlanExecutor}, which
 * returns the result rows.
 */
public class OpenSearchBoundaryTableScan extends TableScan implements EnumerableRel, Scannable {

    private final RelNode logicalFragment;
    @SuppressWarnings("rawtypes")
    private final QueryPlanExecutor planExecutor;

    @SuppressWarnings("rawtypes")
    public OpenSearchBoundaryTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        RelNode logicalFragment,
        QueryPlanExecutor planExecutor
    ) {
        super(cluster, traitSet, List.of(), table);
        this.logicalFragment = logicalFragment;
        this.planExecutor = planExecutor;
    }

    /** Returns the absorbed logical subtree passed to the engine at execution time. */
    public RelNode getLogicalFragment() {
        return logicalFragment;
    }

    /**
     * Derives the row type from the logical fragment rather than the table.
     * This ensures that after absorbing operators like aggregate or project,
     * the boundary node's row type matches the absorbed operator's output type.
     */
    @Override
    public RelDataType deriveRowType() {
        return logicalFragment.getRowType();
    }

    /** Returns the engine executor used for execution. */
    @SuppressWarnings("rawtypes")
    public QueryPlanExecutor getEngineExecutor() {
        return planExecutor;
    }

    /**
     * Implements the EnumerableRel interface using the stash pattern.
     * Generated Janino code calls {@link #execute()} on the stashed reference.
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

        Expression stashedRef = implementor.stash(this, OpenSearchBoundaryTableScan.class);
        return implementor.result(physType, Blocks.toBlock(Expressions.call(stashedRef, "execute")));
    }

    /**
     * Implements {@link Scannable#scan()} so that {@code OpenSearchCalcitePreparingStmt}
     * can bypass Janino code generation entirely when this node is the plan root.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Enumerable<@Nullable Object> scan() {
        return (Enumerable<@Nullable Object>) (Enumerable<?>) bind(null);
    }

    /**
     * Called by generated Janino code at execution time (fallback when this
     * node is NOT the plan root and codegen is used for the parent operators).
     * Delegates to {@link #bind(DataContext)} with a null DataContext.
     *
     * @return result rows as an Enumerable
     */
    public Enumerable<Object[]> execute() {
        return bind(null);
    }

    /**
     * Executes the logical fragment via the {@link QueryPlanExecutor}.
     *
     * @param dataContext the Calcite data context (may be null)
     * @return result rows as an Enumerable
     */
    @SuppressWarnings("unchecked")
    public Enumerable<Object[]> bind(DataContext dataContext) {
        try {
            Iterable<Object[]> result = (Iterable<Object[]>) planExecutor.execute(logicalFragment, dataContext);
            return Linq4j.asEnumerable(result);
        } catch (Exception e) {
            throw new RuntimeException(
                "Engine execution failed for table ["
                    + getTable().getQualifiedName()
                    + "] with logical fragment: "
                    + logicalFragment.explain(),
                e
            );
        }
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchBoundaryTableScan(getCluster(), traitSet, getTable(), logicalFragment, planExecutor);
    }

    @Override
    public org.apache.calcite.rel.RelWriter explainTerms(org.apache.calcite.rel.RelWriter pw) {
        return super.explainTerms(pw).item("fragment", logicalFragment.explain());
    }
}
