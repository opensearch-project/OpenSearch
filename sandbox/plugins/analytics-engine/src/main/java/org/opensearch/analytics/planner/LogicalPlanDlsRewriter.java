/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ReadAccessPolicy;
import org.opensearch.analytics.query.QueryBuilderTranslationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Adds effective document-level restrictions immediately above protected table scans. */
public final class LogicalPlanDlsRewriter {

    private final QueryBuilderTranslationService translationService;

    public LogicalPlanDlsRewriter(QueryBuilderTranslationService translationService) {
        this.translationService = translationService;
    }

    /**
     * Rewrites every scan protected by {@code policy} and verifies complete policy coverage.
     *
     * <p>Each table expression is first replaced by the concrete indices authorized for that scan.
     * Indices sharing one policy group stay in one scan, avoiding unnecessary unions. For example:
     *
     * <pre>{@code
     * TableScan("logs-*")
     * group [logs-2025, logs-2026] restricted by tenant=blue
     *
     * becomes
     *
     * Filter(tenant=blue, TableScan("logs-2025,logs-2026"))
     * }</pre>
     *
     * <p>Different policy groups cannot share one filter safely, so the scan becomes a UNION ALL of
     * independently protected branches. An unrestricted group gets no filter. For example:
     *
     * <pre>{@code
     * TableScan("logs-*")
     * group [logs-blue-1, logs-blue-2] restricted by tenant=blue
     * group [logs-green] restricted by tenant=green
     * group [logs-public] unrestricted
     *
     * becomes
     *
     * UnionAll(
     *   Filter(tenant=blue, TableScan("logs-blue-1,logs-blue-2")),
     *   Filter(tenant=green, TableScan("logs-green")),
     *   TableScan("logs-public")
     * )
     * }</pre>
     */
    public RelNode rewrite(RelNode logicalPlan, ReadAccessPolicy policy, Map<String, List<String>> concreteIndicesByTable)
        throws OpenSearchException {
        if (policy.hasRestrictions() == false) {
            return logicalPlan;
        }

        Set<String> associatedPolicyIndices = new HashSet<>();
        class DlsPlanShuttle extends RelHomogeneousShuttle {
            @Override
            public RelNode visit(RelNode node) {
                RelNode rewrittenNode = super.visit(node);
                // Calcite stores a subquery's relational plan in RexSubQuery rather than in
                // RelNode.getInputs(). Rewrite it before PlannerImpl lowers the subquery to joins.
                return rewrittenNode.accept(new RexShuttle() {
                    @Override
                    public RexNode visitSubQuery(RexSubQuery subQuery) {
                        RexSubQuery rewrittenSubQuery = (RexSubQuery) super.visitSubQuery(subQuery);
                        RelNode rewrittenSubQueryPlan = rewrittenSubQuery.rel.accept(DlsPlanShuttle.this);
                        return rewrittenSubQueryPlan == rewrittenSubQuery.rel
                            ? rewrittenSubQuery
                            : rewrittenSubQuery.clone(rewrittenSubQueryPlan);
                    }
                });
            }

            @Override
            public RelNode visit(TableScan scan) {
                String tableName = scan.getTable().getQualifiedName().getLast();
                List<String> concreteIndices = concreteIndicesByTable.get(tableName);
                if (concreteIndices == null || concreteIndices.isEmpty()) {
                    throw new OpenSearchException("No authorized concrete indices were resolved for table [" + tableName + "]");
                }

                List<RelNode> branches = new ArrayList<>();
                Set<String> indicesRewrittenForScan = new HashSet<>();
                for (ReadAccessPolicy.IndexGroup group : policy.indexGroups()) {
                    List<String> branchIndices = intersection(concreteIndices, group.concreteIndices());
                    if (branchIndices.isEmpty()) {
                        continue;
                    }

                    TableScan concreteScan = replaceTableName(scan, String.join(",", branchIndices));
                    RelNode branch = concreteScan;
                    if (group.restrictions().isPresent()) {
                        RexNode predicate = translationService.translate(
                            group.restrictions().orElseThrow(),
                            concreteScan.getCluster(),
                            concreteScan.getTable()
                        );
                        branch = LogicalFilter.create(concreteScan, predicate);
                    }
                    branches.add(branch);
                    indicesRewrittenForScan.addAll(branchIndices);
                    associatedPolicyIndices.addAll(branchIndices);
                }

                if (indicesRewrittenForScan.containsAll(concreteIndices) == false) {
                    Set<String> missing = new HashSet<>(concreteIndices);
                    missing.removeAll(indicesRewrittenForScan);
                    throw new OpenSearchException(
                        "Read-access policy does not cover concrete indices " + missing + " for table [" + tableName + "]"
                    );
                }
                if (branches.size() == 1) {
                    return branches.getFirst();
                }
                return LogicalUnion.create(branches, true);
            }
        }
        RelNode rewritten = logicalPlan.accept(new DlsPlanShuttle());

        if (associatedPolicyIndices.containsAll(policy.coveredConcreteIndices()) == false) {
            Set<String> missing = new HashSet<>(policy.coveredConcreteIndices());
            missing.removeAll(associatedPolicyIndices);
            throw new OpenSearchException("DLS policy indices could not be associated with table scans: " + missing);
        }
        return rewritten;
    }

    private static List<String> intersection(List<String> concreteIndices, Collection<String> groupIndices) {
        return concreteIndices.stream().filter(groupIndices::contains).toList();
    }

    private static TableScan replaceTableName(TableScan scan, String concreteIndexExpression) {
        RelOptTable table = new ConcreteIndicesTable(scan.getTable(), concreteIndexExpression);
        return new LogicalTableScan(scan.getCluster(), scan.getTraitSet(), scan.getHints(), table);
    }

    private static final class ConcreteIndicesTable extends RelOptAbstractTable {
        private final RelOptTable delegate;

        private ConcreteIndicesTable(RelOptTable delegate, String concreteIndexExpression) {
            super(delegate.getRelOptSchema(), concreteIndexExpression, delegate.getRowType());
            this.delegate = delegate;
        }

        @Override
        public double getRowCount() {
            return delegate.getRowCount();
        }
    }
}
