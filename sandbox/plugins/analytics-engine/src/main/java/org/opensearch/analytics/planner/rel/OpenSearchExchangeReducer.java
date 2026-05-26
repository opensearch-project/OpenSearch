/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.ExchangeInfo;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Coordinator-side reducer for exchanges. Receives streaming Arrow batches from
 * upstream stages via Analytics Core transport. Carries an {@link ExchangeInfo}
 * describing the distribution (defaults to SINGLETON; HASH/RANGE not wired yet).
 * {@code DAGBuilder} reads the ExchangeInfo directly off the reducer when cutting.
 *
 * @opensearch.internal
 */
public class OpenSearchExchangeReducer extends ConverterImpl implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final ExchangeInfo exchangeInfo;

    /** Convenience constructor — defaults to {@link ExchangeInfo#singleton()}. */
    public OpenSearchExchangeReducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<String> viableBackends) {
        this(cluster, traitSet, input, viableBackends, ExchangeInfo.singleton());
    }

    public OpenSearchExchangeReducer(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> viableBackends,
        ExchangeInfo exchangeInfo
    ) {
        // ConverterImpl makes this a Calcite-recognized trait converter — inserted by
        // Volcano via OpenSearchDistributionTraitDef.convert when a downstream operator
        // demands SINGLETON input and the child delivers RANDOM.
        super(cluster, null, traitSet, input);
        this.viableBackends = viableBackends;
        this.exchangeInfo = exchangeInfo;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Distribution this reducer represents — read by DAGBuilder when cutting child stages. */
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchExchangeReducer(getCluster(), traitSet, sole(inputs), viableBackends, exchangeInfo);
    }

    /**
     * Cost = setup overhead + transport per row. The fixed overhead per ER ensures Volcano
     * prefers fewer ERs over more ERs even when the total row count shipped is identical:
     * e.g. {@code Union(SHARD) ← 1 ER above} (one ER moving 20 rows) is cheaper than
     * {@code Union(COORDINATOR) ← 2 ERs below} (two ERs moving 10 rows each, same total
     * transport but double the setup).
     */
    private static final double SETUP_COST = 10.0;

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        return planner.getCostFactory().makeCost(SETUP_COST + rows, SETUP_COST + rows, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends).item("exchange", exchangeInfo);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchExchangeReducer(getCluster(), getTraitSet(), children.getFirst(), List.of(backend), exchangeInfo);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // ExchangeReducer is an infrastructure node — strip children but keep the node itself.
        return new OpenSearchExchangeReducer(getCluster(), getTraitSet(), strippedChildren.getFirst(), viableBackends, exchangeInfo);
    }
}
