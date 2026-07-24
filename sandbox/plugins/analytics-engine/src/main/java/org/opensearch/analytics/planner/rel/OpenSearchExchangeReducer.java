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
import org.apache.calcite.rel.type.RelDataType;
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
    /**
     * Non-null only when QTF (or a future rule) declares additional coord-side columns
     * on the ER's output (e.g. {@code ___ugsi} appended at runtime by
     * {@code ShardFragmentStageExecution.responseListenerFor}). Null in the default case
     * so {@link ConverterImpl#deriveRowType()} drives.
     */
    private final RelDataType overrideRowType;

    /**
     * Field storage matching {@link #overrideRowType} 1:1, supplied by QTF alongside the override
     * so the {@code ___ugsi} column it declares has a corresponding {@link FieldStorageInfo}. Null
     * whenever {@code overrideRowType} is null; non-null only on QTF-rebuilt ERs.
     */
    private final List<FieldStorageInfo> overrideStorage;

    /** Convenience constructor — defaults to {@link ExchangeInfo#singleton()}. */
    public OpenSearchExchangeReducer(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<String> viableBackends) {
        this(cluster, traitSet, input, viableBackends, ExchangeInfo.singleton(), null);
    }

    public OpenSearchExchangeReducer(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> viableBackends,
        ExchangeInfo exchangeInfo
    ) {
        this(cluster, traitSet, input, viableBackends, exchangeInfo, null);
    }

    /**
     * Overload taking an explicit {@code overrideRowType}. Used by the QTF rule to declare
     * {@code ___ugsi} on the ER's output schema — the column is appended coord-side at
     * runtime in {@code ShardFragmentStageExecution.responseListenerFor} per task. Schema
     * declaration here lets the reduce sink's schema-validation pass.
     */
    public OpenSearchExchangeReducer(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> viableBackends,
        ExchangeInfo exchangeInfo,
        RelDataType overrideRowType
    ) {
        this(cluster, traitSet, input, viableBackends, exchangeInfo, overrideRowType, null);
    }

    /**
     * Full constructor. {@code overrideStorage}, when non-null, must align 1:1 with
     * {@code overrideRowType} and is returned verbatim by {@link #getOutputFieldStorage()}. QTF
     * uses it to pair the {@code ___ugsi} column it declares with a matching {@link FieldStorageInfo}.
     */
    public OpenSearchExchangeReducer(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<String> viableBackends,
        ExchangeInfo exchangeInfo,
        RelDataType overrideRowType,
        List<FieldStorageInfo> overrideStorage
    ) {
        // ConverterImpl makes this a Calcite-recognized trait converter — inserted by
        // Volcano via OpenSearchDistributionTraitDef.convert when a downstream operator
        // demands SINGLETON input and the child delivers RANDOM.
        super(cluster, null, traitSet, input);
        this.viableBackends = viableBackends;
        this.exchangeInfo = exchangeInfo;
        this.overrideRowType = overrideRowType;
        this.overrideStorage = overrideStorage;
    }

    @Override
    public RelDataType deriveRowType() {
        return overrideRowType != null ? overrideRowType : super.deriveRowType();
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
        // When QTF rebuilds this ER with an overrideRowType (declaring ___ugsi), it supplies the
        // matching overrideStorage so rowType and FSI stay aligned 1:1. Otherwise delegate to input.
        if (overrideStorage != null) {
            return overrideStorage;
        }
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchExchangeReducer(
            getCluster(),
            traitSet,
            sole(inputs),
            viableBackends,
            exchangeInfo,
            overrideRowType,
            overrideStorage
        );
    }

    /**
     * Cost = setup overhead + transport per row. The fixed overhead per ER ensures Volcano
     * prefers fewer ERs over more ERs even when the total row count shipped is identical:
     * e.g. {@code Union(SHARD) ← 1 ER above} (one ER moving 20 rows) is cheaper than
     * {@code Union(COORDINATOR) ← 2 ERs below} (two ERs moving 10 rows each, same total
     * transport but double the setup).
     */
    // Per-ER setup cost (TCP, gRPC handshake, schema negotiation, allocator init). Tuned to
    // 25 so the coord-centric plan's 2×ER setup outweighs the broadcast plan's 1×ER setup
    // plus build replication, which lets the cost model pick BROADCAST for the canonical
    // small-dim × large-fact LEFT/RIGHT outer cases that BroadcastJoinIT exercises. Lower
    // values made the LEFT outer path tie too closely with coord-centric on the per-ER row
    // arithmetic and Volcano picked the wrong shape; higher values would over-penalize
    // coord-centric on small queries where it's actually optimal.
    private static final double SETUP_COST = 25.0;

    /** Per-gathered-column weight, added (not row-multiplied) to the gather cost. Purely a
     *  TIE-BREAKER for equal-row decisions — chiefly project placement below the ER
     *  ({@link org.opensearch.analytics.planner.rules.OpenSearchAggLiteralArgProjectSplitRule}),
     *  where narrow-vs-wide projects carry identical row counts so only column count differs. Kept
     *  well below 1 row so it can NEVER flip a row-dominated decision (e.g. broadcast-vs-coord-centric,
     *  which differ by tens of rows) — that race must be decided on rows. */
    private static final double WIDTH_COST = 0.1;

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = mq.getRowCount(getInput());
        // Width term is ADDITIVE (per-column overhead), NOT rows × width. Two competing decisions key
        // off this ER cost and pull in opposite directions:
        // 1. Project placement (OpenSearchAggLiteralArgProjectSplitRule): the narrow vs wide project
        // below the ER carry the SAME row count (a Project is 1:1), so only a width term lets CBO
        // prefer gathering fewer columns — pushing the narrow project below the ER.
        // 2. Broadcast vs coordinator-centric (BroadcastJoinIT): coord-centric pays 2× ER, broadcast
        // pays 1× ER + a width-agnostic broadcast exchange. A rows × width ER term (what an
        // upstream merge introduced) inflated the 2×ER plan and made CBO stop picking BROADCAST
        // for modest size asymmetries (dim=5 × fact=30) — the regression.
        // ADDITIVE width satisfies both: it breaks the equal-row project-placement tie, but stays a
        // small constant when row counts differ, so it can't flip the row-dominated join-strategy
        // race. WIDTH_COST weights each gathered column. Keep broadcast/shuffle width-agnostic — the
        // join-strategy race must be decided on rows, not width.
        double widthCost = WIDTH_COST * getRowType().getFieldCount();
        return planner.getCostFactory().makeCost(SETUP_COST + rows + widthCost, SETUP_COST + rows + widthCost, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends).item("exchange", exchangeInfo);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchExchangeReducer(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            List.of(backend),
            exchangeInfo,
            overrideRowType,
            overrideStorage
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // ExchangeReducer is an infrastructure node — strip children but keep the node itself.
        return new OpenSearchExchangeReducer(
            getCluster(),
            getTraitSet(),
            strippedChildren.getFirst(),
            viableBackends,
            exchangeInfo,
            overrideRowType,
            overrideStorage
        );
    }
}
