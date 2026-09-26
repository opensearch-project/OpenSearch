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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * OpenSearch custom Union carrying viable backend list.
 *
 * <p>Per-column output storage is the intersection of inputs' storage at the same
 * positional index — when all inputs report identical storage we keep it; any
 * divergence (e.g. one branch has a derived literal column, another has a real
 * field reference) collapses to a derived column. Downstream rules that push down
 * to physical storage (Filter, Aggregate) therefore treat post-Union columns as
 * derived unless every branch agrees.
 *
 * @opensearch.internal
 */
public class OpenSearchUnion extends Union implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchUnion(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all, List<String> viableBackends) {
        super(cluster, traitSet, List.of(), inputs, all);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<List<FieldStorageInfo>> perInputStorage = new ArrayList<>(getInputs().size());
        for (RelNode input : getInputs()) {
            RelNode unwrapped = RelNodeUtils.unwrapHep(input);
            if (!(unwrapped instanceof OpenSearchRelNode openSearchInput)) {
                throw new IllegalStateException("Union input is not OpenSearchRelNode: " + unwrapped.getClass().getSimpleName());
            }
            perInputStorage.add(openSearchInput.getOutputFieldStorage());
        }

        int columnCount = getRowType().getFieldCount();
        List<FieldStorageInfo> result = new ArrayList<>(columnCount);
        for (int col = 0; col < columnCount; col++) {
            String fieldName = getRowType().getFieldList().get(col).getName();
            SqlTypeName sqlType = getRowType().getFieldList().get(col).getType().getSqlTypeName();

            FieldStorageInfo first = perInputStorage.getFirst().size() > col ? perInputStorage.getFirst().get(col) : null;
            boolean allMatch = first != null && !first.isDerived();
            if (allMatch) {
                for (int i = 1; i < perInputStorage.size(); i++) {
                    List<FieldStorageInfo> branch = perInputStorage.get(i);
                    if (branch.size() <= col) {
                        allMatch = false;
                        break;
                    }
                    FieldStorageInfo other = branch.get(col);
                    if (other.isDerived()
                        || other.getFieldType() != first.getFieldType()
                        || !other.getDocValueFormats().equals(first.getDocValueFormats())
                        || !other.getIndexFormats().equals(first.getIndexFormats())) {
                        allMatch = false;
                        break;
                    }
                }
            }

            result.add(allMatch ? first : FieldStorageInfo.derivedColumn(fieldName, sqlType));
        }
        return result;
    }

    @Override
    public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new OpenSearchUnion(getCluster(), traitSet, inputs, all, viableBackends);
    }

    /**
     * A Union costs nothing of its own — it concatenates. The one real cost is the invariant every operator
     * shares: an arm whose placement is still UNRESOLVED has no defined cost, because nothing above it has a
     * defined location or a defined correctness. The marking phase deliberately registers such a seed (see
     * {@code OpenSearchUnionRule}), and this is what confines it to the ANY subset so only the concrete
     * alternatives {@link #passThroughTraits} and {@code OpenSearchUnionSplitRule} produce are consumable.
     *
     * <p>The placement REQUIREMENT — every arm must be gathered to the SAME place the Union runs — is
     * asserted, not priced. See {@link #assertPlacementIsLegal}.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (hasUnresolvedInput()) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        OpenSearchDistribution selfDist = distributionOf(this);
        if (selfDist == null) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        // The marking phase's UNRESOLVED seed: it claims no placement, so there is nothing to assert, and it
        // must not be consumable — which infinite cost secures.
        if (selfDist.getType() == RelDistribution.Type.ANY) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        assert assertPlacementIsLegal(selfDist);
        return planner.getCostFactory().makeTinyCost();
    }

    /**
     * The placements a Union can correctly concatenate at: itself a SINGLETON, and every arm a SINGLETON at
     * the SAME locality — plus, for the co-located {@code SHARD} shape, the same {@code tableId} and
     * {@code shardCount=1}, since only then do all arms genuinely live on one node.
     *
     * <p>ASSERTED, not priced — these were six {@code makeInfiniteCost()} branches, i.e. legality expressed
     * through the channel that exists for ranking. Unreachable because both producers set self and arm traits
     * together: {@link #passThroughTraits} demands the gathered shape of every arm, and
     * {@code OpenSearchUnionSplitRule} converts every arm to whichever shape it registers.
     * {@link #getDeriveMode} is {@code PROHIBITED}, which closes the derive direction rather than leaving it
     * for cost to cover.
     *
     * <p>Asserted rather than deleted because a violation is a silently WRONG RESULT: a Union whose arms sit
     * at different localities concatenates whatever happens to arrive at each place, so rows go missing.
     *
     * @return always {@code true}, so this reads as {@code assert assertPlacementIsLegal(...)}
     * @throws IllegalStateException when the Union meets arms it cannot correctly concatenate
     */
    private boolean assertPlacementIsLegal(OpenSearchDistribution selfDist) {
        if (selfDist.getType() != RelDistribution.Type.SINGLETON) {
            throw new IllegalStateException("Union at a non-singleton placement [" + selfDist + "] cannot concatenate its arms");
        }
        for (RelNode input : getInputs()) {
            OpenSearchDistribution inputDist = distributionOf(input);
            // No distribution trait at all is not the same as an unresolved one: it carries no placement claim
            // either way, so there is nothing to check.
            if (inputDist == null) continue;
            if (inputDist.getType() != RelDistribution.Type.SINGLETON) {
                throw new IllegalStateException("Union over a partitioned arm [" + inputDist + "]");
            }
            if (selfDist.getLocality() != inputDist.getLocality()) {
                throw new IllegalStateException("Union at [" + selfDist + "] over an arm at another locality [" + inputDist + "]");
            }
            if (selfDist.getLocality() == OpenSearchDistribution.Locality.SHARD) {
                if (selfDist.getTableId() == null || !selfDist.getTableId().equals(inputDist.getTableId())) {
                    throw new IllegalStateException("Co-located union over an arm from another table [" + inputDist + "]");
                }
                if (!Integer.valueOf(1).equals(inputDist.getShardCount())) {
                    throw new IllegalStateException("Co-located union over a multi-shard arm [" + inputDist + "]");
                }
            }
        }
        return true;
    }

    // ---- PhysicalNode (top-down trait propagation) ----

    /**
     * A Union can only deliver a SINGLETON, so it answers EVERY demand with the coordinator-gathered shape:
     * itself at {@code COORDINATOR+SINGLETON} and the same demanded of every arm, which
     * {@link OpenSearchConvention#enforce} materializes as one reducer per arm that is not already there.
     *
     * <p>Two deliberate choices here.
     *
     * <p><b>Answer rather than decline</b>, following the {@link OpenSearchSort#passThroughTraits} precedent: a
     * Union may sit under a join whose arms are asked for {@code RANDOM(SHARD)} or {@code WORKER+HASH}, and no
     * exchange can MOVE data to a scan's natural shard locality. Declining would leave the Union's subset
     * empty and the query would die with "Missing conversion is OpenSearchUnion[]" rather than merely
     * choosing a worse plan. Calcite tolerates a passThrough whose delivered traits differ from the request;
     * it costs the result and the parent re-enforces.
     *
     * <p><b>Normalise the locality to COORDINATOR instead of passing a SINGLETON demand verbatim.</b> The root
     * asks for {@code anySingleton} — SINGLETON with locality {@code null} — and passing that through would
     * put the Union at a null locality while its arms deliver a concrete one, which is exactly the
     * self/arm locality mismatch {@link #assertPlacementIsLegal} rejects. The cheaper co-located
     * {@code SHARD+SINGLETON} shape is not lost by normalising: {@code OpenSearchUnionSplitRule} registers it
     * directly when every arm is a 1-shard scan of the same table, and COORDINATOR+SINGLETON satisfies a
     * locality-agnostic singleton demand anyway.
     */
    @Override
    public org.apache.calcite.util.Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
        OpenSearchDistribution gathered = traitDef.coordSingleton();
        List<RelTraitSet> armDemands = new ArrayList<>(getInputs().size());
        for (RelNode input : getInputs()) {
            armDemands.add(input.getTraitSet().replace(gathered));
        }
        return org.apache.calcite.util.Pair.of(getTraitSet().replace(gathered), armDemands);
    }

    /**
     * No derivation. A Union's output placement is not any single arm's: claiming one arm's distribution would
     * assert a shape the other arms do not have, and Calcite's default {@code LEFT_FIRST} would do exactly
     * that — build {@code Union(RANDOM(SHARD))} from a partitioned first arm while the rest sit elsewhere.
     * The gathered shape is supplied by {@link #passThroughTraits}, and the co-located shard shape by
     * {@code OpenSearchUnionSplitRule}, so prohibiting derivation removes an illegal producer without
     * removing any legal alternative.
     */
    @Override
    public org.apache.calcite.plan.DeriveMode getDeriveMode() {
        return org.apache.calcite.plan.DeriveMode.PROHIBITED;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchUnion(getCluster(), getTraitSet(), children, all, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalUnion.create(strippedChildren, all);
    }
}
