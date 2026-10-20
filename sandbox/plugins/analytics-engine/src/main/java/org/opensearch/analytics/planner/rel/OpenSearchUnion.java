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
     * Cost gate. Locality of the union must match its arms:
     * <ul>
     *   <li>{@code COORDINATOR+SINGLETON} union → every arm must be {@code COORDINATOR+SINGLETON}.
     *       OpenSearchUnionSplitRule's general path inserts ERs to satisfy this.</li>
     *   <li>{@code SHARD+SINGLETON} union (co-location fast path) → every arm must be
     *       {@code SHARD+SINGLETON} with the union's {@code tableId} and {@code shardCount=1}.</li>
     * </ul>
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        OpenSearchDistribution selfDist = distributionOf(this);
        if (selfDist == null || selfDist.getType() != RelDistribution.Type.SINGLETON) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        for (RelNode input : getInputs()) {
            OpenSearchDistribution inputDist = distributionOf(input);
            if (inputDist == null) continue;
            if (inputDist.getType() == RelDistribution.Type.ANY) continue;
            if (inputDist.getType() != RelDistribution.Type.SINGLETON) {
                return planner.getCostFactory().makeInfiniteCost();
            }
            if (selfDist.getLocality() != inputDist.getLocality()) {
                return planner.getCostFactory().makeInfiniteCost();
            }
            if (selfDist.getLocality() == OpenSearchDistribution.Locality.SHARD) {
                if (selfDist.getTableId() == null || !selfDist.getTableId().equals(inputDist.getTableId())) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
                if (!Integer.valueOf(1).equals(inputDist.getShardCount())) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
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
