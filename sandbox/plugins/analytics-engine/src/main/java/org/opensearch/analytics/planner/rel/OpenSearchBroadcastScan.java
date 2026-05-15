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
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * Leaf placeholder representing a broadcast-build input on the probe side of an M1 broadcast
 * join. Analogous to {@link OpenSearchStageInputScan} but for the broadcast-injection path:
 * rather than signalling where a child-stage's partition stream feeds in, it signals where the
 * probe-side {@code BroadcastInjectionHandler} has registered a {@code MemTable} that should
 * satisfy this scan at execution time.
 *
 * <p>The build stage id determines the name used on the wire and in the Substrait plan —
 * {@code "broadcast-&lt;buildStageId&gt;"}. Both the coordinator (when constructing the
 * {@link org.opensearch.analytics.spi.BroadcastInjectionInstructionNode}) and the data node
 * (when registering the memtable) agree on the string.
 *
 * @opensearch.internal
 */
public class OpenSearchBroadcastScan extends AbstractRelNode implements OpenSearchRelNode {

    private final int buildStageId;
    private final List<String> viableBackends;

    public OpenSearchBroadcastScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        int buildStageId,
        RelDataType rowType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet);
        this.buildStageId = buildStageId;
        this.viableBackends = viableBackends;
        this.rowType = rowType;
    }

    public int getBuildStageId() {
        return buildStageId;
    }

    /** Canonical name used by both sides of the wire — matches what the handler registers. */
    public String getNamedInputId() {
        return namedInputIdFor(buildStageId);
    }

    public static String namedInputIdFor(int buildStageId) {
        return "broadcast-" + buildStageId;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        return List.of();
    }

    @Override
    public OpenSearchBroadcastScan copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchBroadcastScan(getCluster(), traitSet, buildStageId, rowType, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.item("buildStageId", buildStageId).item("namedInputId", getNamedInputId()).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchBroadcastScan(getCluster(), getTraitSet(), buildStageId, rowType, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return this; // Leaf placeholder — no annotations, no children to strip.
    }
}
