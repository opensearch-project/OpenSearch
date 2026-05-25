/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom TableScan carrying viable backend list and per-field storage metadata.
 *
 * @opensearch.internal
 */
public class OpenSearchTableScan extends TableScan implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final List<FieldStorageInfo> outputFieldStorage;
    /**
     * Non-null only when QTF (or a future rule) needs a rowType different from
     * {@code getTable().getRowType()} — e.g. fetch cols dropped, {@code ___row_id}
     * appended. Null in the default case so {@link TableScan#deriveRowType()} drives.
     */
    private final RelDataType overrideRowType;

    public OpenSearchTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage
    ) {
        this(cluster, traitSet, table, viableBackends, outputFieldStorage, null);
    }

    /**
     * Overload taking an explicit {@code overrideRowType}. Used by the QTF rule to
     * narrow the scan to {@code [sort/filter cols, ___row_id]} after dropping the fetch
     * list. {@code outputFieldStorage} must align 1:1 with {@code overrideRowType}'s
     * fields (helper columns get synthetic {@link FieldStorageInfo} entries).
     */
    public OpenSearchTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        RelDataType overrideRowType
    ) {
        super(cluster, traitSet, List.of(), table);
        this.viableBackends = viableBackends;
        this.outputFieldStorage = outputFieldStorage;
        this.overrideRowType = overrideRowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return overrideRowType != null ? overrideRowType : super.deriveRowType();
    }

    /**
     * Creates an OpenSearchTableScan with {@code SHARD+SINGLETON} (1 shard) or
     * {@code SHARD+RANDOM} (N shards). Exchange insertion is CBO-driven: downstream cost
     * gates (root, Sort with collation, RexOver Project, Join, Union) demand
     * {@code COORDINATOR+SINGLETON}; Volcano materializes an ER via
     * {@link OpenSearchDistributionTraitDef#convert} wherever a demand can't be satisfied.
     *
     * <p>Join and Union split rules check the SHARD+SINGLETON+shardCount=1+matching-tableId
     * predicate to keep execution local when all inputs co-locate on one node.
     *
     * <p>{@code tableId} is derived from the table's qualified name, stable across plans for
     * the same index.
     */
    public static OpenSearchTableScan create(
        RelOptCluster cluster,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        int shardCount,
        OpenSearchDistributionTraitDef distTraitDef
    ) {
        int tableId = table.getQualifiedName().hashCode();
        OpenSearchDistribution distribution = shardCount == 1
            ? distTraitDef.shardSingleton(tableId, shardCount)
            : distTraitDef.shardRandom(tableId, shardCount);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(distribution);
        return new OpenSearchTableScan(cluster, traitSet, table, viableBackends, outputFieldStorage);
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        return outputFieldStorage;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchTableScan(getCluster(), traitSet, getTable(), viableBackends, outputFieldStorage, overrideRowType);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchTableScan(getCluster(), getTraitSet(), getTable(), List.of(backend), outputFieldStorage, overrideRowType);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // When QTF (or another rule) narrows our rowType via overrideRowType, the original
        // RelOptTable still reports the wide schema — and isthmus emits Substrait based on
        // the table's rowType, not the rel's. Copy the RelOptTable with the narrowed rowType
        // so the stripped scan carries the correct schema all the way to FragmentConvertor.
        RelOptTable table = overrideRowType != null && getTable() instanceof RelOptTableImpl impl
            ? impl.copy(overrideRowType)
            : getTable();
        return LogicalTableScan.create(getCluster(), table, List.of());
    }
}
