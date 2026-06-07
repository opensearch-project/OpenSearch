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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
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
    /** Row count behind this scan; 0 ⇒ {@link #estimateRowCount} falls back to a per-shard estimate. */
    private final long rowCount;

    /** Canonical constructor. {@code overrideRowType} non-null only to narrow output columns (QTF); else null. */
    public OpenSearchTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        RelDataType overrideRowType,
        long rowCount
    ) {
        super(cluster, traitSet, List.of(), table);
        this.viableBackends = viableBackends;
        this.outputFieldStorage = outputFieldStorage;
        this.overrideRowType = overrideRowType;
        this.rowCount = rowCount;
    }

    /** Convenience for callers without an override row type or known row count. */
    public OpenSearchTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage
    ) {
        this(cluster, traitSet, table, viableBackends, outputFieldStorage, null, 0L);
    }

    /** Convenience for callers that narrow the rowType (QTF) but don't have a row-count stat. */
    public OpenSearchTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        RelDataType overrideRowType
    ) {
        this(cluster, traitSet, table, viableBackends, outputFieldStorage, overrideRowType, 0L);
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
     * the same index. {@code rowCount} is threaded through; 0 ⇒ per-shard fallback estimate.
     */
    public static OpenSearchTableScan create(
        RelOptCluster cluster,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        int shardCount,
        long rowCount,
        OpenSearchDistributionTraitDef distTraitDef
    ) {
        int tableId = table.getQualifiedName().hashCode();
        OpenSearchDistribution distribution = shardCount == 1
            ? distTraitDef.shardSingleton(tableId, shardCount)
            : distTraitDef.shardRandom(tableId, shardCount);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(distribution);
        return new OpenSearchTableScan(cluster, traitSet, table, viableBackends, outputFieldStorage, null, rowCount);
    }

    /** Convenience overload for callers without a row-count stat (defaults to 0 / fallback). */
    public static OpenSearchTableScan create(
        RelOptCluster cluster,
        RelOptTable table,
        List<String> viableBackends,
        List<FieldStorageInfo> outputFieldStorage,
        int shardCount,
        OpenSearchDistributionTraitDef distTraitDef
    ) {
        return create(cluster, table, viableBackends, outputFieldStorage, shardCount, 0L, distTraitDef);
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
        return new OpenSearchTableScan(getCluster(), traitSet, getTable(), viableBackends, outputFieldStorage, overrideRowType, rowCount);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    /** Per-shard row-count fallback when no real statistic is available — only the order of magnitude matters to the cost model. */
    private static final double DEFAULT_ROWS_PER_SHARD = 10_000_000.0;

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        if (rowCount > 0) {
            return rowCount;
        }
        Integer shardCount = null;
        for (int i = 0; i < getTraitSet().size(); i++) {
            if (getTraitSet().getTrait(i) instanceof OpenSearchDistribution dist && dist.getShardCount() != null) {
                shardCount = dist.getShardCount();
                break;
            }
        }
        int shards = shardCount != null ? shardCount : 1;
        return shards * DEFAULT_ROWS_PER_SHARD;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchTableScan(
            getCluster(),
            getTraitSet(),
            getTable(),
            List.of(backend),
            outputFieldStorage,
            overrideRowType,
            rowCount
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // OpenSearchTableScan carries no operator annotations to strip and already exposes the
        // correct schema via deriveRowType() (which honours overrideRowType when QTF narrows it).
        // Returning this directly keeps the override visible to isthmus's Substrait conversion;
        // converting to LogicalTableScan would defer to the underlying RelOptTable's wide rowType
        // and silently drop helper columns like __row_id__.
        return this;
    }
}
