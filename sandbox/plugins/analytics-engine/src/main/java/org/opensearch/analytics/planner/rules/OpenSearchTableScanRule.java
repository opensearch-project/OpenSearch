/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.IndexResolution;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link TableScan} → {@link OpenSearchTableScan}.
 * Resolves backend from index data format settings and populates
 * per-column {@link FieldStorageInfo} from IndexMetadata mappings.
 *
 * @opensearch.internal
 */
public class OpenSearchTableScanRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchTableScanRule(PlannerContext context) {
        super(operand(TableScan.class, none()), "OpenSearchTableScanRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        if (scan instanceof OpenSearchTableScan) {
            return;
        }

        String tableName = scan.getTable().getQualifiedName().getLast();

        // Resolve the table name to one or more concrete indices. {@link IndexResolution}
        // validates that all backing indices have compatible mappings (so a query plan over an
        // alias is sound) and rejects filter aliases. Concrete-index names pass through as a
        // singleton list.
        IndexResolution resolution = IndexResolution.resolve(
            tableName,
            context.getClusterState(),
            context.getIndexNameExpressionResolver()
        );
        // Field storage is the union across all backing concrete indices. An index pattern or
        // alias can resolve to indices with differing field sets (e.g. test* where one index has
        // `age` and another has `alias`); the scan's row type is the union of all of them, so a
        // single index would spuriously fail to resolve fields it happens to omit. IndexResolution
        // has already verified fields shared by multiple indices agree on type.
        CapabilityRegistry registry = context.getCapabilityRegistry();
        FieldStorageResolver fieldStorageResolver = registry.resolveFieldStorage(resolution.concreteIndices());

        // TODO : This expects the FrontEnds to attach the row type with all fields.
        // TODO : How will they attach if we perform the index resolution
        List<String> fieldNames = scan.getRowType().getFieldList().stream().map(RelDataTypeField::getName).toList();
        List<FieldStorageInfo> fieldStorage = fieldStorageResolver.resolve(fieldNames);

        // Viable backends: must be able to read ALL requested fields
        // (natively via doc values or via delegation to another backend that can read the field)
        // TODO: also check StoredFields scan capability once stored field support is implemented
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.SCAN);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.SCAN);
        List<String> viableBackends = new ArrayList<>(registry.scanCapableBackends());

        for (FieldStorageInfo field : fieldStorage) {
            if (field.isDerived()) {
                throw new IllegalStateException(
                    "TableScan encountered derived field [" + field.getFieldName() + "] — derived fields cannot appear in a scan"
                );
            }
            // Backends that can natively scan this field's doc values
            List<String> fieldBackends = registry.scanBackendsForField(field);
            // Keep candidates that can scan natively or delegate to one that can
            viableBackends.removeIf(candidate -> {
                if (fieldBackends.contains(candidate)) return false;
                return !delegationSupporters.contains(candidate) || fieldBackends.stream().noneMatch(delegationAcceptors::contains);
            });
        }

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend can scan all requested fields on table [" + tableName + "]");
        }

        long rowCount = context.getTableRowCounts().applyAsLong(tableName);
        RelOptTable indexNameTable = new IndexNameTable(scan.getTable(), tableName, rowCount);

        call.transformTo(
            OpenSearchTableScan.create(
                scan.getCluster(),
                indexNameTable,
                viableBackends,
                fieldStorage,
                resolution.totalShardCount(),
                context.getDistributionTraitDef()
            )
        );
    }

    /**
     * Wraps a {@link RelOptTable} with just the bare index name as the qualified name.
     * Isthmus reads {@code getQualifiedName()} when creating {@code NamedScan} — this ensures
     * the Substrait plan contains only the index name, not the Calcite catalog prefix.
     *
     * <p>Carries an optional row count: when the planner context was wired with a real
     * statistics source (e.g. IndicesStats), the count overrides Calcite's default 100.0,
     * which lets cost-aware exchange operators distinguish small-input from large-input
     * plans during CBO. {@link PlannerContext#UNKNOWN_ROW_COUNT} (-1) means "no stats" and
     * we let the abstract base return its default.
     *
     * <p>TODO: Move table name stripping to the SQL/PPL plugin before dispatching the RelNode
     * to the analytics engine, so the scan rule always receives bare index names.
     */
    private static class IndexNameTable extends RelOptAbstractTable {
        private final long rowCount;

        IndexNameTable(RelOptTable delegate, String indexName, long rowCount) {
            super(delegate.getRelOptSchema(), indexName, delegate.getRowType());
            this.rowCount = rowCount;
        }

        @Override
        public double getRowCount() {
            if (rowCount == PlannerContext.UNKNOWN_ROW_COUNT) {
                return super.getRowCount();
            }
            return rowCount;
        }
    }
}
