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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchTableScanRule.class);

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

        // Two-phase field coverage check:
        // 1. Value-producing backends (DocValues / StoredFields) must cover EVERY field —
        // downstream ops can need any column's actual value, so a value-driver must be
        // able to deliver all of them. Original strict invariant.
        // 2. Metadata-only drivers (today: only Lucene via inverted index) stay viable if
        // they cover SOME field. Downstream ops that need a column the metadata driver
        // can't reach (e.g. Project on a numeric field) self-restrict and PlanForker's
        // chain-agreement filter drops the driver from the surviving alternatives. The
        // only chain that makes it through end-to-end is the count fast-path shape:
        // count(*) / count(col) over filters touching only Lucene-indexable fields.
        //
        // Without the split, a single non-keyword field in the scan's row type (e.g.
        // `amount`) would disqualify Lucene from every query against the index, even
        // queries that never reference it.
        //
        // TODO: today {@code "lucene"} is the only metadata-only driver, identified by
        // membership in the per-field {@code FieldStorageInfo.getIndexFormats()}. When a
        // second metadata-only backend (e.g. Tantivy) lands — or worse, a backend that
        // declares both Index AND DocValues — replace this hardcoded id with a
        // first-class identifier on {@code BackendCapabilityProvider} (e.g. a "metadata
        // driver" marker) so the planner can tell them apart from value-producing peers
        // that happen to also have an inverted index. See
        // CapabilityRegistry.metadataOnlyScanBackends history for the prior precomputed
        // set; collapsed for now to keep the registry surface small.
        final String metadataOnlyDriver = "lucene";
        // When the cluster setting analytics.planner.prefer_metadata_driver is off, skip the
        // permissive metadata-only gate entirely — the metadata driver runs the strict
        // value-producing check like any other backend, and (since Lucene declares no
        // value-producing scan today) gets dropped at the scan level. No alternatives, no
        // post-fork pruning needed downstream.
        final boolean admitMetadataDriver = context.preferMetadataDriver();
        boolean metadataOnlyCoversAny = false;
        for (FieldStorageInfo field : fieldStorage) {
            if (field.isDerived()) {
                throw new IllegalStateException(
                    "TableScan encountered derived field [" + field.getFieldName() + "] — derived fields cannot appear in a scan"
                );
            }
            List<String> dvBackends = registry.scanBackendsForField(field);
            // Index-scan viability must respect the backend's declared supported field types, not
            // just the field's indexFormats. A keyword field with indexFormats=[lucene] satisfies
            // Lucene's Index(supportedFieldTypes={KEYWORD, TEXT, MATCH_ONLY_TEXT}) cap;
            // a numeric field with the same indexFormats does not — even though its values are
            // physically in Lucene, no backend declares an Index scan over numerics today.
            List<String> idxBackends = registry.indexScanBackendsForField(field);
            boolean idxCoversMetadataDriver = idxBackends.contains(metadataOnlyDriver);
            if (idxCoversMetadataDriver) {
                metadataOnlyCoversAny = true;
            }
            LOGGER.debug(
                "[table-scan] field={} type={} indexFormats={} docValueFormats={} dvBackends={} idxBackends={} idxCoversMetadata={}",
                field.getFieldName(),
                field.getFieldType(),
                field.getIndexFormats(),
                field.getDocValueFormats(),
                dvBackends,
                idxBackends,
                idxCoversMetadataDriver
            );
            // Strict: every value-producing candidate must cover this field (or delegate to one
            // that does). When admitMetadataDriver=false the metadata driver is held to the same
            // strict rule; when true it's exempt here and the permissive check below decides.
            viableBackends.removeIf(candidate -> {
                if (admitMetadataDriver && candidate.equals(metadataOnlyDriver)) return false; // metadata-only handled below
                if (dvBackends.contains(candidate)) return false;
                return !delegationSupporters.contains(candidate) || dvBackends.stream().noneMatch(delegationAcceptors::contains);
            });
        }
        // Permissive: keep the metadata-only driver viable iff it covers at least one field —
        // only consulted when the setting allows it.
        if (admitMetadataDriver == false || metadataOnlyCoversAny == false) {
            viableBackends.remove(metadataOnlyDriver);
        }
        LOGGER.debug("[table-scan] viableBackends={}", viableBackends);

        if (viableBackends.isEmpty()) {
            throw new IllegalArgumentException("No backend can scan all requested fields on table [" + tableName + "]");
        }

        RelOptTable indexNameTable = new IndexNameTable(scan.getTable(), tableName);

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
     * <p>TODO: Move table name stripping to the SQL/PPL plugin before dispatching the RelNode
     * to the analytics engine, so the scan rule always receives bare index names.
     */
    private static class IndexNameTable extends RelOptAbstractTable {
        IndexNameTable(RelOptTable delegate, String indexName) {
            super(delegate.getRelOptSchema(), indexName, delegate.getRowType());
        }
    }
}
