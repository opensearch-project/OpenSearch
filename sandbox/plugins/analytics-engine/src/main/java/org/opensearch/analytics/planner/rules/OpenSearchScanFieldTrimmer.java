/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.List;
import java.util.Set;

/**
 * Required-field propagation pass for analytics-engine plans.
 *
 * <p>Walks the plan top-down via Calcite's {@link RelFieldTrimmer} dispatch (Project,
 * Filter, Aggregate, Sort, Join, SetOp, etc.) and at each {@link TableScan} narrows the
 * scan's row type to only the columns the plan actually requires. The narrowed scan is
 * then visible to {@link OpenSearchTableScanRule}, whose viability check is computed
 * over the scan's exposed columns — so columns the query never reads (e.g. an unread
 * {@code geo_point} on the same index) cannot reject viability for the index.
 *
 * <p>Why subclass {@link RelFieldTrimmer} instead of writing a tactical
 * {@code Project(TableScan)} rule:
 * <ul>
 *   <li>The trimmer covers Project-over-Filter, Aggregate, Sort, Join, SetOp, and
 *       Project-over-RexOver shapes uniformly. A pattern-based rule would need a case
 *       per parent operator.</li>
 *   <li>Calcite's {@link org.apache.calcite.plan.RelOptUtil.InputFinder} is the
 *       canonical input-ref collector — it walks {@code RexCall}, {@code RexOver},
 *       {@code RexCorrelVariable}, and other rex shapes correctly. Reusing it avoids
 *       a hand-rolled recursion that drifts out of sync with new rex node types.</li>
 *   <li>The {@link TrimResult} contract guarantees a {@code SourceMapping} that lets
 *       the parent re-target its references to the narrowed child without manual
 *       index bookkeeping.</li>
 * </ul>
 *
 * <p>Window note: Calcite 1.41 has no {@code trimFields(Window, ...)} overload, so a
 * top-level {@code Window} (a {@code LogicalWindow}, distinct from a {@code RexOver}
 * inside a {@code Project}) falls into the generic-{@code RelNode} path that asks all
 * its children for every field. The analytics-engine planner currently lowers window
 * functions as {@code RexOver} inside {@code LogicalProject} (the PPL eventstats /
 * appendcol shape), which {@code Project}'s {@code InputFinder} traverses correctly,
 * so this limitation does not affect the planner's existing window paths.
 *
 * @opensearch.internal
 */
public class OpenSearchScanFieldTrimmer extends RelFieldTrimmer {

    private final PlannerContext context;

    public OpenSearchScanFieldTrimmer(PlannerContext context, RelBuilder relBuilder) {
        super(null, relBuilder);
        this.context = context;
    }

    // ---- Project scope guard ----
    //
    // Calcite's default {@code trimFields(Project, ...)} drops projection expressions
    // whose output is unread by the consumer. That is correct for general query-rewrite
    // passes, but wrong here: a windowed expression (RexOver) on a {@code LogicalProject}
    // is the analytics-engine's lowering for PPL eventstats / appendcol, and downstream
    // {@link OpenSearchProjectRule} relies on the windowed Project surviving the trim
    // pass — a stats query whose windowed column is consumed by an outer aggregate would
    // otherwise have its windowed Project deleted by Calcite's standard DCE.
    //
    // Forcing {@code fieldsUsed} to the full output range tells Calcite's standard
    // Project trim to keep every projection (line 525-527 short-circuits when input is
    // unchanged; otherwise the rebuild path at lines 559-565 visits every projection
    // because {@code fieldsUsed.get(ord.i)} is true for all i). The InputFinder still
    // collects referenced inputs across all projections — so a TableScan beneath gets
    // narrowed correctly. We do NOT override {@code trimFields(Aggregate|Join|SetOp,...)}:
    // those operators' default trim is what produces the column narrowing we want for
    // the scan, and their effect on intermediate column indices is benign.
    @Override
    public TrimResult trimFields(Project project, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        return super.trimFields(project, ImmutableBitSet.range(project.getRowType().getFieldCount()), extraFields);
    }

    /**
     * Overrides Calcite's default {@code trimFields(TableScan, ...)} which wraps the scan in a
     * {@code Project} (leaving the scan's row type untouched). We instead replace the scan with
     * a {@link LogicalTableScan} whose {@link RelOptTable} reports a narrowed row type — that
     * is what {@link OpenSearchTableScanRule} reads when computing viability.
     *
     * <p>{@code extraFields} is intentionally ignored: analytics-engine tables expose their
     * full schema through {@code RelOptTable.getRowType()}, and there are no
     * non-row-type system columns (transaction id, version, snapshot, etc.) that callers can
     * request via {@code extraFields}. Honoring an empty {@code extraFields} contract still
     * preserves correctness for every caller in the analytics-engine flow.
     */
    @Override
    public TrimResult trimFields(TableScan scan, ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
        List<RelDataTypeField> originalFields = scan.getRowType().getFieldList();
        int fieldCount = originalFields.size();

        if (fieldsUsed.cardinality() == fieldCount) {
            // Plan reads every column; narrowing is a no-op.
            return result(scan, Mappings.createIdentity(fieldCount));
        }

        ImmutableBitSet retained;
        if (fieldsUsed.isEmpty()) {
            // The plan reads zero columns from this scan (literal-only project, count(*) over a
            // bare scan, etc.). The scan still has to expose at least one column so downstream
            // operators (and Calcite type assertions) have something to reference. Pick the
            // first column whose mapping has a NATIVE scan backend (delegation is excluded —
            // a delegation-only column passes viability but DataFusion fails at runtime when
            // the field isn't physically present in the columnar file the native scan opens).
            Integer placeholder = pickReadableColumn(scan, originalFields);
            if (placeholder == null) {
                // No readable column on this index — leave the scan alone so the scan rule's
                // viability error still surfaces (correct behavior: the index genuinely cannot
                // be read by any backend).
                return result(scan, Mappings.createIdentity(fieldCount));
            }
            retained = ImmutableBitSet.of(placeholder);
        } else {
            retained = fieldsUsed;
        }

        // Build the narrowed row type, preserving original column order so the resulting
        // schema is a strict prefix-preserving subset of the original.
        RelDataTypeFactory.Builder narrowedBuilder = scan.getCluster().getTypeFactory().builder();
        for (int oldIndex : retained) {
            narrowedBuilder.add(originalFields.get(oldIndex));
        }
        RelDataType narrowedRowType = narrowedBuilder.build();

        LogicalTableScan narrowedScan = LogicalTableScan.create(
            scan.getCluster(),
            new NarrowedRowTypeTable(scan.getTable(), narrowedRowType),
            scan.getHints()
        );

        // Mapping contract: SourceMapping with sourceCount=fieldCount (original) and
        // targetCount=retained.cardinality() (narrowed). For each surviving original index
        // i, mapping.set(i, newIndex) tells the parent "field i of the original lives at
        // newIndex of the narrowed output". The createMapping helper inherited from
        // RelFieldTrimmer builds exactly that shape.
        Mapping mapping = createMapping(retained, fieldCount);

        // result(...) preserves rel hints from the original scan and runs the correlation-
        // variable rewrite pass — required for any tree that contains a LogicalCorrelate.
        return result(narrowedScan, mapping, scan);
    }

    /**
     * Returns the index of the first column whose mapping has a NATIVE scan backend.
     * Delegation is intentionally excluded here: a placeholder column has to actually
     * be present in the on-disk format the executing backend reads (e.g. a parquet
     * field for the datafusion backend). A delegation-only column passes
     * {@link OpenSearchTableScanRule}'s viability check but DataFusion fails at
     * runtime with {@code Schema error: No field named <col>} because the field
     * doesn't exist in the columnar file the native scan opens.
     *
     * <p>Returns {@code null} if no native scan-capable column exists, or if the
     * index is not in cluster state. Skips derived columns: derived fields cannot
     * legally appear in a {@code TableScan} row type (the scan rule throws on
     * encountering one), so leaving them out of the placeholder pool keeps the
     * trimmer aligned with that invariant.
     */
    private Integer pickReadableColumn(TableScan scan, List<RelDataTypeField> originalFields) {
        String tableName = scan.getTable().getQualifiedName().getLast();
        IndexMetadata indexMetadata = context.getClusterState().metadata().index(tableName);
        if (indexMetadata == null) {
            return null;
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();
        FieldStorageResolver resolver = registry.resolveFieldStorage(indexMetadata);
        List<String> fieldNames = originalFields.stream().map(RelDataTypeField::getName).toList();
        List<FieldStorageInfo> fieldStorage = resolver.resolve(fieldNames);

        for (int i = 0; i < fieldStorage.size(); i++) {
            FieldStorageInfo info = fieldStorage.get(i);
            if (info.isDerived()) {
                continue;
            }
            if (!registry.scanBackendsForField(info).isEmpty()) {
                return i;
            }
        }
        return null;
    }

    /**
     * Wraps a {@code RelOptTable} returning a narrower row type than the underlying table.
     * Used to feed a pruned schema into a {@link LogicalTableScan} without subclassing
     * Calcite's {@code TableScan} or rebuilding the schema at the {@code SchemaPlus} level.
     */
    private static class NarrowedRowTypeTable extends RelOptAbstractTable {
        NarrowedRowTypeTable(RelOptTable delegate, RelDataType rowType) {
            super(delegate.getRelOptSchema(), nameFrom(delegate), rowType);
        }

        private static String nameFrom(RelOptTable delegate) {
            List<String> qualifiedName = delegate.getQualifiedName();
            return qualifiedName.get(qualifiedName.size() - 1);
        }
    }
}
