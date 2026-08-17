/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.index.engine.dataformat.DataFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-Isthmus rewriter that fixes the Substrait {@code base_schema} type of Engine-4 nested string
 * leaves so it matches their physical parquet layout.
 *
 * <p>A nested string leaf (e.g. {@code attributes.key}) is exposed to the planner as a scalar
 * {@code VARCHAR} column ({@code OpenSearchSchemaBuilder.addNestedStringLeafFields}) so that a
 * nested-leaf equality predicate can plan. Physically the parquet primary stores that column as a
 * {@code LIST<Utf8>} (one element per nested object). The predicate itself is
 * <em>correctness-delegated</em> to the element index ({@code aux__lucene__nested}), so DataFusion
 * never reads or evaluates the column — but Isthmus still emits the field into the scan's
 * {@code base_schema} with its scalar {@code Utf8} type, and the DataFusion Substrait consumer
 * rejects the plan up front ({@code ensure_schema_compatibility}) because the parquet table schema
 * reports {@code List(Utf8)} for the same field.
 *
 * <p>After delegation the leaf column is referenced by <em>no</em> {@code RexNode} in the fragment
 * (the delegated predicate is a field-free {@code delegated_predicate(annotationId)} marker; a
 * {@code count()} projects nothing). So this rewriter keeps the column at its original position (no
 * re-indexing of sibling refs) and only widens its <em>type</em> to {@code ARRAY(element)} via
 * {@link OpenSearchTableScan}'s {@code overrideRowType}. The emitted {@code base_schema} field then
 * reads {@code List(Utf8)} and matches parquet.
 *
 * <p>Scope: this makes nested-filter queries that project only flat columns (or nothing, e.g.
 * {@code count()}) execute. A query that actually <em>projects</em> a nested leaf would carry a
 * {@code RexInputRef} of the old scalar type into a parent operator and is out of scope here (the
 * deferred nested-projection / unnest path).
 */
final class NestedLeafListTypeRewriter {

    private NestedLeafListTypeRewriter() {}

    static RelNode rewrite(RelNode node) {
        if (node instanceof OpenSearchTableScan scan) {
            return retypeScan(scan);
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewrite(input);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RelNode retypeScan(OpenSearchTableScan scan) {
        List<FieldStorageInfo> fieldStorage = scan.getOutputFieldStorage();
        List<RelDataTypeField> fields = scan.getRowType().getFieldList();
        // outputFieldStorage aligns 1:1 with the (full) scan row type. If it doesn't, we can't safely
        // map columns to storage metadata — leave the scan untouched rather than mis-retype.
        if (fieldStorage == null || fieldStorage.size() != fields.size()) {
            return scan;
        }
        RelDataTypeFactory typeFactory = scan.getCluster().getTypeFactory();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        boolean changed = false;
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField field = fields.get(i);
            // Only widen scalar (non-collection) aux-delegated leaves; a column already typed as an
            // array is left as-is.
            if (isAuxiliaryDelegatedOnly(fieldStorage.get(i)) && field.getType().getComponentType() == null) {
                RelDataType listType = typeFactory.createTypeWithNullability(
                    typeFactory.createArrayType(field.getType(), -1),
                    true
                );
                builder.add(field.getName(), listType);
                changed = true;
            } else {
                builder.add(field.getName(), field.getType());
            }
        }
        if (changed == false) {
            return scan;
        }
        return new OpenSearchTableScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getTable(),
            scan.getViableBackends(),
            fieldStorage,
            builder.build()
        );
    }

    /**
     * True for a field whose only home is an auxiliary index (Engine-4's element index,
     * {@code aux__lucene__nested}): no doc-value format and every index format auxiliary
     * ({@link DataFormat#AUXILIARY_NAME_PREFIX}). Mirrors {@code OpenSearchTableScanRule}.
     */
    private static boolean isAuxiliaryDelegatedOnly(FieldStorageInfo field) {
        if (field.getDocValueFormats().isEmpty() == false) {
            return false;
        }
        List<String> indexFormats = field.getIndexFormats();
        if (indexFormats.isEmpty()) {
            return false;
        }
        for (String format : indexFormats) {
            if (format.startsWith(DataFormat.AUXILIARY_NAME_PREFIX) == false) {
                return false;
            }
        }
        return true;
    }
}
