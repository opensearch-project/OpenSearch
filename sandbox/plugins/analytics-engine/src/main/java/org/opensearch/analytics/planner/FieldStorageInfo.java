/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import java.util.List;

/**
 * Per-column storage metadata describing where doc values and indices live.
 * Flows through the plan tree: each OpenSearchRelNode computes this for its
 * output columns from its input's metadata.
 *
 * <p>TODO: use {@code DataFormat} instead of String for format identifiers
 * once the dependency is wired through.
 *
 * @opensearch.internal
 */
public class FieldStorageInfo {

    private final String fieldName;
    private final String fieldType;
    private final List<String> docValueFormats;
    private final List<String> indexFormats;
    private final boolean derived;

    public FieldStorageInfo(String fieldName, String fieldType, List<String> docValueFormats,
                            List<String> indexFormats, boolean derived) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.docValueFormats = docValueFormats;
        this.indexFormats = indexFormats;
        this.derived = derived;
    }

    /** Creates a derived column (agg result, expression) with no physical storage. */
    public static FieldStorageInfo derivedColumn(String fieldName, String fieldType) {
        return new FieldStorageInfo(fieldName, fieldType, List.of(), List.of(), true);
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    /** Data formats holding doc values for this field (e.g. ["parquet"], ["parquet", "lucene"]). */
    public List<String> getDocValueFormats() {
        return docValueFormats;
    }

    /** Data formats holding an index for this field (e.g. ["lucene"], ["lucene", "tantivy"]). */
    public List<String> getIndexFormats() {
        return indexFormats;
    }

    /** True for computed columns (agg results, expressions) with no physical storage. */
    public boolean isDerived() {
        return derived;
    }

    public boolean hasDocValues() {
        return !docValueFormats.isEmpty();
    }

    public boolean hasIndex() {
        return !indexFormats.isEmpty();
    }
}
