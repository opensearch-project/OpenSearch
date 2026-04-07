/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldType;

import java.util.List;

/**
 * Per-column storage metadata describing where doc values, indices and stored fields live.
 * Flows through the plan tree: each OpenSearchRelNode computes this for its
 * output columns from its input's metadata.
 *
 * @opensearch.internal
 */
public class FieldStorageInfo {

    private final String fieldName;
    private final String mappingType;
    private final FieldType fieldType;
    private final List<String> docValueFormats;
    private final List<String> indexFormats;
    private final List<String> storedFieldFormats;
    private final boolean derived;

    public FieldStorageInfo(
        String fieldName,
        String mappingType,
        FieldType fieldType,
        List<String> docValueFormats,
        List<String> indexFormats,
        List<String> storedFieldFormats,
        boolean derived
    ) {
        this.fieldName = fieldName;
        this.mappingType = mappingType;
        this.fieldType = fieldType;
        this.docValueFormats = docValueFormats;
        this.indexFormats = indexFormats;
        this.storedFieldFormats = storedFieldFormats;
        this.derived = derived;
    }

    /** Creates a derived column (agg result, expression) with no physical storage.
     *  FieldType inferred from SqlTypeName. */
    public static FieldStorageInfo derivedColumn(String fieldName, SqlTypeName sqlTypeName) {
        return new FieldStorageInfo(
            fieldName,
            sqlTypeName.getName(),
            FieldType.fromSqlTypeName(sqlTypeName),
            List.of(),
            List.of(),
            List.of(),
            true
        );
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public FieldType getFieldType() {
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

    /** Data formats holding stored fields for this field (e.g. ["parquet"], ["lucene"]). */
    public List<String> getStoredFieldFormats() {
        return storedFieldFormats;
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

    public boolean hasStoredFields() {
        return !storedFieldFormats.isEmpty();
    }
}
