/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.opensearch.be.lucene.LuceneFieldFactory;
import org.opensearch.be.lucene.LuceneFieldFactoryRegistry;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Set;

/**
 * Lucene-specific {@link DocumentInput} that builds a Lucene {@link Document}.
 *
 * Field creation is delegated to a {@link LuceneFieldFactoryRegistry} which maps
 * OpenSearch field type names to {@link LuceneFieldFactory} instances. This makes
 * the set of supported field types extensible without modifying this class.
 *
 * Only field types registered in the registry are accepted. Attempting to add a field
 * of an unregistered type throws {@link IllegalArgumentException}.
 *
 * The row ID field is stored as a {@link SortedNumericDocValuesField} for efficient doc-value
 * access and compatibility with the {@code SortedNumericSortField}-based IndexSort,
 * maintaining 1:1 correspondence between Lucene doc IDs and Parquet row offsets.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneDocumentInput implements DocumentInput<Document> {

    private final Document document;
    private final LuceneFieldFactoryRegistry fieldFactoryRegistry;
    private long rowId = -1L;

    // Nested and flat_object data is intentionally never represented in Lucene at all — Parquet is
    // the sole source for both. This counter only tracks whether addField is currently inside a
    // nested scope, so it can be skipped entirely; no path/name information is needed since nothing
    // is written for that scope.
    private int nestedDepth = 0;

    /**
     * Creates a new LuceneDocumentInput with the default field factory registry.
     */
    public LuceneDocumentInput() {
        this(new LuceneFieldFactoryRegistry());
    }

    /**
     * Creates a new LuceneDocumentInput with a custom field factory registry.
     *
     * @param fieldFactoryRegistry the registry to use for field creation
     */
    public LuceneDocumentInput(LuceneFieldFactoryRegistry fieldFactoryRegistry) {
        this.document = new Document();
        this.fieldFactoryRegistry = fieldFactoryRegistry;
    }

    /**
     * Returns the built Lucene {@link Document} containing all added fields.
     *
     * @return the Lucene document
     */
    @Override
    public Document getFinalInput() {
        return document;
    }

    /**
     * Adds a field via the registered {@link LuceneFieldFactory} for its type. Silently skipped if no
     * format declared support (empty capability map) — mirrors {@code ParquetDocumentInput}'s
     * self-filtering. Inside a nested scope, this is a no-op — nested leaves are Parquet-only.
     *
     * @param fieldType the OpenSearch mapped field type
     * @param value     the field value
     */
    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        if (nestedDepth > 0) {
            return;
        }
        Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap().getOrDefault(LucenePlugin.DATA_FORMAT, Set.of());
        if (capabilities.isEmpty()) {
            // nothing to support on this format for this field.
            return;
        }
        if (value == null) {
            throw new IllegalArgumentException(
                "Field value must not be null for: " + fieldType.name() + " of type: " + fieldType.typeName()
            );
        }
        LuceneFieldFactory factory = fieldFactory(fieldType);
        if (factory == null) {
            // capabilities need to be supported but actual implementation to support lucene field type does not exist.
            throw new IllegalArgumentException(
                "Field: " + fieldType.name() + " requests capability: " + capabilities + " but does not have any factory to support"
            );
        }
        FieldType luceneFieldType = getFieldType(fieldType, capabilities);
        factory.addField(document, fieldType, value, luceneFieldType);
    }

    private static FieldType getFieldType(MappedFieldType fieldType, Set<FieldTypeCapabilities.Capability> capabilities) {
        FieldType luceneFieldType = null;
        if (fieldType.getTextSearchInfo() != null && fieldType.getTextSearchInfo().getLuceneFieldType() != null) {
            luceneFieldType = new FieldType(fieldType.getTextSearchInfo().getLuceneFieldType());
            if (!capabilities.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)) {
                // Disable doc values even if core mappers have set it on lucene fields
                // once we introduce more frontend params, we can remove this check.
                luceneFieldType.setDocValuesType(DocValuesType.NONE);
            }
            luceneFieldType.setStored(false);
            luceneFieldType.setOmitNorms(true);
        }
        return luceneFieldType;
    }

    private LuceneFieldFactory fieldFactory(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("Field type and value must not be null");
        }
        return fieldFactoryRegistry.get(fieldType.typeName());
    }

    /**
     * Stores the row ID as a {@link SortedNumericDocValuesField} to maintain 1:1 correspondence
     * between Lucene doc IDs and Parquet row offsets.
     *
     * @param rowIdFieldName the name of the row ID field
     * @param rowId          the row ID value (0-based sequential within the writer)
     */
    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        document.add(new SortedNumericDocValuesField(rowIdFieldName, rowId));
        this.rowId = rowId;
    }

    /** Returns the row ID assigned via {@link #setRowId}, or {@code -1} if none. */
    public long getRowId() {
        return rowId;
    }

    /** Enters a nested scope — tracked only to gate {@link #addField}, no path/name is retained. */
    @Override
    public void startNestedChild(String nestedPath) {
        nestedDepth++;
    }

    /** Leaves the innermost open nested scope. */
    @Override
    public void endNestedChild() {
        if (nestedDepth == 0) {
            throw new IllegalStateException("endNestedChild called with no open nested child");
        }
        nestedDepth--;
    }

    /**
     * No-op. {@code flat_object} data — whether at the document root or inside a nested scope — is
     * Parquet-only; Lucene never represents it, so this intentionally does nothing.
     *
     * @param mapField the flat_object field the entry belongs to
     * @param key      the entry key — the leaf's dotted path relative to {@code mapField}
     * @param value    the entry value, or {@code null}
     */
    @Override
    public void addMapEntry(MappedFieldType mapField, String key, Object value) {
        // Intentionally empty — see class-level note on nested/flat_object being Parquet-only.
    }

    @Override
    public long getFieldCount(String fieldName) {
        return document.getFields(fieldName).length;
    }

    /** No-op — this document input holds no closeable resources. */
    @Override
    public void close() {
        // No resources to release
    }
}
