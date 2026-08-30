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
import org.opensearch.index.mapper.FlatObjectFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.ArrayDeque;
import java.util.Deque;
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

    // ── Nested handling ──
    // Lucene never represents nested structure with per-element child docs / block-join — every
    // source document is always exactly one Lucene document; Parquet's LIST<STRUCT> column remains
    // the sole source of truth for real structured/correlated queries over a nested field. What
    // Lucene DOES carry, for every leaf under a nested scope, is a coarse flat_object-STYLE,
    // doc-values-only projection (no inverted index — "index: false"): every leaf at any depth under
    // the outermost open nested field collapses into that field's own flat_object-shaped
    // _value/_valueAndPath doc-value entries (see FlatObjectFieldMapper.addDocValueOnlyLeaf), the
    // same lossy "bag of values" a real flat_object already accepts. This is enough to answer
    // coarse per-field queries like exists() but — exactly like flat_object — CANNOT correctly answer
    // multi-field correlation within one nested element; any correctness-sensitive conjunctive query
    // over a nested path must be evaluated only via Parquet/DataFusion, never delegated here.
    // `nestedPathStack` tracks the currently-open nested scopes; the OUTERMOST one (the bottom of the
    // stack, i.e. peekLast()) is the flattening anchor for every leaf added while any scope is open.
    private final Deque<String> nestedPathStack = new ArrayDeque<>();

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
     * Adds a field to the underlying Lucene document by looking up the appropriate
     * {@link LuceneFieldFactory} from the registry based on the field's type name.
     * <p>
     * The field is accepted only if OWNING_FORMAT owns at least one capability
     * for this field according to {@link MappedFieldType#getCapabilityMap()}. Fields with
     * an empty capability map (no format declared support) and fields owned by other
     * formats are silently skipped, mirroring the per-format self-filtering used by
     * {@code ParquetDocumentInput}. Fields added while inside a nested scope (between a
     * {@link #startNestedChild} and its matching {@link #endNestedChild}) are routed instead to
     * the coarse flat_object-style doc-values-only projection described on {@link #nestedPathStack}
     * — never to the normal per-type Lucene field the factory registry would otherwise build.
     *
     * @param fieldType the OpenSearch mapped field type
     * @param value     the field value
     */
    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        if (nestedPathStack.isEmpty() == false) {
            if (value == null) {
                return;
            }
            String rootFieldName = nestedPathStack.peekLast();
            String leafRelativePath = fieldType.name().substring(rootFieldName.length() + 1);
            String stringValue = String.valueOf(value);
            FlatObjectFieldMapper.addDocValueOnlyLeaf(document, rootFieldName, leafRelativePath, stringValue);
            FlatObjectFieldMapper.addDocValueOnlyPathMarker(document, rootFieldName, leafRelativePath);
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

    /**
     * Enters a nested scope for {@code nestedPath}. Does not materialize anything itself — just
     * records the scope so subsequent {@link #addField}/{@link #addMapEntry} calls, until the
     * matching {@link #endNestedChild()}, flatten relative to the OUTERMOST open path (see {@link
     * #nestedPathStack}). Stack-based so nesting composes to arbitrary depth, always anchoring to
     * the first (outermost) scope opened.
     */
    @Override
    public void startNestedChild(String nestedPath) {
        nestedPathStack.push(nestedPath);
    }

    /** Leaves the innermost open nested scope. */
    @Override
    public void endNestedChild() {
        if (nestedPathStack.isEmpty()) {
            throw new IllegalStateException("endNestedChild called with no open nested child");
        }
        nestedPathStack.pop();
    }

    /**
     * Emits one {@code (key, value)} entry of a {@code flat_object}'s open key space — the same
     * signal Parquet consumes to fill a {@code MAP<Utf8,Utf8>} column (see {@code
     * ParquetDocumentInput#addMapEntry}). Lucene has no MAP notion, so it reuses the flat_object's
     * OWN doc-values-only encoding: when {@code mapField} sits at the document root, the anchor is
     * the flat_object's own name (matching a real flat_object's classic behavior exactly); when it
     * sits inside a nested scope, the anchor is the OUTERMOST open nested path instead, and the
     * flat_object's leaves flatten into that path's coarse projection alongside every other leaf
     * under it (see {@link #nestedPathStack}).
     *
     * @param mapField the flat_object field the entry belongs to
     * @param key      the entry key — the leaf's dotted path relative to {@code mapField}
     * @param value    the entry value, or {@code null}
     */
    @Override
    public void addMapEntry(MappedFieldType mapField, String key, Object value) {
        if (value == null) {
            return;
        }
        String rootFieldName;
        String leafRelativePath;
        if (nestedPathStack.isEmpty() == false) {
            rootFieldName = nestedPathStack.peekLast();
            String mapFieldRelativeToAnchor = mapField.name().substring(rootFieldName.length() + 1);
            leafRelativePath = mapFieldRelativeToAnchor + "." + key;
        } else {
            rootFieldName = mapField.name();
            leafRelativePath = key;
        }
        String stringValue = String.valueOf(value);
        FlatObjectFieldMapper.addDocValueOnlyLeaf(document, rootFieldName, leafRelativePath, stringValue);
        FlatObjectFieldMapper.addDocValueOnlyPathMarker(document, rootFieldName, leafRelativePath);
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
