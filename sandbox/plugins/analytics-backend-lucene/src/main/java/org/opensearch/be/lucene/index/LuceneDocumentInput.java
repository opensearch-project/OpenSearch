/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.opensearch.be.lucene.LuceneFieldFactory;
import org.opensearch.be.lucene.LuceneFieldFactoryRegistry;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

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

    /**
     * Name of the row ID field stored in each Lucene document.
     * @deprecated Use {@link DocumentInput#ROW_ID_FIELD} instead.
     */
    @Deprecated
    public static final String ROW_ID_FIELD = DocumentInput.ROW_ID_FIELD;

    private final Document document;
    private final LuceneFieldFactoryRegistry fieldFactoryRegistry;

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
     * Silently skips null values, null field types, and unregistered type names.
     *
     * @param fieldType the OpenSearch mapped field type
     * @param value     the field value
     */
    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        if (value == null || fieldType == null) {
            return;
        }

        String typeName = fieldType.typeName();
        LuceneFieldFactory factory = fieldFactoryRegistry.get(typeName);
        if (factory == null) {
            return;
        }

        factory.addField(document, fieldType, value);
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
    }

    /** No-op — this document input holds no closeable resources. */
    @Override
    public void close() {
        // No resources to release
    }
}
