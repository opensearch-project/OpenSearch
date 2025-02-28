/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * DerivedSourceGenerator is used to generate derived source field based on field mapping and how
 * it is stored in lucene
 */
public class DerivedFieldGenerator {
    private final Map<FieldValueType, FieldValueFetcher> fieldValueFetchers;

    private final MappedFieldType mappedFieldType;

    public DerivedFieldGenerator(MappedFieldType mappedFieldType) {
        this.mappedFieldType = mappedFieldType;
        this.fieldValueFetchers = new HashMap<>();
    }

    /**
     * Register the field value fetcher for a specific field value type, should be called from a FieldMapper to
     * register the field value fetcher for a field value type
     * @param fieldValueType - field value type
     * @param fieldValueFetcher - field value fetcher
     */
    public void registerFieldValueFetcher(FieldValueType fieldValueType, FieldValueFetcher fieldValueFetcher) {
        fieldValueFetchers.put(fieldValueType, fieldValueFetcher);
    }

    /**
     * Get the preference of the derived field based on field mapping, should be overridden at a FieldMapper to
     * alter the preference of derived field
     */
    public FieldValueType getDerivedFieldPreference() {
        if (mappedFieldType.hasDocValues()) {
            return FieldValueType.DOC_VALUES;
        } else if (mappedFieldType.isStored()) {
            return FieldValueType.STORED;
        } else {
            return FieldValueType.NONE;
        }
    }

    /**
     * Generate the derived field value based on the preference of derived field and field value type
     * @param builder - builder to store the derived source filed
     * @param reader - leafReader to read data from
     * @param docId - docId for which we want to generate the source
     */
    public void generate(XContentBuilder builder, LeafReader reader, int docId) throws IOException {
        FieldValueType preference = getDerivedFieldPreference();
        if (!fieldValueFetchers.containsKey(preference)) {
            throw new IllegalStateException("Unexpected value for derive source preference" + preference);
        }
        FieldValueFetcher fieldValueFetcher = fieldValueFetchers.get(preference);
        fieldValueFetcher.fetch(reader, docId);
        fieldValueFetcher.write(builder);
    }
}
