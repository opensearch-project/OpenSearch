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
import java.util.Objects;

/**
 * DerivedSourceGenerator is used to generate derived source field based on field mapping and how
 * it is stored in lucene
 */
public class DerivedFieldGenerator {

    private final MappedFieldType mappedFieldType;
    private final FieldValueFetcher fieldValueFetcher;

    public DerivedFieldGenerator(
        MappedFieldType mappedFieldType,
        FieldValueFetcher docValuesFetcher,
        FieldValueFetcher storedFieldFetcher
    ) {
        this.mappedFieldType = mappedFieldType;
        if (Objects.requireNonNull(getDerivedFieldPreference()) == FieldValueType.DOC_VALUES) {
            assert docValuesFetcher != null;
            this.fieldValueFetcher = docValuesFetcher;
        } else {
            assert storedFieldFetcher != null;
            this.fieldValueFetcher = storedFieldFetcher;
        }
    }

    /**
     * Get the preference of the derived field based on field mapping, should be overridden at a FieldMapper to
     * alter the preference of derived field
     */
    public FieldValueType getDerivedFieldPreference() {
        if (mappedFieldType.hasDocValues()) {
            return FieldValueType.DOC_VALUES;
        }
        return FieldValueType.STORED;
    }

    /**
     * Generate the derived field value based on the preference of derived field and field value type
     * @param builder - builder to store the derived source filed
     * @param reader - leafReader to read data from
     * @param docId - docId for which we want to generate the source
     */
    public void generate(XContentBuilder builder, LeafReader reader, int docId) throws IOException {
        fieldValueFetcher.write(builder, fieldValueFetcher.fetch(reader, docId));
    }
}
