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
    private final DerivedSourceKeep derivedSourceKeep;

    /**
     * Creates a DerivedFieldGenerator with the specified fetchers that don't specify derived_source_keep.
     * Defaults to DerivedSourceKeep.NONE (uses doc values when available).
     *
     * @param mappedFieldType the field type
     * @param docValuesFetcher fetcher for doc values (sorted/deduplicated)
     * @param storedFieldFetcher fetcher for stored fields (preserves order/duplicates)
     */
    public DerivedFieldGenerator(
        MappedFieldType mappedFieldType,
        FieldValueFetcher docValuesFetcher,
        FieldValueFetcher storedFieldFetcher
    ) {
        this(mappedFieldType, docValuesFetcher, storedFieldFetcher, DerivedSourceKeep.NONE);
    }

    /**
     * Creates a DerivedFieldGenerator with the specified fetchers and derived source keep mode.
     *
     * @param mappedFieldType the field type
     * @param docValuesFetcher fetcher for doc values (sorted/deduplicated)
     * @param storedFieldFetcher fetcher for stored fields (preserves order/duplicates)
     * @param derivedSourceKeep the mode for source reconstruction (NONE or ARRAYS)
     */
    public DerivedFieldGenerator(
        MappedFieldType mappedFieldType,
        FieldValueFetcher docValuesFetcher,
        FieldValueFetcher storedFieldFetcher,
        DerivedSourceKeep derivedSourceKeep
    ) {
        this.mappedFieldType = mappedFieldType;
        this.derivedSourceKeep = Objects.requireNonNull(derivedSourceKeep, "derivedSourceKeep cannot be null");

        if (derivedSourceKeep.requiresStoredFields()) {
            // User explicitly requested array preservation via stored fields
            if (storedFieldFetcher == null) {
                throw new IllegalArgumentException(
                    "derived_source_keep='arrays' requires stored fields to be enabled for field [" + mappedFieldType.name() + "]"
                );
            }
            this.fieldValueFetcher = storedFieldFetcher;
        } else {
            // Default behavior: prefer doc values if available, otherwise use stored fields
            if (Objects.requireNonNull(getDerivedFieldPreference()) == FieldValueType.DOC_VALUES) {
                assert docValuesFetcher != null;
                this.fieldValueFetcher = docValuesFetcher;
            } else {
                assert storedFieldFetcher != null;
                this.fieldValueFetcher = storedFieldFetcher;
            }
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
     * Returns the derived source keep mode for this generator.
     */
    public DerivedSourceKeep getDerivedSourceKeep() {
        return derivedSourceKeep;
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
