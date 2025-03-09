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
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all field value fetchers be it doc values or stored field, consumer should override
 * fetch method to read the field value from the LeafReader, read access pattern can be different for
 * among different type of doc values as well, like SortedNumericDocValues and SortedSetDocValues
 * are stored in different manner, so reading them would also differ
 */
public abstract class FieldValueFetcher {
    final MappedFieldType mappedFieldType;

    final List<Object> values = new ArrayList<>();

    protected FieldValueFetcher(MappedFieldType mappedFieldType) {
        this.mappedFieldType = mappedFieldType;
    }

    /**
     * Fetches the field value from the LeafReader, whether it is doc value or stored field
     * It should be overridden by fetchers to read the doc values and stored field appropriately
     * @param reader - LeafReader to read data from
     * @param docId - document id to read
     */
    abstract void fetch(LeafReader reader, int docId) throws IOException;

    /**
     * Converts the field value to required representation, should be overridden by field mappers as needed
     * @param value - value to convert
     */
    Object convert(Object value) {
        return value;
    }

    boolean hasValue() {
        return !values.isEmpty();
    }

    void clear() {
        values.clear();
    }

    /**
     * Writes the field value(s) to the builder
     * It calls clear() to empty the list containing values after writing them to builder
     * For each value, it calls convert to transform the field value to required representation
     * @param builder - builder to store the field value(s) in
     */
    void write(XContentBuilder builder) throws IOException {
        if (!hasValue()) {
            return;
        }
        if (values.size() == 1) {
            builder.field(mappedFieldType.name(), convert(values.getFirst()));
        } else {
            builder.array(mappedFieldType.name(), values.stream().map(this::convert).toArray());
        }
        clear();
    }
}
