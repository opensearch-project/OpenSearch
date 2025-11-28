/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Composite fetcher that tries multiple sources and returns already-converted values from the available source
 * with the highest priority
 *
 * @opensearch.internal
 */
public class CompositeFieldValueFetcher extends FieldValueFetcher {

    private final List<FieldValueFetcher> fieldValueFetchers;

    public CompositeFieldValueFetcher(
        String simpleName,
        List<FieldValueFetcher> fieldValueFetchers
    ) {
        super(simpleName);
        this.fieldValueFetchers = fieldValueFetchers;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        // Try fetching values from various fetchers as per priority
        for (final FieldValueFetcher fieldValueFetcher : fieldValueFetchers) {
            List<Object> values = fieldValueFetcher.fetch(reader, docId);

            // Convert values immediately after fetching
            if (values != null && !values.isEmpty()) {
                List<Object> convertedValues = new ArrayList<>(values.size());
                for (Object value : values) {
                    convertedValues.add(fieldValueFetcher.convert(value));
                }
                return convertedValues;
            }
        }
        return null;
    }

    @Override
    Object convert(Object value) {
        // Values are already converted, return as-is
        return value;
    }
}
