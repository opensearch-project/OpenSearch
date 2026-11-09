/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A mock {@link DocumentInput} for testing purposes.
 */
public class MockDocumentInput implements DocumentInput<Map<String, Object>> {
    private final Map<String, Object> fields = new HashMap<>();

    @Override
    public Map<String, Object> getFinalInput() {
        return Collections.unmodifiableMap(fields);
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        fields.put(fieldType != null ? fieldType.name() : "field_" + fields.size(), value);
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        fields.put(rowIdFieldName, rowId);
    }

    @Override
    public long getFieldCount(String fieldName) {
        return fields.containsKey(fieldName) ? 1 : 0;
    }

    @Override
    public void close() {}
}
