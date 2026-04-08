/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.opensearch.analytics.backend.EngineResultBatch;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapts an Arrow {@link VectorSchemaRoot} to the engine-agnostic
 * {@link EngineResultBatch} interface, exposing field names, row count,
 * and positional value access without leaking Arrow types to callers.
 *
 * @opensearch.internal
 */
public class DataFusionResultBatch implements EngineResultBatch {

    private final VectorSchemaRoot root;
    private final List<String> fieldNames;

    public DataFusionResultBatch(VectorSchemaRoot root) {
        this.root = root;
        this.fieldNames = root.getSchema().getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public int getRowCount() {
        return root.getRowCount();
    }

    @Override
    public Object getFieldValue(String fieldName, int rowIndex) {
        FieldVector vector = root.getVector(fieldName);
        if (vector == null) {
            throw new IllegalArgumentException("Unknown field: " + fieldName);
        }
        return vector.getObject(rowIndex);
    }
}
