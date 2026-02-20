/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.eqe.engine.ExecutionEngine;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Mock executor that returns hardcoded Arrow results for testing.
 * Returns 3 rows: Alice/30/95.5, Bob/25/87.3, Charlie/35/92.1.
 */
public class MockExecutionEngine implements ExecutionEngine {

    @Override
    public VectorSchemaRoot execute(byte[] planBytes, BufferAllocator allocator) {
        Schema schema = new Schema(List.of(
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)
        ));

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        BigIntVector ageVec = (BigIntVector) root.getVector("age");
        Float8Vector scoreVec = (Float8Vector) root.getVector("score");

        nameVec.setSafe(0, "Alice".getBytes(StandardCharsets.UTF_8));
        ageVec.setSafe(0, 30);
        scoreVec.setSafe(0, 95.5);

        nameVec.setSafe(1, "Bob".getBytes(StandardCharsets.UTF_8));
        ageVec.setSafe(1, 25);
        scoreVec.setSafe(1, 87.3);

        nameVec.setSafe(2, "Charlie".getBytes(StandardCharsets.UTF_8));
        ageVec.setSafe(2, 35);
        scoreVec.setSafe(2, 92.1);

        root.setRowCount(3);
        return root;
    }
}
