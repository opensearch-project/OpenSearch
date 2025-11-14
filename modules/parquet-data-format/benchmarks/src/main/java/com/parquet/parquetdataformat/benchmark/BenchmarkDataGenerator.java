/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.benchmark;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Utility class for generating test data for JNI benchmarks.
 * Creates Arrow schemas and record batches with varying complexity levels.
 */
public class BenchmarkDataGenerator {

    private final BufferAllocator allocator;
    private final Random random;

    public BenchmarkDataGenerator() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.random = new Random(42); // Fixed seed for reproducible benchmarks
    }

    public BenchmarkData generate(String schemaType, int fieldCount, int recordCount) {
        VectorSchemaRoot root  = createRecordBatch(schemaType, fieldCount, recordCount);
        ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);

        Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema);

        return new BenchmarkData(root, arrowSchema, arrowArray);
    }

    /**
     * Creates a simple schema with primitive types only.
     */
    public Schema createSimpleSchema(int fieldCount) {
        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < fieldCount; i++) {
            ArrowType type;
            String name = switch (i % 5) {
                case 0 -> {
                    type = new ArrowType.Int(32, true);
                    yield "int_field_" + i;
                }
                case 1 -> {
                    type = new ArrowType.Int(64, true);
                    yield "long_field_" + i;
                }
                case 2 -> {
                    type = new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
                    yield "double_field_" + i;
                }
                case 3 -> {
                    type = new ArrowType.Bool();
                    yield "bool_field_" + i;
                }
                default -> {
                    type = new ArrowType.Utf8();
                    yield "string_field_" + i;
                }
            };

            fields.add(new Field(name, FieldType.nullable(type), null));
        }

        return new Schema(fields);
    }

    /**
     * Creates a complex schema with nullable fields and mixed types.
     */
    public Schema createComplexSchema(int fieldCount) {
        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < fieldCount; i++) {
            ArrowType type;
            String name;
            boolean nullable = i % 3 == 0; // Every third field is nullable

            name = switch (i % 7) {
                case 0 -> {
                    type = new ArrowType.Int(32, true);
                    yield "int_field_" + i;
                }
                case 1 -> {
                    type = new ArrowType.Int(64, true);
                    yield "long_field_" + i;
                }
                case 2 -> {
                    type = new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
                    yield "double_field_" + i;
                }
                case 3 -> {
                    type = new ArrowType.Bool();
                    yield "bool_field_" + i;
                }
                case 4 -> {
                    type = new ArrowType.Utf8();
                    yield "string_field_" + i;
                }
                case 5 -> {
                    type = new ArrowType.Binary();
                    yield "binary_field_" + i;
                }
                default -> {
                    type = new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");
                    yield "timestamp_field_" + i;
                }
            };

            FieldType fieldType = nullable ? FieldType.nullable(type) : FieldType.notNullable(type);
            fields.add(new Field(name, fieldType, null));
        }

        return new Schema(fields);
    }

    /**
     * Creates a nested schema with struct arrays and lists.
     */
    public Schema createNestedSchema(int fieldCount) {
        List<Field> fields = new ArrayList<>();

        // Add some basic fields
        int basicFields = fieldCount / 2;
        for (int i = 0; i < basicFields; i++) {
            ArrowType type = i % 2 == 0 ? new ArrowType.Int(32, true) : new ArrowType.Utf8();
            String name = "basic_field_" + i;
            fields.add(new Field(name, FieldType.nullable(type), null));
        }

        // Add nested struct fields
        int structFields = fieldCount - basicFields;
        for (int i = 0; i < structFields; i++) {
            List<Field> structChildren = new ArrayList<>();
            structChildren.add(new Field("nested_int", FieldType.nullable(new ArrowType.Int(32, true)), null));
            structChildren.add(new Field("nested_string", FieldType.nullable(new ArrowType.Utf8()), null));
            structChildren.add(new Field("nested_double", FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null));

            Field structField = new Field("struct_field_" + i, FieldType.nullable(ArrowType.Struct.INSTANCE), structChildren);
            fields.add(structField);
        }

        return new Schema(fields);
    }

    /**
     * Creates a VectorSchemaRoot with test data based on the schema type.
     */
    public VectorSchemaRoot createRecordBatch(String schemaType, int fieldCount, int recordCount) {
        Schema schema = switch (schemaType) {
            case "complex" -> createComplexSchema(fieldCount);
            case "nested" -> createNestedSchema(fieldCount);
            default -> createSimpleSchema(fieldCount);
        };

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        populateRecordBatch(root, recordCount);
        root.setRowCount(recordCount);

        return root;
    }

    private void populateRecordBatch(VectorSchemaRoot root, int recordCount) {
        for (int fieldIndex = 0; fieldIndex < root.getFieldVectors().size(); fieldIndex++) {
            var vector = root.getVector(fieldIndex);

            if (vector instanceof IntVector intVector) {
                intVector.allocateNew(recordCount);
                for (int i = 0; i < recordCount; i++) {
                    intVector.set(i, random.nextInt(10000));
                }
                intVector.setValueCount(recordCount);

            } else if (vector instanceof BigIntVector longVector) {
                longVector.allocateNew(recordCount);
                for (int i = 0; i < recordCount; i++) {
                    longVector.set(i, random.nextLong());
                }
                longVector.setValueCount(recordCount);

            } else if (vector instanceof Float8Vector doubleVector) {
                doubleVector.allocateNew(recordCount);
                for (int i = 0; i < recordCount; i++) {
                    doubleVector.set(i, random.nextDouble() * 1000.0);
                }
                doubleVector.setValueCount(recordCount);

            } else if (vector instanceof BitVector boolVector) {
                boolVector.allocateNew(recordCount);
                for (int i = 0; i < recordCount; i++) {
                    boolVector.set(i, random.nextBoolean() ? 1 : 0);
                }
                boolVector.setValueCount(recordCount);

            } else if (vector instanceof VarCharVector stringVector) {
                stringVector.allocateNew(recordCount * 64, recordCount); // Estimate 64 chars per string
                for (int i = 0; i < recordCount; i++) {
                    String value = "benchmark_string_" + i + "_" + UUID.randomUUID().toString().substring(0, 8);
                    stringVector.set(i, value.getBytes(StandardCharsets.UTF_8));
                }
                stringVector.setValueCount(recordCount);

            } else if (vector instanceof StructVector structVector) {
                structVector.allocateNew();
                // Populate nested struct fields
                for (int i = 0; i < recordCount; i++) {
                    // This is a simplified population for nested structures
                    // In a real scenario, you'd need to handle each child vector properly
                }
                structVector.setValueCount(recordCount);
            }
        }
    }

    /**
     * Generates a temporary file path for benchmark operations.
     */
    public String generateTempFilePath() {
        return System.getProperty("java.io.tmpdir") + "/benchmark_" + UUID.randomUUID().toString() + ".parquet";
    }

    /**
     * Clean up resources.
     */
    public void close() {
        allocator.close();
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }
}
