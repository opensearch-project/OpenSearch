/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.benchmark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing scalar (flat) Arrow vectors vs List vectors.
 *
 * <p>Mirrors the row-at-a-time write order used by {@code VSRManager.addDocument()}:
 * outer loop over rows, inner loop over fields, writing each field value at the
 * current row index before incrementing the row count.
 *
 * <p>When {@code useList=false}, values are written directly to flat vectors (IntVector,
 * BigIntVector, VarCharVector). When {@code useList=true}, each field is a ListVector
 * wrapping the same element type, and each row writes a single-element list — measuring
 * the pure structural overhead of list encoding.
 */
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ListVsScalarBenchmark {

    @Param({ "true", "false" })
    private boolean useList;

    @Param({ "50000" })
    private int totalRows;

    @Param({ "20" })
    private int fieldCount;

    @Param({ "50001" })
    private int maxRowsPerVSR;

    private BufferAllocator allocator;
    private Schema schema;

    @Setup(Level.Invocation)
    public void setup() {
        allocator = new RootAllocator(Long.MAX_VALUE);

        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            fields.add(useList ? listField(i) : scalarField(i));
        }
        schema = new Schema(fields);
    }

    /**
     * Writes single values per field per row, matching VSRManager.addDocument() order:
     * for each row, iterate all fields, write value, then increment row count.
     * Rotates the VSR when maxRowsPerVSR is reached, mirroring VSRPool behavior.
     */
    @Benchmark
    public int writeRows() {
        int totalWritten = 0;
        VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, allocator);
        int rowInVsr = 0;

        for (int row = 0; row < totalRows; row++) {
            for (int f = 0; f < fieldCount; f++) {
                writeValue(vsr, f, rowInVsr, row);
            }
            rowInVsr++;
            vsr.setRowCount(rowInVsr);

            // Rotate VSR when threshold reached
            if (rowInVsr >= maxRowsPerVSR) {
                totalWritten += rowInVsr;
                vsr.close();
                vsr = VectorSchemaRoot.create(schema, allocator);
                rowInVsr = 0;
            }
        }

        totalWritten += rowInVsr;
        vsr.close();
        return totalWritten;
    }

    private void writeValue(VectorSchemaRoot vsr, int f, int rowIdx, int row) {
        // For list: resolve child vector and child index; for scalar: use vector directly at row index
        var vector = vsr.getVector(f);
        int writeIdx;
        if (useList) {
            ListVector lv = (ListVector) vector;
            writeIdx = lv.startNewValue(rowIdx);
            vector = lv.getDataVector();
        } else {
            writeIdx = rowIdx;
        }

        switch (f % 3) {
            case 0:
                ((IntVector) vector).setSafe(writeIdx, row + f);
                break;
            case 1:
                ((BigIntVector) vector).setSafe(writeIdx, (long) row * 100 + f);
                break;
            default:
                ((VarCharVector) vector).setSafe(writeIdx, ("value_" + row).getBytes(StandardCharsets.UTF_8));
                break;
        }

        if (useList) {
            ((ListVector) vsr.getVector(f)).endValue(rowIdx, 1);
        }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        allocator.close();
    }

    private static Field scalarField(int index) {
        switch (index % 3) {
            case 0:
                return new Field("int_" + index, FieldType.nullable(new ArrowType.Int(32, true)), null);
            case 1:
                return new Field("long_" + index, FieldType.nullable(new ArrowType.Int(64, true)), null);
            default:
                return new Field("kw_" + index, FieldType.nullable(new ArrowType.Utf8()), null);
        }
    }

    private static Field listField(int index) {
        Field child;
        switch (index % 3) {
            case 0:
                child = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
                break;
            case 1:
                child = new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), null);
                break;
            default:
                child = new Field("item", FieldType.nullable(new ArrowType.Utf8()), null);
                break;
        }
        return new Field(scalarField(index).getName(), FieldType.nullable(ArrowType.List.INSTANCE), Collections.singletonList(child));
    }
}
