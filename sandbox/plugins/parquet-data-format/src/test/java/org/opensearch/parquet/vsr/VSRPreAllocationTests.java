/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Measures throughput difference between default (grow-on-demand) and
 * pre-allocated Arrow VectorSchemaRoot for variable-width fields.
 *
 * This validates that pre-allocation eliminates reallocDataBuffer calls
 * during the hot indexing loop, reducing both memcpy and allocator lock pressure.
 */
public class VSRPreAllocationTests extends OpenSearchTestCase {

    private static final int BATCH_SIZE = 8192;
    private static final int NUM_VARCHAR_FIELDS = 10;
    private static final int AVG_VALUE_BYTES = 20;
    private static final int WARMUP_ITERATIONS = 5;
    private static final int MEASURE_ITERATIONS = 20;

    public void testPreAllocationThroughputImprovement() {
        Schema schema = buildSchema(NUM_VARCHAR_FIELDS);
        byte[][] values = generateValues(BATCH_SIZE, AVG_VALUE_BYTES);

        try (RootAllocator root = new RootAllocator(Long.MAX_VALUE)) {
            // Warmup
            for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                runDefaultAllocation(root, schema, values);
                runPreAllocated(root, schema, values);
            }

            // Measure default (grow-on-demand)
            long defaultTotalNs = 0;
            for (int i = 0; i < MEASURE_ITERATIONS; i++) {
                defaultTotalNs += runDefaultAllocation(root, schema, values);
            }

            // Measure pre-allocated
            long preAllocTotalNs = 0;
            for (int i = 0; i < MEASURE_ITERATIONS; i++) {
                preAllocTotalNs += runPreAllocated(root, schema, values);
            }

            double defaultAvgMs = (defaultTotalNs / MEASURE_ITERATIONS) / 1_000_000.0;
            double preAllocAvgMs = (preAllocTotalNs / MEASURE_ITERATIONS) / 1_000_000.0;
            double speedup = defaultAvgMs / preAllocAvgMs;

            logger.info("=== VSR Pre-Allocation Benchmark ===");
            logger.info("Schema: {} varchar fields, batch size: {}, avg value: {} bytes", NUM_VARCHAR_FIELDS, BATCH_SIZE, AVG_VALUE_BYTES);
            logger.info("Default (grow-on-demand): {:.2f} ms per batch", defaultAvgMs);
            logger.info("Pre-allocated:            {:.2f} ms per batch", preAllocAvgMs);
            logger.info("Speedup:                  {:.2f}x", speedup);
            logger.info("====================================");

            // Assert meaningful improvement
            assertTrue(
                "Pre-allocation should be faster than grow-on-demand. Default: " + defaultAvgMs + "ms, PreAlloc: " + preAllocAvgMs + "ms",
                preAllocAvgMs < defaultAvgMs
            );
        }
    }

    /**
     * Default path: VectorSchemaRoot.create() with no pre-allocation.
     * Vectors start at minimal capacity and reallocate on each overflow.
     */
    private long runDefaultAllocation(BufferAllocator root, Schema schema, byte[][] values) {
        try (BufferAllocator child = root.newChildAllocator("default-" + System.nanoTime(), 0, Long.MAX_VALUE)) {
            long start = System.nanoTime();

            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, child);
            fillVSR(vsr, values);
            vsr.close();

            return System.nanoTime() - start;
        }
    }

    /**
     * Pre-allocated path: only offset + validity buffers are pre-sized to final row capacity.
     * Data buffer stays grow-on-demand (content-dependent, doubling strategy is efficient).
     * This eliminates row-count-boundary reallocs without over-provisioning data.
     */
    private long runPreAllocated(BufferAllocator root, Schema schema, byte[][] values) {
        try (BufferAllocator child = root.newChildAllocator("prealloc-" + System.nanoTime(), 0, Long.MAX_VALUE)) {
            long start = System.nanoTime();

            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, child);
            for (int i = 0; i < vsr.getFieldVectors().size(); i++) {
                if (vsr.getFieldVectors().get(i) instanceof BaseVariableWidthVector varVector) {
                    varVector.setInitialCapacity(BATCH_SIZE);
                    varVector.allocateNew();
                }
            }
            fillVSR(vsr, values);
            vsr.close();

            return System.nanoTime() - start;
        }
    }

    private void fillVSR(VectorSchemaRoot vsr, byte[][] values) {
        List<VarCharVector> vectors = new ArrayList<>();
        for (int i = 0; i < vsr.getFieldVectors().size(); i++) {
            vectors.add((VarCharVector) vsr.getFieldVectors().get(i));
        }

        for (int row = 0; row < BATCH_SIZE; row++) {
            for (VarCharVector vector : vectors) {
                vector.setSafe(row, values[row]);
            }
        }
        vsr.setRowCount(BATCH_SIZE);
    }

    private Schema buildSchema(int numFields) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            fields.add(new Field("field_" + i, FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        }
        return new Schema(fields);
    }

    private byte[][] generateValues(int count, int avgBytes) {
        Random rng = new Random(42);
        byte[][] values = new byte[count][];
        for (int i = 0; i < count; i++) {
            int len = Math.max(1, avgBytes + rng.nextInt(avgBytes) - avgBytes / 2);
            byte[] v = new byte[len];
            for (int j = 0; j < len; j++) {
                v[j] = (byte) ('a' + rng.nextInt(26));
            }
            values[i] = v;
        }
        return values;
    }
}
