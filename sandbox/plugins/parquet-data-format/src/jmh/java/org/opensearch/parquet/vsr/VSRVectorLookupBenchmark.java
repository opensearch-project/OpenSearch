/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.vsr;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Measures the per-field vector resolution in the Parquet write path ({@code ManagedVSR.getVector} and its
 * {@code HashMap.getNode} share in bulk-indexing CPU profiles).
 *
 * <p>{@code VSRManager.addDocument} currently resolves the same vector <b>twice</b> per field: once for a
 * null-check, and again inside {@code ParquetField.addToGroup} (every implementation re-looks-up by
 * {@code fieldType.name()}). Variants, all against a real {@link ManagedVSR} with real Arrow vectors:
 * <ul>
 *   <li>{@code doubleLookup} — the current pattern: null-check lookup + write-side lookup per field.</li>
 *   <li>{@code singleLookup} — resolve once per field and reuse (what a pass-the-vector SPI change buys).</li>
 *   <li>{@code ordinalArray} — lower bound: vectors pre-resolved to a flat array once per schema, per-field
 *       access is an array read (what an ordinal-based layout would buy).</li>
 * </ul>
 *
 * <p>The delta between {@code doubleLookup} and {@code singleLookup} is the recoverable cost of the redundant
 * lookup; {@code ordinalArray} shows the remaining headroom. This gates whether the ~22-file SPI change
 * (passing the resolved vector into {@code addToGroup}) is worth its churn.
 */
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class VSRVectorLookupBenchmark {

    @Param({ "5", "20", "50" })
    private int fields;

    private RootAllocator allocator;
    private ManagedVSR vsr;
    private String[] fieldNames;
    private FieldVector[] ordinalVectors;

    @Setup(Level.Trial)
    public void setup() {
        allocator = new RootAllocator();
        List<Field> schemaFields = new ArrayList<>(fields);
        fieldNames = new String[fields];
        for (int i = 0; i < fields; i++) {
            String name = ((i & 1) == 0 ? "num_" : "kw_") + i;
            fieldNames[i] = name;
            ArrowType type = (i & 1) == 0 ? Types.MinorType.BIGINT.getType() : new ArrowType.Utf8();
            schemaFields.add(new Field(name, FieldType.nullable(type), null));
        }
        vsr = new ManagedVSR("bench", new Schema(schemaFields), allocator);
        // Ordinal layout: resolve once per schema, as an ordinal-based design would.
        ordinalVectors = new FieldVector[fields];
        for (int i = 0; i < fields; i++) {
            ordinalVectors[i] = vsr.getVector(fieldNames[i]);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        vsr.moveToFrozen();
        vsr.close();
    }

    /** Current pattern: null-check lookup + write-side lookup, both by name, per field. */
    @Benchmark
    public void doubleLookup(Blackhole bh) {
        for (int i = 0; i < fieldNames.length; i++) {
            FieldVector check = vsr.getVector(fieldNames[i]);
            if (check == null) {
                throw new IllegalStateException("missing vector");
            }
            bh.consume(vsr.getVector(fieldNames[i]));
        }
    }

    /** Pass-the-vector: resolve once per field, reuse for the null-check and the write. */
    @Benchmark
    public void singleLookup(Blackhole bh) {
        for (int i = 0; i < fieldNames.length; i++) {
            FieldVector vector = vsr.getVector(fieldNames[i]);
            if (vector == null) {
                throw new IllegalStateException("missing vector");
            }
            bh.consume(vector);
        }
    }

    /** Ordinal lower bound: vectors resolved once per schema; per-field access is an array read. */
    @Benchmark
    public void ordinalArray(Blackhole bh) {
        for (int i = 0; i < ordinalVectors.length; i++) {
            FieldVector vector = ordinalVectors[i];
            if (vector == null) {
                throw new IllegalStateException("missing vector");
            }
            bh.consume(vector);
        }
    }
}
