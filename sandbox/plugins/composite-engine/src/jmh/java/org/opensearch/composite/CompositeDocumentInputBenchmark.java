/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures {@code CompositeDocumentInput}'s per-field broadcast to secondary format inputs — the
 * {@code IdentityHashMap$IdentityHashMapIterator.hasNext/next} and {@code UnmodifiableEntrySet} frames seen
 * under {@code CompositeDocumentInput.addField} in bulk-indexing CPU profiles.
 *
 * <ul>
 *   <li>{@code current} drives the real {@link CompositeDocumentInput}, whose constructor snapshots the
 *       secondary map into flat arrays so {@code addField} loops an array (no per-call iterator).</li>
 *   <li>{@code legacyMapIteration} replicates the previous body inline — iterating
 *       {@code unmodifiableMap(IdentityHashMap).entrySet()} per field — over the same map shape the engine
 *       builds ({@code CompositeIndexingExecutionEngine.newDocumentInput} uses an {@code IdentityHashMap}).</li>
 * </ul>
 *
 * <p>The per-format delegate inputs are no-op stubs: the delegate cost is identical in both variants, so the
 * delta isolates the broadcast strategy itself. {@link #fields} sweeps per-document field counts;
 * {@link #secondaries} covers the one-secondary common case (Lucene primary + Parquet secondary) and a larger
 * fan-out.
 */
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class CompositeDocumentInputBenchmark {

    /** No-op delegate: the broadcast strategy is the subject, not the per-format input. */
    private static final class NoopDocumentInput implements DocumentInput<Object> {
        @Override
        public void addField(MappedFieldType fieldType, Object value) {}

        @Override
        public void setRowId(String rowIdFieldName, long rowId) {}

        @Override
        public Object getFinalInput() {
            return null;
        }

        @Override
        public long getFieldCount(String fieldName) {
            return 0;
        }

        @Override
        public void close() {}
    }

    private static final class TestFormat extends DataFormat {
        private final String name;

        TestFormat(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    }

    @Param({ "5", "20", "50" })
    private int fields;

    @Param({ "1", "3" })
    private int secondaries;

    private MappedFieldType[] fieldTypes;
    private Object[] values;
    private CompositeDocumentInput composite;
    private Map<DataFormat, DocumentInput<?>> legacyMap;
    private DocumentInput<?> primary;

    @Setup(Level.Trial)
    public void setup() {
        fieldTypes = new MappedFieldType[fields];
        values = new Object[fields];
        for (int i = 0; i < fields; i++) {
            if ((i & 1) == 0) {
                fieldTypes[i] = new NumberFieldMapper.NumberFieldType("num_" + i, NumberFieldMapper.NumberType.LONG);
                values[i] = (long) i;
            } else {
                fieldTypes[i] = new KeywordFieldMapper.KeywordFieldType("kw_" + i);
                values[i] = "v_" + i;
            }
        }
        primary = new NoopDocumentInput();
        // Same map shape the engine builds in newDocumentInput().
        Map<DataFormat, DocumentInput<?>> secondaryMap = new IdentityHashMap<>();
        for (int s = 0; s < secondaries; s++) {
            secondaryMap.put(new TestFormat("format_" + s), new NoopDocumentInput());
        }
        composite = new CompositeDocumentInput(new TestFormat("primary"), primary, secondaryMap);
        legacyMap = java.util.Collections.unmodifiableMap(secondaryMap);
    }

    /** Shipped code path: the real {@link CompositeDocumentInput#addField} (array-snapshot broadcast). */
    @Benchmark
    public void current(Blackhole bh) {
        for (int i = 0; i < fieldTypes.length; i++) {
            composite.addField(fieldTypes[i], values[i]);
        }
        bh.consume(composite);
    }

    /** Previous behavior reproduced inline: per-field entry-set iteration over the unmodifiable map. */
    @Benchmark
    public void legacyMapIteration(Blackhole bh) {
        for (int i = 0; i < fieldTypes.length; i++) {
            primary.addField(fieldTypes[i], values[i]);
            for (Map.Entry<DataFormat, DocumentInput<?>> entry : legacyMap.entrySet()) {
                entry.getValue().addField(fieldTypes[i], values[i]);
            }
        }
        bh.consume(legacyMap);
    }

    /** Sanity/control: {@code List.copyOf(map.values())} indexed loop — an alternative snapshot shape. */
    @Benchmark
    public void listSnapshot(Blackhole bh) {
        List<DocumentInput<?>> snapshot = List.copyOf(legacyMap.values());
        for (int i = 0; i < fieldTypes.length; i++) {
            primary.addField(fieldTypes[i], values[i]);
            for (int s = 0; s < snapshot.size(); s++) {
                snapshot.get(s).addField(fieldTypes[i], values[i]);
            }
        }
        bh.consume(snapshot);
    }
}
