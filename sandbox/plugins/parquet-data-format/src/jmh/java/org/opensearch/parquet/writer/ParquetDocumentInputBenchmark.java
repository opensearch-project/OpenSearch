/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Realistic microbenchmark of the Parquet composite writer's per-document field-collection loop — the
 * {@code ParquetDocumentInput.addField} hot spot seen in bulk-indexing CPU profiles:
 *
 * <pre>
 *   JVM_IHashCode &lt;- System.identityHashCode &lt;- IdentityHashMap.put &lt;- SetFromMap.add
 *     &lt;- ParquetDocumentInput.addField &lt;- CompositeDocumentInput.addField
 *     &lt;- NumberFieldMapper.parseCreateFieldForPluggableFormat &lt;- DocumentParser...
 * </pre>
 *
 * <p>Both variants drive the loop with <b>real</b> {@link MappedFieldType} instances
 * ({@link NumberFieldMapper.NumberFieldType} / {@link KeywordFieldMapper.KeywordFieldType}) carrying real
 * Parquet capability maps, so the {@code getCapabilityMap().getOrDefault(...)} lookup and the identity of the
 * hashed objects match production exactly:
 * <ul>
 *   <li>{@code current} calls the shipped {@link ParquetDocumentInput#addField}, which dedups by field name in
 *       a {@code HashSet<String>} (the name String has a cached hash, so no {@code identityHashCode}).</li>
 *   <li>{@code legacy} replicates the original body inline — {@code Collections.newSetFromMap(new
 *       IdentityHashMap&lt;&gt;())} with one {@code add} per field — over the same field-type objects and the
 *       same capability check. Isolates the {@code identityHashCode} cost the shipped change removes.</li>
 *   <li>{@code pureLinearScan} replicates an unconditional linear reference-equality scan — a naive fix — to
 *       show its O(n²) blow-up on wide documents.</li>
 *   <li>{@code nameHashSet} replicates the shipped name-keyed {@code HashSet} strategy inline, as a control
 *       against {@code current} (which additionally pays for constructing/closing a real
 *       {@link ParquetDocumentInput} per invocation).</li>
 * </ul>
 *
 * <p>{@link #fields} sweeps realistic per-document field counts. Expectation: the name-keyed strategy
 * ({@code current} / {@code nameHashSet}) is at or near the fastest at every size with no quadratic blow-up.
 */
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class ParquetDocumentInputBenchmark {

    @Param({ "5", "20", "50", "200" })
    private int fields;

    private MappedFieldType[] fieldTypes;
    private Object[] values;

    @Setup(Level.Trial)
    public void setup() {
        fieldTypes = new MappedFieldType[fields];
        values = new Object[fields];
        Map<?, ?> parquetCaps = Map.of(
            ParquetDataFormatPlugin.PARQUET_DATA_FORMAT,
            Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
        );
        for (int i = 0; i < fields; i++) {
            // Alternate numeric and keyword field types, as a mixed document would carry.
            if ((i & 1) == 0) {
                NumberFieldMapper.NumberFieldType ft = new NumberFieldMapper.NumberFieldType("num_" + i, NumberFieldMapper.NumberType.LONG);
                @SuppressWarnings("unchecked")
                Map<org.opensearch.index.engine.dataformat.DataFormat, Set<FieldTypeCapabilities.Capability>> caps = (Map<
                    org.opensearch.index.engine.dataformat.DataFormat,
                    Set<FieldTypeCapabilities.Capability>>) parquetCaps;
                ft.setCapabilityMap(caps);
                fieldTypes[i] = ft;
                values[i] = (long) i;
            } else {
                KeywordFieldMapper.KeywordFieldType ft = new KeywordFieldMapper.KeywordFieldType("kw_" + i);
                @SuppressWarnings("unchecked")
                Map<org.opensearch.index.engine.dataformat.DataFormat, Set<FieldTypeCapabilities.Capability>> caps = (Map<
                    org.opensearch.index.engine.dataformat.DataFormat,
                    Set<FieldTypeCapabilities.Capability>>) parquetCaps;
                ft.setCapabilityMap(caps);
                fieldTypes[i] = ft;
                values[i] = "v_" + i;
            }
        }
    }

    /**
     * Shipped code path: the real {@link ParquetDocumentInput#addField}, pre-sized from the field count as
     * {@code ParquetIndexingEngine.newDocumentInput} does (no dedup-set/list resize allocations).
     */
    @Benchmark
    public void current(Blackhole bh) {
        ParquetDocumentInput input = new ParquetDocumentInput(fieldTypes.length);
        for (int i = 0; i < fieldTypes.length; i++) {
            input.addField(fieldTypes[i], values[i]);
        }
        bh.consume(input.getFinalInput());
        input.close();
    }

    /**
     * Same real {@code addField} but default-sized collections — isolates the allocation cost of incremental
     * {@code HashMap}/{@code ArrayList} resizes that pre-sizing removes (visible in {@code -prof gc} B/op).
     */
    @Benchmark
    public void currentUnsized(Blackhole bh) {
        ParquetDocumentInput input = new ParquetDocumentInput();
        for (int i = 0; i < fieldTypes.length; i++) {
            input.addField(fieldTypes[i], values[i]);
        }
        bh.consume(input.getFinalInput());
        input.close();
    }

    /**
     * Pre-change behavior reproduced faithfully: same capability gate, same real field-type objects, but an
     * {@code IdentityHashMap}-backed dedup set (one {@code add} per field). Measures the {@code identityHashCode}
     * cost the shipped change removes.
     */
    @Benchmark
    public void legacy(Blackhole bh) {
        List<Object> collected = new ArrayList<>();
        Set<MappedFieldType> dedup = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < fieldTypes.length; i++) {
            MappedFieldType fieldType = fieldTypes[i];
            Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
                .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
            if (capabilities.isEmpty()) {
                continue;
            }
            if (dedup.add(fieldType) == false) {
                throw new MapperParsingException("dup");
            }
            collected.add(values[i]);
        }
        bh.consume(collected);
    }

    /**
     * The naive fix: an unconditional linear reference-equality scan, same capability gate and real field-type
     * objects. Shows the O(n²) cost on wide documents that the name-keyed set avoids.
     */
    @Benchmark
    public void pureLinearScan(Blackhole bh) {
        List<MappedFieldType> collected = new ArrayList<>();
        List<Object> collectedValues = new ArrayList<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            MappedFieldType fieldType = fieldTypes[i];
            Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
                .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
            if (capabilities.isEmpty()) {
                continue;
            }
            for (int j = 0; j < collected.size(); j++) {
                if (collected.get(j) == fieldType) {
                    throw new MapperParsingException("dup");
                }
            }
            collected.add(fieldType);
            collectedValues.add(values[i]);
        }
        bh.consume(collectedValues);
    }

    /**
     * Dedup by field <b>name</b> in a plain {@link java.util.HashSet}. {@code MappedFieldType} inherits
     * {@code Object.hashCode()} (identity → {@code identityHashCode}), so a set of the objects would still pay
     * {@code JVM_IHashCode}; but {@code String.hashCode()} is cached, so hashing the name avoids it. O(n), so no
     * wide-document blow-up. Note: this dedups by name-equality rather than object identity — arguably the more
     * correct contract, matching the name-based duplicate error message.
     */
    @Benchmark
    public void nameHashSet(Blackhole bh) {
        List<Object> collected = new ArrayList<>();
        Set<String> dedup = new HashSet<>();
        for (int i = 0; i < fieldTypes.length; i++) {
            MappedFieldType fieldType = fieldTypes[i];
            Set<FieldTypeCapabilities.Capability> capabilities = fieldType.getCapabilityMap()
                .getOrDefault(ParquetDataFormatPlugin.PARQUET_DATA_FORMAT, Set.of());
            if (capabilities.isEmpty()) {
                continue;
            }
            if (dedup.add(fieldType.name()) == false) {
                throw new MapperParsingException("dup");
            }
            collected.add(values[i]);
        }
        bh.consume(collected);
    }
}
