/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of {@link DataFormat#hashCode()} on the composite-indexing hot path, where a
 * {@code DataFormat} is used as a map key once per field per document (capability-map lookups and
 * per-format routing).
 *
 * <p>{@code cachedHashCode} / {@code cachedMapLookup} exercise the shipped {@link DataFormat}, whose
 * {@code hashCode()} is {@code final} and caches the name hash after first use. Since the method is
 * {@code final} it cannot be overridden to simulate the pre-change behavior, so {@code uncachedHashCode}
 * reproduces the exact old body inline — {@code Objects.hashCode(name())} recomputed every call — and the
 * delta between the two isolates the caching win. {@code cachedMapLookup} shows the effect in situ: a
 * {@code HashMap} keyed by {@code DataFormat}, the shape of the real capability/routing maps.
 */
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class DataFormatHashCodeBenchmark {

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

    private DataFormat format;
    private Map<DataFormat, Object> map;

    @Setup
    public void setup() {
        format = new TestFormat("lucene");
        map = new HashMap<>();
        map.put(format, new Object());
        // Prime the cached hash so steady-state measurement reflects the cache hit, not the one-time compute.
        format.hashCode();
    }

    /** Shipped behavior: {@code hashCode()} returns the cached value. */
    @Benchmark
    public int cachedHashCode() {
        return format.hashCode();
    }

    /** Pre-change behavior reproduced inline: recompute {@code Objects.hashCode(name())} every call. */
    @Benchmark
    public int uncachedHashCode() {
        return Objects.hashCode(format.name());
    }

    /** In-situ effect: a map keyed by {@code DataFormat}, as in the capability/routing lookups. */
    @Benchmark
    public void cachedMapLookup(Blackhole bh) {
        bh.consume(map.get(format));
    }
}
