/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.store.remote.filecache;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.IndexInput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;

/**
 * Simple benchmark test of {@link FileCache}. It uses a uniform random distribution
 * of keys, which is very simple but unlikely to be representative of any real life
 * workload.
 */
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Threads(8)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class FileCacheBenchmark {
    private static final CachedIndexInput INDEX_INPUT = new FixedSizeStubIndexInput();

    @Benchmark
    public void get(CacheParameters parameters, Blackhole blackhole) {
        blackhole.consume(parameters.fileCache.get(randomKeyInCache(parameters)));
    }

    @Benchmark
    public void replace(CacheParameters parameters, Blackhole blackhole) {
        blackhole.consume(parameters.fileCache.put(randomKeyInCache(parameters), INDEX_INPUT));
    }

    @Benchmark
    public void put(CacheParameters parameters, Blackhole blackhole) {
        blackhole.consume(parameters.fileCache.put(randomKeyNotInCache(parameters), INDEX_INPUT));
    }

    @Benchmark
    public void remove(CacheParameters parameters) {
        parameters.fileCache.remove(randomKeyInCache(parameters));
    }

    private static Path randomKeyInCache(CacheParameters parameters) {
        int i = ThreadLocalRandom.current().nextInt(parameters.maximumNumberOfEntries);
        return Paths.get(Integer.toString(i));
    }

    private static Path randomKeyNotInCache(CacheParameters parameters) {
        int i = ThreadLocalRandom.current().nextInt(parameters.maximumNumberOfEntries, parameters.maximumNumberOfEntries * 2);
        return Paths.get(Integer.toString(i));
    }

    @State(Scope.Benchmark)
    public static class CacheParameters {
        @Param({ "65536", "1048576" })
        int maximumNumberOfEntries;

        @Param({ "1", "8" })
        int concurrencyLevel;

        FileCache fileCache;

        @Setup
        public void setup() {
            fileCache = FileCacheFactory.createConcurrentLRUFileCache(
                (long) maximumNumberOfEntries * INDEX_INPUT.length(),
                concurrencyLevel,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST)
            );
            for (long i = 0; i < maximumNumberOfEntries; i++) {
                final Path key = Paths.get(Long.toString(i));
                fileCache.put(key, INDEX_INPUT);
                fileCache.decRef(key);
            }
        }
    }

    /**
     * Stubbed out IndexInput that does nothing but report a fixed size
     */
    private static class FixedSizeStubIndexInput implements CachedIndexInput {
        @Override
        public IndexInput getIndexInput() {
            return null;
        }

        @Override
        public long length() {
            return 1024 * 1024 * 8; // 8MiB
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
