/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.sort;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmarks for the _shard_doc composite key path:
 *   key = (shardKeyPrefix | (docBase + doc))
 *
 * Mirrors hot operations in ShardDocFieldComparatorSource without needing Lucene classes.
 */
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)

public class ShardDocComparatorBenchmark {

    @Param({ "1", "4", "16" })
    public int segments;

    @Param({ "50000" })
    public int docsPerSegment;

    @Param({ "7" })
    public int shardId;

    private long shardKeyPrefix;
    private int[] docBases;
    private int[] docs;
    private long[] keys;    // precomputed composite keys

    // per-doc global doc (docBase + doc) for doc-only baseline
    private int[] globalDocs;

    @Setup
    public void setup() {
        shardKeyPrefix = ((long) shardId) << 32;  // Must mirror ShardDocFieldComparatorSource.shardKeyPrefix

        docBases = new int[segments];
        for (int i = 1; i < segments; i++) {
            docBases[i] = docBases[i - 1] + docsPerSegment;
        }

        int total = segments * docsPerSegment;
        docs = new int[total];
        keys = new long[total];
        globalDocs = new int[total];

        Random r = new Random(42);
        int pos = 0;
        for (int s = 0; s < segments; s++) {
            int base = docBases[s];
            for (int d = 0; d < docsPerSegment; d++) {
                int doc = r.nextInt(docsPerSegment);
                docs[pos] = doc;
                keys[pos] = computeGlobalDocKey(base, doc);
                globalDocs[pos] = base + doc;
                pos++;
            }
        }
    }

    /** Baseline: compare only globalDoc  */
    @Benchmark
    public long compareDocOnlyAsc() {
        long acc = 0;
        for (int i = 1; i < globalDocs.length; i++) {
            acc += Integer.compare(globalDocs[i - 1], globalDocs[i]);
        }
        return acc;
    }

    /** raw key packing cost */
    @Benchmark
    public void packKey(Blackhole bh) {
        int total = segments * docsPerSegment;
        int idx = 0;
        for (int s = 0; s < segments; s++) {
            int base = docBases[s];
            for (int d = 0; d < docsPerSegment; d++) {
                long k = computeGlobalDocKey(base, docs[idx++]);
                bh.consume(k);
            }
        }
    }

    /** compare already-packed keys as ASC */
    @Benchmark
    public long compareAsc() {
        long acc = 0;
        for (int i = 1; i < keys.length; i++) {
            acc += Long.compare(keys[i - 1], keys[i]);
        }
        return acc;
    }

    /** compare already-packed keys as DESC */
    @Benchmark
    public long compareDesc() {
        long acc = 0;
        for (int i = 1; i < keys.length; i++) {
            acc += Long.compare(keys[i], keys[i - 1]); // reversed
        }
        return acc;
    }

    /** rough “collector loop” mix: copy + occasional compareBottom */
    @Benchmark
    public int copyAndCompareBottomAsc() {
        long bottom = Long.MIN_VALUE;
        int worse = 0;
        for (int i = 0; i < keys.length; i++) {
            long v = keys[i];                 // simulate copy(slot, doc)
            if ((i & 31) == 0) bottom = v;    // simulate setBottom every 32 items
            if (Long.compare(bottom, v) < 0) worse++;
        }
        return worse;
    }

    // Must mirror ShardDocFieldComparatorSource.computeGlobalDocKey: (shardId << 32) | (docBase + doc)
    private long computeGlobalDocKey(int docBase, int doc) {
        return shardKeyPrefix | (docBase + doc);
    }
}
