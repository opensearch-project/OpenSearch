/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.benchmark.search.aggregations.bucket.terms;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(1_000_000)
@State(Scope.Benchmark)
public class LongKeyedBucketOrdsBenchmark {
    private static final long LIMIT = 1_000_000;
    /**
     * The number of distinct values to add to the buckets.
     */
    private static final long DISTINCT_VALUES = 10;
    /**
     * The number of buckets to create in the {@link #multiBucket} case.
     * <p>
     * If this is not relatively prime to {@link #DISTINCT_VALUES} then the
     * values won't be scattered evenly across the buckets.
     */
    private static final long DISTINCT_BUCKETS = 21;

    private final PageCacheRecycler recycler = new PageCacheRecycler(Settings.EMPTY);
    private final BigArrays bigArrays = new BigArrays(recycler, null, "REQUEST");

    /**
     * Force loading all of the implementations just for extra paranoia's sake.
     * We really don't want the JVM to be able to eliminate one of them just
     * because we don't use it in the particular benchmark. That is totally a
     * thing it'd do. It is sneaky.
     */
    @Setup
    public void forceLoadClasses(Blackhole bh) {
        bh.consume(LongKeyedBucketOrds.FromSingle.class);
        bh.consume(LongKeyedBucketOrds.FromMany.class);
    }

    /**
     * Emulates a way that we do <strong>not</strong> use {@link LongKeyedBucketOrds}
     * because it is not needed.
     */
    @Benchmark
    public void singleBucketIntoSingleImmutableMonmorphicInvocation(Blackhole bh) {
        try (LongKeyedBucketOrds.FromSingle ords = new LongKeyedBucketOrds.FromSingle(bigArrays)) {
            for (long i = 0; i < LIMIT; i++) {
                ords.add(0, i % DISTINCT_VALUES);
            }
            bh.consume(ords);
        }
    }

    /**
     * Emulates the way that most aggregations use {@link LongKeyedBucketOrds}.
     */
    @Benchmark
    public void singleBucketIntoSingleImmutableBimorphicInvocation(Blackhole bh) {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE)) {
            for (long i = 0; i < LIMIT; i++) {
                ords.add(0, i % DISTINCT_VALUES);
            }
            bh.consume(ords);
        }
    }

    /**
     * Emulates the way that {@link AutoDateHistogramAggregationBuilder} uses {@link LongKeyedBucketOrds}.
     */
    @Benchmark
    public void singleBucketIntoSingleMutableMonmorphicInvocation(Blackhole bh) {
        LongKeyedBucketOrds.FromSingle ords = new LongKeyedBucketOrds.FromSingle(bigArrays);
        for (long i = 0; i < LIMIT; i++) {
            if (i % 100_000 == 0) {
                ords.close();
                bh.consume(ords);
                ords = new LongKeyedBucketOrds.FromSingle(bigArrays);
            }
            ords.add(0, i % DISTINCT_VALUES);
        }
        bh.consume(ords);
        ords.close();
    }

    /**
     * Emulates a way that we do <strong>not</strong> use {@link LongKeyedBucketOrds}
     * because it is significantly slower than the
     * {@link #singleBucketIntoSingleMutableMonmorphicInvocation monomorphic invocation}.
     */
    @Benchmark
    public void singleBucketIntoSingleMutableBimorphicInvocation(Blackhole bh) {
        LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
        for (long i = 0; i < LIMIT; i++) {
            if (i % 100_000 == 0) {
                ords.close();
                bh.consume(ords);
                ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
            }
            ords.add(0, i % DISTINCT_VALUES);

        }
        bh.consume(ords);
        ords.close();
    }

    /**
     * Emulates an aggregation that collects from a single bucket "by accident".
     * This can happen if an aggregation is under, say, a {@code terms}
     * aggregation and there is only a single value for that term in the index.
     */
    @Benchmark
    public void singleBucketIntoMulti(Blackhole bh) {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            for (long i = 0; i < LIMIT; i++) {
                ords.add(0, i % DISTINCT_VALUES);
            }
            bh.consume(ords);
        }
    }

    /**
     * Emulates an aggregation that collects from many buckets.
     */
    @Benchmark
    public void multiBucket(Blackhole bh) {
        try (LongKeyedBucketOrds ords = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY)) {
            for (long i = 0; i < LIMIT; i++) {
                ords.add(i % DISTINCT_BUCKETS, i % DISTINCT_VALUES);
            }
            bh.consume(ords);
        }
    }
}
